"""报文迟到/订正后的可恢复重算与复核编排。

重算按“计划项”粒度逐项落库：每项独立事务、状态为 pending/failed/done/skipped，
失败后重试只挑未完成项；任何一步都是幂等的，重放不会重复占用航道通行证或引航班次。
"""
import json
from typing import Any, Dict, List, Optional, Tuple

from .domain import Actor, Conflict, ResourceBusy
from .repository import Repository
from .rules import DomainRules


# 报文触发重算的计划状态
AFFECTED_STATES = {"confirmed", "held", "berthed"}
# 已开始靠泊的计划：保留当时依据，只进复核队列
RETAIN_STATES = {"berthed"}


class RecoveryOrchestrator:
    def __init__(self, repository: Repository, rules: DomainRules) -> None:
        self.repository = repository
        self.rules = rules

    # ---------- 报文入账 ----------

    def ingest_tide_report(self, actor_id: str, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """报文按 (series_id, message_no) 唯一入账；迟到首报或订正都会触发一次重算运行。

        返回 (报文, 运行)。同一报文重放时返回既有报文与 None，绝不重复入账、重复触发。
        """
        report = self.rules.validate_tide_report(payload)
        existing = self.repository.get_tide_report(report["series_id"], report["message_no"])
        if existing is not None:
            # 同一测次同一报文号只入账一次：校验内容一致后按重放处理
            self._assert_same_report(existing, report)
            return existing, None

        with self.repository.transaction() as connection:
            previous = self.repository.active_tide_report_locked(connection, report["series_id"])
            if previous is not None and int(previous["correction_seq"]) >= int(report["correction_seq"]):
                # 更旧或同订正序号的报文不允许覆盖新报文
                raise Conflict("已存在订正序号不低于%s的生效报文（报文#%s）"
                               % (report["correction_seq"], previous["id"]))
            inserted = self.repository.insert_tide_report_locked(connection, report, actor_id)
            self.repository.supersede_tide_reports_locked(connection, report["series_id"], int(inserted["id"]))
            trigger_kind = "correction" if previous is not None else "late_arrival"
            plans = self.repository.plans_for_series_locked(connection, report["series_id"])
            run = self.repository.create_recompute_run_locked(
                connection, int(inserted["id"]), trigger_kind,
                [plan["id"] for plan in plans], actor_id,
            )
            items = self.repository.unfinished_items_locked(connection, int(run["id"]))
            self.repository.add_audit_locked(
                connection, None, actor_id, "tide_report_ingested",
                {"report_id": inserted["id"], "series_id": inserted["series_id"],
                 "message_no": inserted["message_no"], "trigger_kind": trigger_kind,
                 "superseded_report_id": previous["id"] if previous else None,
                 "run_id": run["id"], "affected": len(items)},
            )

        run = self.process_run(int(run["id"]))
        return self.repository.get_tide_report_by_id(int(inserted["id"])), run

    @staticmethod
    def _assert_same_report(existing: Dict[str, Any], incoming: Dict[str, Any]) -> None:
        if (int(existing["correction_seq"]) != int(incoming["correction_seq"])
                or abs(float(existing["level_m"]) - float(incoming["level_m"])) > 1e-9
                or int(existing["observed_hour"]) != int(incoming["observed_hour"])):
            raise Conflict("同一报文号%s/%s已入账且内容不同，订正请使用新的报文号"
                           % (incoming["series_id"], incoming["message_no"]))

    # ---------- 重算执行 ----------

    def process_run(self, run_id: int, max_items: Optional[int] = None) -> Dict[str, Any]:
        """处理运行中所有未完成（pending/failed）项；每项独立事务，可安全反复调用。"""
        processed = 0
        # 先取出待处理清单；每项独立事务，单项失败不影响其他项
        with self.repository._connect() as connection:
            items = self.repository.unfinished_items_locked(connection, run_id)
        for item in items:
            if max_items is not None and processed >= max_items:
                break
            self.process_item(int(item["id"]))
            processed += 1
        return self.repository.get_recompute_run(run_id)

    def retry_run(self, run_id: int) -> Dict[str, Any]:
        """重算失败后的恢复入口：只重试未完成项，已完成项不回放。"""
        run = self.repository.get_recompute_run(run_id)
        if run["status"] not in {"running", "completed"}:
            raise Conflict("运行状态%s不允许重试" % run["status"])
        return self.process_run(run_id)

    def process_item(self, item_id: int) -> Dict[str, Any]:
        """执行单个重算项，全程幂等。

        - 已开始靠泊（berthed）：原判定保留为 retained，资源不动，进复核队列。
        - 未靠泊（confirmed/held）：原判定失效（superseded）、释放旧占用，按新报文重算；
          通过则占用新资源转 confirmed，不通过则不占资源转 held。
        - 计划已取消/离泊或已有更新判定：跳过。
        """
        # 先在事务外读取快照；真正执行在独立事务中重新加锁校验
        with self.repository._connect() as connection:
            item = self.repository.get_recompute_item_locked(connection, item_id)
        if item is None:
            raise Conflict("重算项不存在")
        if item["status"] in {"done", "skipped"}:
            return self.repository.get_recompute_run(int(item["run_id"]))

        run = self.repository.get_recompute_run(int(item["run_id"]))
        new_report = self.repository.get_tide_report_by_id(int(run["trigger_message_id"]))
        actor_id = run["created_by"]

        try:
            with self.repository.transaction() as connection:
                locked_item = self.repository.get_recompute_item_locked(connection, item_id)
                if locked_item["status"] in {"done", "skipped"}:
                    self.repository.refresh_run_status_locked(connection, int(item["run_id"]))
                    return self.repository.get_recompute_run(int(item["run_id"]))

                plan_row = connection.execute("SELECT * FROM records WHERE id=?", (int(item["plan_id"]),)).fetchone()
                if plan_row is None:
                    self.repository.mark_item_locked(connection, item_id, "skipped", "计划已不存在")
                    self.repository.refresh_run_status_locked(connection, int(item["run_id"]))
                    return self.repository.get_recompute_run(int(item["run_id"]))

                plan = self.repository._row(plan_row)
                old_decision = self.repository.open_decision_locked(connection, int(plan["id"]))
                effective_report = new_report
                if new_report["status"] != "active":
                    active = self.repository.active_tide_report_locked(connection, new_report["series_id"])
                    if active is not None:
                        # 期间又有更新报文：本项对当前最新报文重算即可，保证结果一致
                        effective_report = active

                if plan["state"] not in AFFECTED_STATES:
                    self.repository.mark_item_locked(connection, item_id, "skipped",
                                                     "计划状态为%s，无需重算" % plan["state"])
                elif plan["state"] in RETAIN_STATES:
                    self._retain_for_review_locked(connection, locked_item, plan, old_decision,
                                                   effective_report, run, actor_id)
                else:
                    self._recompute_open_plan_locked(connection, locked_item, plan, old_decision,
                                                     effective_report, run, actor_id)
                self.repository.refresh_run_status_locked(connection, int(item["run_id"]))
        except ResourceBusy as exc:
            # 资源冲突：整项回滚，不落任何半成品占用；标记 failed 等待重试
            self._mark_item_failed_atomic(item_id, str(exc))
        except Conflict as exc:
            self._mark_item_failed_atomic(item_id, str(exc))
        return self.repository.get_recompute_run(int(item["run_id"]))

    def _mark_item_failed_atomic(self, item_id: int, error: str) -> None:
        with self.repository.transaction() as connection:
            connection.execute(
                "UPDATE recompute_items SET status='failed',error=?,attempt_count=attempt_count+1,updated_at=? WHERE id=?",
                (error[:500], self._now(), item_id),
            )
            row = connection.execute("SELECT run_id FROM recompute_items WHERE id=?", (item_id,)).fetchone()
            self.repository.refresh_run_status_locked(connection, int(row["run_id"]))

    @staticmethod
    def _now() -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    # ----- 内部：单项处理的两种分支（均在调用方事务内） -----

    def _retain_for_review_locked(self, connection, item: Dict[str, Any], plan: Dict[str, Any],
                                  old_decision: Optional[Dict[str, Any]], new_report: Dict[str, Any],
                                  run: Dict[str, Any], actor_id: str) -> None:
        from_decision_id = old_decision["id"] if old_decision else None
        if old_decision is not None and old_decision["status"] == "active":
            # 已开始靠泊：保留当时依据（retained），通行证/引航班次不释放、不改判
            self.repository.set_decision_status_locked(connection, int(old_decision["id"]), "retained")
        basis = {
            "old_message_id": old_decision.get("basis_message_id") if old_decision else None,
            "old_basis_kind": old_decision.get("basis_kind") if old_decision else None,
            "old_available_depth_m": old_decision.get("available_depth_m") if old_decision else None,
            "new_message_id": int(new_report["id"]),
            "new_level_m": float(new_report["level_m"]),
            "new_correction_seq": int(new_report["correction_seq"]),
        }
        self.repository.insert_review_locked(
            connection, int(plan["id"]), int(old_decision["id"]) if old_decision else None,
            int(run["id"]), "tide_correction_berthed", basis, actor_id,
        )
        self.repository.mark_item_locked(connection, int(item["id"]), "done", "",
                                         from_decision_id=from_decision_id)
        self.repository.add_audit_locked(
            connection, int(plan["id"]), actor_id, "recompute_retain",
            {"run_id": int(run["id"]), "plan_id": int(plan["id"]),
             "retained_decision_id": from_decision_id, "new_message_id": int(new_report["id"]),
             "summary": "已靠泊，保留当时依据并转人工复核"},
            version=int(plan["version"]),
        )

    def _recompute_open_plan_locked(self, connection, item: Dict[str, Any], plan: Dict[str, Any],
                                    old_decision: Optional[Dict[str, Any]], new_report: Dict[str, Any],
                                    run: Dict[str, Any], actor_id: str) -> None:
        from_decision_id = old_decision["id"] if old_decision else None

        # 若计划当前生效依据已经不早于该报文，则判定无需再算（重放保护）
        if old_decision is not None and old_decision.get("basis_message_id") == int(new_report["id"]):
            self.repository.mark_item_locked(connection, int(item["id"]), "skipped",
                                             "已基于报文#%s判定" % int(new_report["id"]),
                                             from_decision_id=from_decision_id,
                                             to_decision_id=int(old_decision["id"]))
            return

        payload = dict(plan["payload"])
        if old_decision is not None:
            payload["chart_depth_m"] = float(old_decision["chart_depth_m"])
            # 原判定失效，释放原占用
            self.repository.set_decision_status_locked(connection, int(old_decision["id"]), "superseded")
            self.repository.release_bookings_locked(connection, int(plan["id"]))
        elif "chart_depth_m" not in payload:
            payload["chart_depth_m"] = float(payload["berth_depth_m"])

        evaluation = self.rules.evaluate_clearance(payload, new_report)
        passed = evaluation["result"] == "pass"
        decision = dict(evaluation)
        decision.update({
            "plan_id": int(plan["id"]),
            "kind": "recompute",
            "status": "active" if passed else "held",
            "run_id": int(run["id"]),
            "created_by": actor_id,
        })
        new_decision = self.repository.insert_decision_locked(connection, decision)
        if passed:
            # 重放保护：旧占用已在上方统一释放，此处只会插一次，唯一索引兜住重复占资源
            self.repository.insert_bookings_locked(
                connection, int(new_decision["id"]), int(plan["id"]),
                self.rules.resource_keys(payload),
            )
            new_state = "confirmed"
        else:
            new_state = "held"
        payload["last_clearance"] = {
            "decision_id": int(new_decision["id"]),
            "result": evaluation["result"],
            "available_depth_m": evaluation["available_depth_m"],
            "required_depth_m": evaluation["required_depth_m"],
            "message_id": int(new_report["id"]),
        }
        self.repository.update_record_locked(
            connection, int(plan["id"]), int(plan["version"]), new_state, payload, actor_id,
            "recompute",
            {"run_id": int(run["id"]), "from_decision_id": from_decision_id,
             "to_decision_id": int(new_decision["id"]), "trigger_message_id": int(new_report["id"]),
             "result": evaluation["result"], "available_depth_m": evaluation["available_depth_m"],
             "required_depth_m": evaluation["required_depth_m"],
             "summary": "潮位订正到达，原判定失效并按新报文重算" if passed else "潮位订正到达，重算后水深不足，暂停放行"},
        )
        self.repository.mark_item_locked(connection, int(item["id"]), "done", "",
                                         from_decision_id=from_decision_id,
                                         to_decision_id=int(new_decision["id"]))
