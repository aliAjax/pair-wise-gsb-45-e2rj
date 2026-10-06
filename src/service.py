"""业务用例编排、权限检查与审计。"""
from typing import Any, Dict, List, Optional

from .audit import AuditRecorder
from .domain import Actor, Conflict, PermissionDenied, ResourceBusy, ValidationError, VersionConflict, text
from .recovery import RecoveryOrchestrator
from .repository import Repository
from .rules import DomainRules


class Service:
    def __init__(self, repository: Repository, rules: DomainRules, audit: AuditRecorder = None) -> None:
        self.repository = repository
        self.rules = rules
        self.audit = audit or AuditRecorder(repository)
        self.recovery = RecoveryOrchestrator(repository, rules)

    @staticmethod
    def _actor(actor: Actor) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        return actor

    def _ensure_known_role(self, actor: Actor) -> None:
        if not self.rules.known_role(actor.role):
            raise PermissionDenied("角色无权访问该服务")

    def create(self, actor: Actor, reference: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_create(actor.role):
            raise PermissionDenied("角色无权创建记录")
        reference = text({"reference": reference}, "reference")
        prepared = self.rules.prepare_create(payload or {})
        self.rules.check_create_conflicts(prepared, self.repository.list_records(limit=500))
        return self.repository.create(reference, self.rules.INITIAL_STATE, prepared, actor.user_id)

    def list_records(self, actor: Actor, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_records(state=state, limit=limit)

    def get_record(self, actor: Actor, record_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.get(record_id)

    # ---------- 状态机动作 ----------

    def act(self, actor: Actor, record_id: int, expected_version: int, action: str, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        action = text({"action": action}, "action")
        if not self.rules.role_can_action(actor.role, action):
            raise PermissionDenied("角色无权执行该操作")
        if action == "confirm":
            return self.confirm_plan(actor, record_id, int(expected_version), data or {}, enforce_role=False)
        if action == "berth":
            return self.berth(actor, record_id, int(expected_version), data or {}, enforce_role=False)
        if action == "depart":
            return self.depart(actor, record_id, int(expected_version), data or {}, enforce_role=False)
        if action == "cancel":
            return self.cancel(actor, record_id, int(expected_version), data or {}, enforce_role=False)
        raise Conflict("未知动作%s" % action)

    def confirm_plan(self, actor: Actor, record_id: int, expected_version: int, data: Dict[str, Any],
                     enforce_role: bool = True) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if enforce_role and not self.rules.role_can_action(actor.role, "confirm"):
            raise PermissionDenied("角色无权执行该操作")

        record = self.repository.get(record_id)
        inputs = self.rules.validate_confirm(record, data)
        payload = dict(record["payload"])
        payload["pilot_id"] = inputs["pilot_id"]
        payload["channel_id"] = inputs["channel_id"]
        payload["series_id"] = inputs["series_id"]
        payload["chart_depth_m"] = inputs["chart_depth_m"]

        # 放行判定：有生效报文按潮位，没有则按人工录入水深先放行，报文到达后重算
        report = self.repository.active_tide_report(inputs["series_id"]) if inputs["series_id"] else None
        evaluation = self.rules.evaluate_clearance(payload, report)

        draft_snapshot = {"action": "confirm", "data": dict(data or {})}
        try:
            with self.repository.transaction() as connection:
                prior_open = self.repository.open_decision_locked(connection, record_id)
                if prior_open is not None:
                    # held 重新放行/重确认：旧未结判定作废；无论新判定是否通过，旧占用先释放，
                    # 新判定通过时再重新占用，保证不会出现僵尸占用或双重占用
                    self.repository.set_decision_status_locked(connection, int(prior_open["id"]), "superseded")
                    self.repository.release_bookings_locked(connection, record_id)
                new_decision = self.repository.insert_decision_locked(connection, {
                    "plan_id": record_id,
                    "kind": "initial",
                    "result": evaluation["result"],
                    "status": "active" if evaluation["result"] == "pass" else "held",
                    **evaluation,
                    "created_by": actor.user_id,
                })
                if evaluation["result"] == "pass":
                    self.repository.insert_bookings_locked(
                        connection, int(new_decision["id"]), record_id,
                        self.rules.resource_keys(payload),
                    )
                    target_state = "confirmed"
                else:
                    target_state = "held"
                payload["last_clearance"] = {
                    "decision_id": int(new_decision["id"]),
                    "result": evaluation["result"],
                    "available_depth_m": evaluation["available_depth_m"],
                    "required_depth_m": evaluation["required_depth_m"],
                    "message_id": evaluation.get("basis_message_id"),
                }
                result = self.repository.update_record_locked(
                    connection, record_id, expected_version, target_state, payload, actor.user_id, "confirm",
                    {"summary": "已确认并放行，占用航道通行证与引航班次" if evaluation["result"] == "pass"
                                else "水深不足，计划挂起等待重算",
                     "decision_id": int(new_decision["id"]), "clearance": evaluation,
                     "from": record["state"], "to": target_state},
                    allowed_from={"draft", "held"},
                )
        except VersionConflict as exc:
            # 两名调度员同时确认：后到版本不生效，原样保留为冲突草稿
            draft = self.repository.save_conflict_draft(
                record_id, actor.user_id, "confirm", int(expected_version),
                draft_snapshot["data"], "版本%s已被先到版本更新" % expected_version,
            )
            raise VersionConflict("确认未生效：已被先到版本抢先，冲突草稿#%s已保留" % draft["id"]) from exc
        return result

    def berth(self, actor: Actor, record_id: int, expected_version: int, data: Dict[str, Any],
              enforce_role: bool = True) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if enforce_role and not self.rules.role_can_action(actor.role, "berth"):
            raise PermissionDenied("角色无权执行该操作")
        record = self.repository.get(record_id)
        new_state, new_payload, summary = self.rules.apply_action(record, "berth", data or {})

        open_decision = self.repository.list_decisions(record_id)
        open_decision = [d for d in open_decision if d["status"] in {"active", "held", "retained"}]
        current = open_decision[-1] if open_decision else None
        if current is not None:
            actual_draft = float(new_payload["actual_draft_m"])
            if actual_draft + 0.5 > float(current["available_depth_m"]):
                raise ValidationError("按当前生效水深%sm，实际吃水%sm不满足富余水深"
                                      % (current["available_depth_m"], actual_draft))

        with self.repository.transaction() as connection:
            # 靠泊瞬间把生效判定转为 retained：当时依据被冻结，供后续复核
            if current is not None and current["status"] == "active":
                self.repository.set_decision_status_locked(connection, int(current["id"]), "retained")
            result = self.repository.update_record_locked(
                connection, record_id, expected_version, new_state, new_payload, actor.user_id, "berth",
                {"summary": summary, "input": data or {}, "from": record["state"], "to": new_state,
                 "retained_decision_id": int(current["id"]) if current else None},
            )
        return result

    def depart(self, actor: Actor, record_id: int, expected_version: int, data: Dict[str, Any],
               enforce_role: bool = True) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if enforce_role and not self.rules.role_can_action(actor.role, "depart"):
            raise PermissionDenied("角色无权执行该操作")
        record = self.repository.get(record_id)
        new_state, new_payload, summary = self.rules.apply_action(record, "depart", data or {})
        with self.repository.transaction() as connection:
            # 离泊释放航道通行证与引航班次；所有历史判定收尾为 completed
            released = self.repository.release_bookings_locked(connection, record_id)
            decisions = connection.execute(
                "SELECT id,status FROM clearance_decisions WHERE plan_id=? AND status IN ('active','held','retained')",
                (record_id,),
            ).fetchall()
            for decision in decisions:
                self.repository.set_decision_status_locked(connection, int(decision["id"]), "completed")
            result = self.repository.update_record_locked(
                connection, record_id, expected_version, new_state, new_payload, actor.user_id, "depart",
                {"summary": summary, "input": data or {}, "from": record["state"], "to": new_state,
                 "released_resources": released},
            )
        return result

    def cancel(self, actor: Actor, record_id: int, expected_version: int, data: Dict[str, Any],
               enforce_role: bool = True) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if enforce_role and not self.rules.role_can_action(actor.role, "cancel"):
            raise PermissionDenied("角色无权执行该操作")
        record = self.repository.get(record_id)
        new_state, new_payload, summary = self.rules.apply_action(record, "cancel", data or {})
        with self.repository.transaction() as connection:
            released = self.repository.release_bookings_locked(connection, record_id)
            decisions = connection.execute(
                "SELECT id,status FROM clearance_decisions WHERE plan_id=? AND status IN ('active','held')",
                (record_id,),
            ).fetchall()
            for decision in decisions:
                self.repository.set_decision_status_locked(connection, int(decision["id"]), "superseded")
            result = self.repository.update_record_locked(
                connection, record_id, expected_version, new_state, new_payload, actor.user_id, "cancel",
                {"summary": summary, "input": data or {}, "from": record["state"], "to": new_state,
                 "released_resources": released},
            )
        return result

    # ---------- 潮位报文与重算 ----------

    def ingest_tide_report(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_ingest_tide(actor.role):
            raise PermissionDenied("角色无权录入潮位报文")
        report, run = self.recovery.ingest_tide_report(actor.user_id, payload or {})
        result = dict(report)
        result["recompute_run_id"] = run["id"] if run else None
        return result

    def list_tide_reports(self, actor: Actor, series_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_tide_reports(series_id=series_id, limit=limit)

    def retry_recompute(self, actor: Actor, run_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.recovery.retry_run(int(run_id))

    def get_recompute_run(self, actor: Actor, run_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        run = self.repository.get_recompute_run(run_id)
        run["items"] = self.repository.list_recompute_items(run_id)
        return run

    def list_recompute_runs(self, actor: Actor, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_recompute_runs(limit=limit)

    # ---------- 冲突草稿 ----------

    def list_conflict_drafts(self, actor: Actor, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_conflict_drafts(status=status, limit=limit)

    def resolve_conflict_draft(self, actor: Actor, draft_id: int, resolution: str) -> Dict[str, Any]:
        """放弃或重新应用冲突草稿。reapply 时以草稿内容对当前最新版本再确认一次。"""
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        draft = self.repository.get_conflict_draft(draft_id)
        if draft["status"] != "open":
            raise Conflict("冲突草稿已处理")
        if resolution == "discard":
            with self.repository.transaction() as connection:
                self.repository.resolve_draft_locked(connection, draft_id, "discarded")
            return self.repository.get_conflict_draft(draft_id)
        if resolution != "reapply":
            raise ValidationError("resolution只能是reapply或discard")
        plan = self.repository.get(int(draft["plan_id"]))
        applied = self.confirm_plan(actor, int(draft["plan_id"]), int(plan["version"]),
                                    dict(draft["payload"]), enforce_role=True)
        with self.repository.transaction() as connection:
            self.repository.resolve_draft_locked(connection, draft_id, "reapplied")
        resolved = self.repository.get_conflict_draft(draft_id)
        resolved["applied_record"] = applied
        return resolved

    # ---------- 复核 ----------

    def list_reviews(self, actor: Actor, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_reviews(status=status, limit=limit)

    def resolve_review(self, actor: Actor, review_id: int, resolution: str) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        resolution = text({"resolution": resolution}, "resolution")
        return self.repository.resolve_review(int(review_id), resolution, actor.user_id)

    # ---------- 只读视图 ----------

    def list_decisions(self, actor: Actor, plan_id: int) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        self.repository.get(plan_id)
        return self.repository.list_decisions(plan_id)

    def list_bookings(self, actor: Actor, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_bookings(status=status, limit=limit)

    def timeline(self, actor: Actor, record_id: int) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.audit.timeline(record_id)

    def stats(self, actor: Actor) -> Dict[str, int]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.stats()
