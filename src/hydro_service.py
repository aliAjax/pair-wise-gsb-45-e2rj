"""水文复核用例编排。

把四者接成一条可恢复复核链路：

    测次录入水深 ──► 靠泊计划 ──► 调度确认 ──► 航道通行证 + 引航员班次
                         ▲                       │
                         └── 潮位报文迟到/订正 ── 重算（分段断点、可重放）

关键语义：
- 报文同一测次按报文号只入账一次（存储层 UNIQUE 约束 + 收件箱短路）；
- 报文到达后，未靠泊计划原判定失效（superseded）并重算，按新判定调整资源；
- 已开始靠泊的计划冻结当时依据（frozen），新判定仅 advisory，结论反转才挂待复核；
- 重算分"判定/对账"两段并持久化断点，失败后只重试未完成段，重放靠唯一索引幂等；
- 两名调度员并发确认时，整事务 CAS 保证只有先到版本放行，后到者保留冲突草稿。
"""
from typing import Any, Callable, Dict, List, Optional

from .domain import Actor, Conflict, PermissionDenied, ReleaseRejected, ValidationError
from .hydrology import ACTIVE_PLAN_STATES, HydroRules, PASS, stage_of
from .hydro_repository import HydroRepository

PLAN_ROLES = {"port_controller"}
REPORT_ROLES = {"tide_observer", "port_controller"}
SYSTEM_ACTOR = "system"


class HydroService:
    def __init__(self, repository: HydroRepository, rules: HydroRules = None,
                 failure_hook: Optional[Callable[[str, int], None]] = None) -> None:
        self.repository = repository
        self.rules = rules or HydroRules()
        # 测试用故障注入：failure_hook(stage, plan_id) 抛异常即模拟该段失败
        self.failure_hook = failure_hook

    # ---- 身份与权限 ----

    @staticmethod
    def _actor(actor: Optional[Actor]) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        return actor

    def _require(self, actor: Actor, roles: set, action: str) -> Actor:
        if actor.role != "admin" and actor.role not in roles:
            raise PermissionDenied("角色无权执行%s" % action)
        return actor

    # ---- 测次与计划 ----

    def create_series(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._require(self._actor(actor), PLAN_ROLES, "录入水深")
        data = self.rules.validate_series(payload or {})
        return self.repository.create_series(data["name"], data["sounding_m"], data["observed_hour"], actor.user_id)

    def list_series(self, actor: Optional[Actor] = None) -> List[Dict[str, Any]]:
        if actor is not None:
            self._actor(actor)
        return self.repository.list_series()

    def create_plan(self, actor: Actor, series_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._require(self._actor(actor), PLAN_ROLES, "编制靠泊计划")
        data = self.rules.validate_plan(payload or {})
        return self.repository.create_plan(
            int(series_id), data["name"], data["draft_m"], data["channel_pass"], data["pilot_shift"], actor.user_id
        )

    def get_plan(self, actor: Actor, plan_id: int) -> Dict[str, Any]:
        self._actor(actor)
        return self.repository.get_plan(int(plan_id))

    def list_plans(self, actor: Actor, series_id: Optional[int] = None, state: Optional[str] = None) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_plans(series_id=None if series_id is None else int(series_id), state=state)

    def decisions(self, actor: Actor, plan_id: int) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_decisions(int(plan_id))

    def reservations(self, actor: Actor, plan_id: int) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_reservations(int(plan_id))

    def drafts(self, actor: Actor, plan_id: int) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_drafts(int(plan_id))

    def events(self, actor: Actor, plan_id: int) -> List[Dict[str, Any]]:
        self._actor(actor)
        return self.repository.list_events(int(plan_id))

    # ---- 潮位报文：去重入账 + 触发复核作业 ----

    def ingest_tide_report(self, actor: Actor, series_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._require(self._actor(actor), REPORT_ROLES, "录入潮位报文")
        data = self.rules.validate_report(payload or {})
        result = self.repository.ingest_report(
            int(series_id), data["report_no"], data["tide_m"], data["observed_hour"], data["corrected"], actor.user_id
        )
        jobs = []
        # 同一报文号重放：不触发任何作业，已确认计划不会被动第二次
        for job in result["jobs"]:
            jobs.append(self.run_job(job["id"]))
        result["jobs"] = jobs
        return result

    # ---- 调度确认放行（含并发裁决） ----

    def release(self, actor: Actor, plan_id: int, expected_version: int, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._require(self._actor(actor), PLAN_ROLES, "确认放行")
        if not isinstance(expected_version, int) or isinstance(expected_version, bool):
            raise ValidationError("expected_version必须是整数")
        plan = self.repository.get_plan(int(plan_id))
        resources = self.rules.validate_release_data(data or {}, plan)
        series = self.repository.get_series(int(plan["series_id"]))
        tide = self.repository.latest_report(int(plan["series_id"]))
        basis = self.rules.decision_for_plan(plan, series, tide)
        plan["expected_version"] = expected_version
        if basis["verdict"] != PASS:
            # 判定不通过：不放行、不写放行依据、不占资源
            self.repository.add_event(
                "release_rejected", actor.user_id,
                {"basis": basis, "channel_pass": resources["channel_pass"], "pilot_shift": resources["pilot_shift"]},
                plan_id=int(plan_id),
            )
            raise ReleaseRejected(
                "有效水深%s米不足（需要%s米），不予放行" % (basis["effective_depth_m"], basis["required_depth_m"])
            )
        outcome = self.repository.release_plan_tx(plan, basis, resources["channel_pass"],
                                                  resources["pilot_shift"], actor.user_id)
        if outcome["outcome"] == "stale":
            raise Conflict("版本冲突，请刷新后重试")
        if outcome["outcome"] == "conflict":
            winner = outcome["plan"]
            # 后到版本保留为冲突草稿，便于人工复核；不重复占用航道通行证和引航员班次
            draft = self.repository.insert_draft(
                int(plan_id), actor.user_id, expected_version, resources, basis,
                "计划版本%s已被先到的确认放行（当前状态=%s）" % (expected_version, winner["state"]),
            )
            self.repository.add_event(
                "confirmation_conflict", actor.user_id,
                {"draft_id": draft["id"], "winner_state": winner["state"], "winner_version": winner["version"]},
                plan_id=int(plan_id),
            )
            error = Conflict("已有先到的确认版本放行，后到版本保留为冲突草稿#%s" % draft["id"])
            error.draft = draft
            raise error
        result = dict(outcome["plan"])
        result["decision_id"] = outcome["decision_id"]
        return result

    # ---- 靠泊 / 离泊 / 取消 ----

    def mark_berth(self, actor: Actor, plan_id: int) -> Dict[str, Any]:
        actor = self._require(self._actor(actor), PLAN_ROLES, "开始靠泊")
        plan = self.repository.get_plan(int(plan_id))
        self.rules.require_plan_state(plan, ["released"], "开始靠泊")
        result = self.repository.lifecycle_tx(
            int(plan_id), ("released",), "berthed", actor.user_id, "berthed", freeze=True
        )
        if result is None:
            raise Conflict("计划状态已变化，请刷新后重试")
        return result

    def depart(self, actor: Actor, plan_id: int) -> Dict[str, Any]:
        actor = self._require(self._actor(actor), PLAN_ROLES, "离泊")
        plan = self.repository.get_plan(int(plan_id))
        self.rules.require_plan_state(plan, ["berthed", "review"], "离泊")
        result = self.repository.lifecycle_tx(
            int(plan_id), ("berthed", "review"), "departed", actor.user_id, "depart",
            release_resources=True, clear_frozen=True
        )
        if result is None:
            raise Conflict("计划状态已变化，请刷新后重试")
        return result

    def cancel_plan(self, actor: Actor, plan_id: int) -> Dict[str, Any]:
        actor = self._require(self._actor(actor), PLAN_ROLES, "取消")
        plan = self.repository.get_plan(int(plan_id))
        self.rules.require_plan_state(plan, ["draft", "released", "held"], "取消")
        release = plan["state"] in ("released", "held")
        result = self.repository.lifecycle_tx(
            int(plan_id), ("draft", "released", "held"), "cancelled", actor.user_id, "cancel",
            release_resources=release, clear_frozen=True
        )
        if result is None:
            raise Conflict("计划状态已变化，请刷新后重试")
        return result

    # ---- 可恢复复核 saga ----

    def list_jobs(self, actor: Optional[Actor] = None, series_id: Optional[int] = None) -> List[Dict[str, Any]]:
        if actor is not None:
            self._actor(actor)
        return self.repository.list_jobs(None if series_id is None else int(series_id))

    def job_detail(self, actor: Actor, job_id: int) -> Dict[str, Any]:
        self._actor(actor)
        job = self.repository.get_job(int(job_id))
        job["items"] = self.repository.list_items(int(job_id))
        return job

    def run_job(self, job_id: int) -> Dict[str, Any]:
        """执行（或恢复）一个复核作业：逐段领取断点，只跑未完成/失败的段。

        某一段失败时本次运行即停止（熔断），失败段落盘为 failed；再次调用本方法
        才会重试该段——这正是"重算失败后只重试未完成项"的恢复语义。
        """
        job = self.repository.get_job(int(job_id))
        failed_fast = False
        while not failed_fast:
            item = self.repository.claim_item(int(job_id))
            if item is None:
                break
            stage = item.pop("_stage")
            try:
                if stage == "decide":
                    self._run_decide(item, int(job_id))
                else:
                    self._run_reconcile(item, int(job_id))
            except Exception as exc:  # 段失败落盘为 failed，熔断，等待下次恢复
                self.repository.finish_stage(int(item["id"]), stage, "failed", error=str(exc))
                failed_fast = True
        self.repository.refresh_all_jobs()
        detail = self.repository.get_job(int(job_id))
        detail["items"] = self.repository.list_items(int(job_id))
        return detail

    def _trigger_basis(self, job: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        series = self.repository.get_series(int(plan["series_id"]))
        report = self.repository.get_report_by_no(int(job["series_id"]), job["trigger_report_no"])
        return self.rules.decision_for_plan(plan, series, report)

    def _run_decide(self, item: Dict[str, Any], job_id: int) -> None:
        plan = self.repository.get_plan(int(item["plan_id"]))
        job = self.repository.get_job(job_id)
        basis = self._trigger_basis(job, plan)
        item["series_id"] = int(plan["series_id"])
        if self.failure_hook is not None:
            self.failure_hook("decide", int(plan["id"]))
        if stage_of(plan["state"]) == "berthed":
            # 已开始靠泊：保留当时依据，新判定只入账 advisory，资源保持占用
            self.repository.advisory_decision_tx(item, basis, SYSTEM_ACTOR, job_id)
        self.repository.finish_stage(int(item["id"]), "decide", "done", result=basis)

    def _run_reconcile(self, item: Dict[str, Any], job_id: int) -> None:
        plan = self.repository.get_plan(int(item["plan_id"]))
        stored = self.repository.list_items(job_id)
        own = next(row for row in stored if int(row["id"]) == int(item["id"]))
        basis = own["decide_result"]
        if basis is None:  # 理论上不会发生：claim 保证判定段已完成
            raise ValidationError("判定段尚未完成，不能对账")
        item["series_id"] = int(plan["series_id"])
        stage = stage_of(plan["state"])
        if stage == "berthed":
            # 靠泊分支在判定段已处理，本段无资源动作
            self.repository.finish_stage(int(item["id"]), "reconcile", "done",
                                         result={"skipped": "berthed_advisory"})
            return
        if plan["state"] in ("departed", "cancelled"):
            self.repository.finish_stage(int(item["id"]), "reconcile", "done",
                                         result={"skipped": plan["state"]})
            return
        # hook 由事务内部触发：模拟在"原判定已失效、资源尚未调整"的中途失败，
        # 用于验证重试时整段重放且不重复占用资源
        outcome = self.repository.reconcile_item_tx(
            item, basis, SYSTEM_ACTOR, job_id, self.failure_hook
        )
        self.repository.finish_stage(int(item["id"]), "reconcile", "done", result=outcome)
