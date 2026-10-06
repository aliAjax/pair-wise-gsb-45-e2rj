"""港口泊位与航道调度领域规则与状态转换。"""
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .domain import Actor, Conflict, ValidationError, boolean, choice, integer, number, text, text_list


INITIAL_STATE = "draft"
CREATE_ROLES = {'port_controller'}
TIDE_ROLES = {'tide_observer', 'port_controller'}
ACTION_ROLES = {'confirm': {'port_controller'}, 'berth': {'port_controller'}, 'depart': {'port_controller'}, 'cancel': {'port_controller'}}
TRANSITIONS = {
    'confirm': {'draft': 'confirmed', 'held': 'confirmed'},
    'berth': {'confirmed': 'berthed'},
    'depart': {'berthed': 'departed'},
    'cancel': {'draft': 'cancelled', 'confirmed': 'cancelled', 'held': 'cancelled'},
}
SAFETY_UNDERKEEL_M = 0.5
DEFAULT_CHANNEL = "CH-DEFAULT"
# 夜班时段：20:00-次日06:00
NIGHT_START_HOUR = 20
NIGHT_END_HOUR = 6


def is_night_shift(hour: int) -> bool:
    return hour >= NIGHT_START_HOUR or hour < NIGHT_END_HOUR


class DomainRules:
    INITIAL_STATE = INITIAL_STATE

    def known_role(self, role: str) -> bool:
        all_roles = set(CREATE_ROLES) | set(TIDE_ROLES)
        for roles in ACTION_ROLES.values():
            all_roles.update(roles)
        return role == "admin" or role in all_roles

    def role_can_create(self, role: str) -> bool:
        return role == "admin" or role in CREATE_ROLES

    def role_can_ingest_tide(self, role: str) -> bool:
        return role == "admin" or role in TIDE_ROLES

    def role_can_action(self, role: str, action: str) -> bool:
        return role == "admin" or role in ACTION_ROLES.get(action, set())

    def validate_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload)
        vessel = text(p, "vessel")
        berth = text(p, "berth")
        vessel_length = number(p, "vessel_length_m", 1)
        berth_length = number(p, "berth_length_m", 1)
        draft = number(p, "draft_m", 0)
        berth_depth = number(p, "berth_depth_m", 0)
        eta = integer(p, "eta_hour", 0, 23)
        etd = integer(p, "etd_hour", 1, 24)
        choice(p, "risk_level", ["low", "medium", "high"])
        dangerous = boolean(p, "dangerous_goods")
        if etd <= eta:
            raise ValidationError("etd_hour必须晚于eta_hour")
        if berth_length < vessel_length:
            raise ValidationError("泊位长度不足")
        if berth_depth - draft < 0.5:
            raise ValidationError("剩余水深不足")
        if dangerous:
            text(p, "dangerous_class")
        p["series_id"] = optional_stripped(p.get("series_id"))
        p["channel_id"] = optional_stripped(p.get("channel_id")) or DEFAULT_CHANNEL
        p["shift"] = "night" if is_night_shift(eta) else "day"
        return p

    def prepare_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = self.validate_create(payload)
        p["safety_margin_m"] = round(float(p["berth_depth_m"]) - float(p["draft_m"]), 2)
        p["window_hours"] = int(p["etd_hour"]) - int(p["eta_hour"])
        p["quay_ok"] = bool(p["berth_length_m"] >= p["vessel_length_m"] and p["safety_margin_m"] >= 0.5)
        return p

    def check_create_conflicts(self, payload: Dict[str, Any], existing: Iterable[Dict[str, Any]]) -> None:
        for item in existing:
            other = item["payload"]
            if item["state"] in {"cancelled", "departed"} or other.get("berth") != payload.get("berth"):
                continue
            if int(payload["eta_hour"]) < int(other.get("etd_hour", 0)) and int(payload["etd_hour"]) > int(other.get("eta_hour", 24)):
                raise Conflict("同一泊位时间窗冲突")

    def validate_tide_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = payload or {}
        return {
            "series_id": text(data, "series_id"),
            "message_no": text(data, "message_no"),
            "correction_seq": integer(data, "correction_seq", 1, 999) if "correction_seq" in data else 1,
            "level_m": round(number(data, "level_m", -50, 50), 2),
            "observed_hour": integer(data, "observed_hour", 0, 23),
        }

    def validate_confirm(self, record: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        data = data or {}
        pilot_id = text(data, "pilot_id")
        channel_id = optional_stripped(data.get("channel_id")) or str(record["payload"].get("channel_id") or DEFAULT_CHANNEL)
        series_id = optional_stripped(data.get("series_id")) or str(record["payload"].get("series_id") or "")
        # 海图水深可在确认时补录，缺省沿用建计划时的泊位水深
        chart_default = float(record["payload"].get("chart_depth_m", record["payload"]["berth_depth_m"]))
        chart_depth = number(data, "chart_depth_m", 0) if "chart_depth_m" in data else chart_default
        return {"pilot_id": pilot_id, "channel_id": channel_id, "series_id": series_id, "chart_depth_m": chart_depth}

    def resource_keys(self, payload: Dict[str, Any]) -> List[Tuple[str, str]]:
        eta = int(payload["eta_hour"])
        slot = "%02d" % eta
        return [
            ("channel_pass", "%s|%s" % (payload.get("channel_id", DEFAULT_CHANNEL), slot)),
            ("pilot_shift", "%s|%s" % (payload["pilot_id"], slot)),
        ]

    def evaluate_clearance(self, payload: Dict[str, Any], report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """依据当前生效报文计算放行判定；无报文时按人工录入水深入账（basis_kind=entered）。"""
        required = round(float(payload["draft_m"]) + SAFETY_UNDERKEEL_M, 2)
        chart_depth = round(float(payload.get("chart_depth_m", payload["berth_depth_m"])), 2)
        if report is None:
            # 夜班先按录入水深放行：录入值即当时可用水深，潮位项留空待报文到达后重算
            return {
                "basis_kind": "entered",
                "basis_message_id": None,
                "series_id": str(payload.get("series_id") or ""),
                "correction_seq": 0,
                "tide_level_m": None,
                "chart_depth_m": chart_depth,
                "available_depth_m": chart_depth,
                "required_depth_m": required,
                "result": "pass" if chart_depth >= required else "block",
            }
        tide_level = round(float(report["level_m"]), 2)
        available = round(chart_depth + tide_level, 2)
        return {
            "basis_kind": "tide",
            "basis_message_id": int(report["id"]),
            "series_id": str(report["series_id"]),
            "correction_seq": int(report["correction_seq"]),
            "tide_level_m": tide_level,
            "chart_depth_m": chart_depth,
            "available_depth_m": available,
            "required_depth_m": required,
            "result": "pass" if available >= required else "block",
        }

    def require_transition(self, record: Dict[str, Any], action: str) -> str:
        allowed = TRANSITIONS.get(action, {}).get(record["state"])
        if allowed is None:
            raise Conflict("当前状态不允许执行%s" % action)
        return allowed

    def apply_action(self, record: Dict[str, Any], action: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
        new_state = self.require_transition(record, action)
        data = dict(data or {})
        p = dict(record["payload"])
        changes: Dict[str, Any] = {}
        summary = ""
        if action == "confirm":
            pilot = text(data, "pilot_id")
            changes["pilot_id"] = pilot
            summary = "已确认引航员"
        elif action == "berth":
            actual = number(data, "actual_draft_m", 0)
            if float(p["berth_depth_m"]) - actual < 0.5:
                raise ValidationError("实际吃水导致水深不足")
            changes["actual_draft_m"] = actual
            summary = "船舶已靠泊"
        elif action == "depart":
            if not boolean(data, "cargo_operation_complete"):
                raise ValidationError("货物作业尚未完成")
            summary = "船舶已离泊"
        elif action == "cancel":
            changes["cancel_reason"] = text(data, "cancel_reason")
            summary = "计划已取消"
        p.update(changes)
        return new_state, p, summary or ("已执行%s" % action)


def optional_stripped(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValidationError("该字段必须是文本")
    return value.strip()
