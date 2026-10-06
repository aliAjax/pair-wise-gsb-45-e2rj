"""水文复核纯领域规则：有效水深判定、计划阶段与复核状态机。

本模块不碰数据库，方便直接做规则单测：

- 录入水深（sounding）来自测次，是航道/泊位测得的静基准；
- 潮位（tide）来自潮位观测报文，与水深相加得到有效水深；
- 报文未到时只能按录入水深作"暂判"，报文迟到/订正后必须重算。
"""
from typing import Any, Dict, Optional

from .domain import ValidationError, number, text


# 富余水深（米），与 rules.py 中 berth_depth - draft >= 0.5 的口径保持一致。
UKC_MARGIN_M = 0.5

# 计划阶段：未靠泊 / 已开始靠泊。reconcile 对两者的处理不同。
PLAN_STAGES = ("not_berthed", "berthed")

# 计划状态机
PLAN_STATES = ("draft", "released", "held", "berthed", "review", "departed", "cancelled")

# 判定结果
PASS = "pass"
FAIL = "fail"

# 判定记录的生命周期标签
LEDGER_STATUS = ("current", "superseded", "frozen", "advisory")

# 资源类型
RESOURCE_KINDS = ("channel_pass", "pilot_shift")

# 可占用资源的计划状态
ACTIVE_PLAN_STATES = ("released", "held", "berthed", "review")


def evaluate(sounding_m: float, draft_m: float, tide_m: Optional[float]) -> Dict[str, Any]:
    """计算单次放行判定。

    tide_m 为 None 表示报文未到，此时按录入水深作暂判（provisional=True），
    报文到达后同一测次的暂判必须失效并重算。
    """
    sounding_m = float(sounding_m)
    draft_m = float(draft_m)
    provisional = tide_m is None
    tide_value = 0.0 if provisional else float(tide_m)
    effective = round(sounding_m + tide_value, 3)
    required = round(draft_m + UKC_MARGIN_M, 3)
    verdict = PASS if effective >= required else FAIL
    return {
        "verdict": verdict,
        "sounding_m": round(sounding_m, 3),
        "tide_m": None if provisional else round(tide_value, 3),
        "effective_depth_m": effective,
        "required_depth_m": required,
        "margin_m": round(effective - required, 3),
        "provisional": provisional,
    }


def stage_of(state: str) -> str:
    return "berthed" if state in ("berthed", "review") else "not_berthed"


class HydroRules:
    """水文用例的输入校验与状态约束（纯逻辑，无副作用）。"""

    def validate_series(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        sounding = number(payload, "sounding_m", 0)
        observed_hour = number(payload, "observed_hour", 0, 23)
        name = text(payload, "name")
        return {"sounding_m": round(float(sounding), 3), "observed_hour": int(observed_hour), "name": name}

    def validate_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        report_no = text(payload, "report_no")
        tide = number(payload, "tide_m")
        observed_hour = number(payload, "observed_hour", 0, 23)
        corrected = bool(payload.get("corrected", False))
        return {
            "report_no": report_no,
            "tide_m": round(float(tide), 3),
            "observed_hour": int(observed_hour),
            "corrected": corrected,
        }

    def validate_plan(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        name = text(payload, "name")
        draft = number(payload, "draft_m", 0)
        channel_pass = text(payload, "channel_pass")
        pilot_shift = text(payload, "pilot_shift")
        return {
            "name": name,
            "draft_m": round(float(draft), 3),
            "channel_pass": channel_pass,
            "pilot_shift": pilot_shift,
        }

    def validate_release_data(self, payload: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, str]:
        """放行确认时允许覆盖分配的通行证/引航班次；缺省用计划建时的值。"""
        data = payload or {}

        def pick(key: str) -> str:
            if key in data and data[key] is not None:
                return text(data, key)
            return str(defaults[key])

        return {"channel_pass": pick("channel_pass"), "pilot_shift": pick("pilot_shift")}

    def require_plan_state(self, plan: Dict[str, Any], allowed, action: str) -> None:
        if plan["state"] not in allowed:
            raise ValidationError("计划当前状态(%s)不允许%s" % (plan["state"], action))

    def decision_for_plan(self, plan: Dict[str, Any], series: Dict[str, Any], tide_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """按测次录入水深 + 当前最新潮位报文，对计划做一次判定。"""
        tide_m = None
        basis_report_no = None
        corrected = False
        if tide_row is not None:
            tide_m = tide_row["tide_m"]
            basis_report_no = tide_row["report_no"]
            corrected = bool(tide_row["corrected"])
        result = evaluate(series["sounding_m"], plan["draft_m"], tide_m)
        result["basis_report_no"] = basis_report_no
        result["corrected"] = corrected
        return result
