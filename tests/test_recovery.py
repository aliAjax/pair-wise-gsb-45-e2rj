import json
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from pathlib import Path

from app import build_service
from src.domain import Actor, Conflict, ResourceBusy, VersionConflict
from src.http_api import create_server


def make_plan(reference, berth="B12", pilot="P1", channel="C1", series="S1", eta=22):
    return {
        "reference": reference,
        "data": {
            "vessel": "HaiYun-" + reference, "berth": berth,
            "vessel_length_m": 180, "berth_length_m": 220,
            "draft_m": 10.2, "berth_depth_m": 11.5,
            "eta_hour": eta, "etd_hour": 24,
            "risk_level": "medium", "dangerous_goods": False,
            "series_id": series, "channel_id": channel,
        },
    }


def tide(series, message_no, level, seq=1, hour=22):
    return {"series_id": series, "message_no": message_no,
            "correction_seq": seq, "level_m": level, "observed_hour": hour}


CTRL = lambda uid: Actor(uid, "port_controller")
OBS = Actor("obs-1", "tide_observer")


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = build_service(str(Path(self.temp.name) / "test.db"))

    def tearDown(self):
        self.temp.cleanup()

    def create_and_confirm(self, reference="V-1", **kwargs):
        plan = make_plan(reference, **kwargs)
        self.service.create(CTRL("creator"), plan["reference"], plan["data"])
        record = next(r for r in self.service.list_records(CTRL("creator")) if r["reference"] == reference)
        pilot = kwargs.get("pilot", "P1")
        record = self.service.act(CTRL("disp-a"), record["id"], record["version"],
                                  "confirm", {"pilot_id": pilot})
        return record

    # 1. 同一测次按报文号只入账一次：重放不产生新运行、不改变判定与占用
    def test_report_idempotent_by_message_no(self):
        self.create_and_confirm()
        report, run = self.service.recovery.ingest_tide_report("obs-1", tide("S1", "MSG-001", 0.5))
        self.assertIsNotNone(run)
        reports_before = self.service.list_tide_reports(OBS, "S1")
        decisions_before = self.service.list_decisions(CTRL("d"), 1)
        held_before = self.service.list_bookings(CTRL("d"), status="held")

        again, run_again = self.service.recovery.ingest_tide_report("obs-1", tide("S1", "MSG-001", 0.5))
        self.assertEqual(again["id"], report["id"])
        self.assertIsNone(run_again)
        self.assertEqual(len(self.service.list_tide_reports(OBS, "S1")), len(reports_before))
        self.assertEqual(len(self.service.list_decisions(CTRL("d"), 1)), len(decisions_before))
        self.assertEqual(len(self.service.list_bookings(CTRL("d"), status="held")), len(held_before))

        # 同报文号但内容不同：拒绝，订正必须换新报文号
        with self.assertRaises(Conflict):
            self.service.recovery.ingest_tide_report("obs-1", tide("S1", "MSG-001", 0.9))

    # 2. 迟到报文使未靠泊计划原判定失效并重算：不足挂起、订正后恢复
    def test_late_report_blocks_then_correction_passes(self):
        record = self.create_and_confirm()
        self.assertEqual(record["state"], "confirmed")
        self.assertEqual(2, len(self.service.list_bookings(CTRL("d"), status="held")))

        # 迟到首报：潮位 -1.0 → 可用水深 10.5 < 需求 10.7
        report, run = self.service.recovery.ingest_tide_report("obs-1", tide("S1", "MSG-1", -1.0))
        self.assertEqual(run["trigger_kind"], "late_arrival")
        self.assertEqual(run["status"], "completed")
        self.assertEqual(run["failed_count"], 0)
        record = self.service.get_record(CTRL("d"), record["id"])
        self.assertEqual(record["state"], "held")
        decisions = self.service.list_decisions(CTRL("d"), record["id"])
        self.assertEqual([d["status"] for d in decisions], ["superseded", "held"])
        self.assertEqual(0, len(self.service.list_bookings(CTRL("d"), status="held")))
        # held 计划不能靠泊
        with self.assertRaises(Conflict):
            self.service.act(CTRL("d"), record["id"], record["version"], "berth", {"actual_draft_m": 10.3})

        # 订正到达：潮位 +1.0 → 12.5 通过，资源重新占用
        report2, run2 = self.service.recovery.ingest_tide_report("obs-1", tide("S1", "MSG-2", 1.0, seq=2))
        self.assertEqual(run2["trigger_kind"], "correction")
        record = self.service.get_record(CTRL("d"), record["id"])
        self.assertEqual(record["state"], "confirmed")
        statuses = [d["status"] for d in self.service.list_decisions(CTRL("d"), record["id"])]
        self.assertEqual(statuses, ["superseded", "superseded", "active"])
        held = self.service.list_bookings(CTRL("d"), status="held")
        self.assertEqual(2, len(held))
        self.assertEqual(2, report2["id"])  # 订正为新报文，旧报文被置为 superseded
        self.assertEqual("active", report2["status"])
        self.assertEqual("superseded", self.service.list_tide_reports(OBS, "S1")[1]["status"])

    # 3. 已开始靠泊：保留当时依据待复核，资源不动，重放不重复入复核
    def test_berthed_plan_retains_basis_and_awaits_review(self):
        record = self.create_and_confirm()
        record = self.service.act(CTRL("d"), record["id"], record["version"],
                                  "berth", {"actual_draft_m": 10.3})
        self.assertEqual(record["state"], "berthed")
        decisions = self.service.list_decisions(CTRL("d"), record["id"])
        self.assertEqual(decisions[-1]["status"], "retained")
        self.assertEqual(2, len(self.service.list_bookings(CTRL("d"), status="held")))

        report, run = self.service.recovery.ingest_tide_report("obs-1", tide("S1", "MSG-9", -2.0))
        self.assertEqual(run["status"], "completed")
        # 计划仍靠泊，依据保留，通行证/引航班次仍占用
        record = self.service.get_record(CTRL("d"), record["id"])
        self.assertEqual(record["state"], "berthed")
        self.assertEqual("retained", self.service.list_decisions(CTRL("d"), record["id"])[-1]["status"])
        self.assertEqual(2, len(self.service.list_bookings(CTRL("d"), status="held")))

        reviews = self.service.list_reviews(CTRL("d"), status="open")
        self.assertEqual(1, len(reviews))
        self.assertEqual(reviews[0]["basis"]["new_level_m"], -2.0)
        self.assertEqual(reviews[0]["plan_id"], record["id"])

        # 重放报文不重复入复核
        self.service.recovery.ingest_tide_report("obs-1", tide("S1", "MSG-9", -2.0))
        self.assertEqual(1, len(self.service.list_reviews(CTRL("d"), status="open")))
        # 重试已完成运行也不产生新判定
        self.service.recovery.retry_run(run["id"])
        self.assertEqual(1, len(self.service.list_decisions(CTRL("d"), record["id"])))

        resolved = self.service.resolve_review(CTRL("d"), reviews[0]["id"], "潮位订正不影响夜间作业，维持靠泊")
        self.assertEqual(resolved["status"], "resolved")
        self.assertEqual(0, len(self.service.list_reviews(CTRL("d"), status="open")))

    # 4. 重算失败只重试未完成项，且重放不会重复占用资源
    def test_failed_item_retried_only_once_without_double_booking(self):
        plan_a = self.create_and_confirm("VA", berth="B12", pilot="PA", channel="CA")
        plan_b = self.create_and_confirm("VB", berth="B13", pilot="PB", channel="CB")
        self.assertEqual(4, len(self.service.list_bookings(CTRL("d"), status="held")))

        repo = self.service.repository
        original = repo.insert_bookings_locked

        def failing_insert(connection, decision_id, plan_id, resources):
            for rtype, rkey in resources:
                if rtype == "pilot_shift" and rkey.startswith("PA|"):
                    raise ResourceBusy("注入失败：引航班次暂不可用")
            return original(connection, decision_id, plan_id, resources)

        repo.insert_bookings_locked = failing_insert
        report, run = self.service.recovery.ingest_tide_report("obs-1", tide("S1", "R-1", 0.8))
        self.assertEqual("running", run["status"])
        self.assertEqual(1, run["failed_count"])
        # A 回滚干净：仍是旧的 active 判定与旧占用，没有半成品
        decisions_a = self.service.list_decisions(CTRL("d"), plan_a["id"])
        self.assertEqual(["active"], [d["status"] for d in decisions_a])
        self.assertEqual(4, len(self.service.list_bookings(CTRL("d"), status="held")))
        # B 只算过一次
        recompute_events_b = [e for e in self.service.timeline(CTRL("d"), plan_b["id"]) if e["action"] == "recompute"]
        self.assertEqual(1, len(recompute_events_b))

        # 恢复后重试：只处理 A 这一个 failed 项
        repo.insert_bookings_locked = original
        run = self.service.recovery.retry_run(run["id"])
        self.assertEqual("completed", run["status"])
        self.assertEqual(0, run["failed_count"])
        recompute_events_b = [e for e in self.service.timeline(CTRL("d"), plan_b["id"]) if e["action"] == "recompute"]
        self.assertEqual(1, len(recompute_events_b))  # B 未被重放
        recompute_events_a = [e for e in self.service.timeline(CTRL("d"), plan_a["id"]) if e["action"] == "recompute"]
        self.assertEqual(1, len(recompute_events_a))

        # 每个资源只有一条 held 占用，没有双重占资源
        held = self.service.list_bookings(CTRL("d"), status="held")
        self.assertEqual(4, len(held))
        keys = [(b["resource_type"], b["resource_key"]) for b in held]
        self.assertEqual(len(keys), len(set(keys)))
        # 旧占用行保留为 released 供审计
        released = self.service.list_bookings(CTRL("d"), status="released")
        self.assertGreaterEqual(len(released), 2)
        statuses_a = [d["status"] for d in self.service.list_decisions(CTRL("d"), plan_a["id"])]
        self.assertEqual(["superseded", "active"], statuses_a)

    # 5. 两名调度员同时确认：先到版本放行，后到者保留冲突草稿
    def test_concurrent_confirm_first_wins_loser_keeps_draft(self):
        plan = make_plan("VC", berth="B20", pilot="PX", channel="CX")
        self.service.create(CTRL("creator"), plan["reference"], plan["data"])
        record = next(r for r in self.service.list_records(CTRL("d")) if r["reference"] == "VC")

        first = self.service.confirm_plan(CTRL("disp-a"), record["id"], 1, {"pilot_id": "P10"})
        self.assertEqual(first["state"], "confirmed")
        self.assertEqual(first["payload"]["pilot_id"], "P10")

        # 后到者基于同一旧版本确认 → 不生效，落冲突草稿
        with self.assertRaises(VersionConflict):
            self.service.confirm_plan(CTRL("disp-b"), record["id"], 1, {"pilot_id": "P11"})
        drafts = self.service.list_conflict_drafts(CTRL("d"), status="open")
        self.assertEqual(1, len(drafts))
        self.assertEqual(drafts[0]["payload"], {"pilot_id": "P11"})
        self.assertEqual(drafts[0]["actor_id"], "disp-b")
        # 计划未被后到版本改动；后到版本没有多占一份资源
        self.assertEqual("P10", self.service.get_record(CTRL("d"), record["id"])["payload"]["pilot_id"])
        self.assertEqual(2, len(self.service.list_bookings(CTRL("d"), status="held")))

        # 计划已被确认，草稿只能放弃
        with self.assertRaises(Conflict):
            self.service.resolve_conflict_draft(CTRL("disp-b"), drafts[0]["id"], "reapply")
        resolved = self.service.resolve_conflict_draft(CTRL("disp-b"), drafts[0]["id"], "discard")
        self.assertEqual(resolved["status"], "discarded")

    def test_conflict_draft_reapply_from_held(self):
        # 潮位不足时两人同时确认：先到挂起，后到留草稿；订正重算通过后草稿可重放
        plan = make_plan("VD", berth="B21", pilot="PY", channel="CY")
        self.service.create(CTRL("creator"), plan["reference"], plan["data"])
        rid = self.service.list_records(CTRL("d"))[0]["id"]
        self.service.recovery.ingest_tide_report("obs-1", tide("S1", "T-1", -1.0))
        first = self.service.confirm_plan(CTRL("disp-a"), rid, 1, {"pilot_id": "P20"})
        self.assertEqual(first["state"], "held")
        with self.assertRaises(VersionConflict):
            self.service.confirm_plan(CTRL("disp-b"), rid, 1, {"pilot_id": "P21"})
        draft = self.service.list_conflict_drafts(CTRL("d"), status="open")[0]
        # held 状态可直接重放：旧挂起判定作废，重放版本仍因潮位不足保持挂起、不占资源
        resolved = self.service.resolve_conflict_draft(CTRL("disp-b"), draft["id"], "reapply")
        self.assertEqual(resolved["status"], "reapplied")
        self.assertEqual("held", resolved["applied_record"]["state"])
        self.assertEqual(resolved["applied_record"]["payload"]["pilot_id"], "P21")
        self.assertEqual(0, len(self.service.list_bookings(CTRL("d"), status="held")))
        self.assertEqual(0, len(self.service.list_conflict_drafts(CTRL("d"), status="open")))

        # 报文好转后重算自动放行
        self.service.recovery.ingest_tide_report("obs-1", tide("S1", "T-2", 1.5, seq=2))
        self.assertEqual("confirmed", self.service.get_record(CTRL("d"), rid)["state"])
        self.assertEqual(2, len(self.service.list_bookings(CTRL("d"), status="held")))

    # 6. 资源互斥：同一引航班次/通行证槽位不能重复占用
    def test_resource_busy_blocks_second_confirm(self):
        self.create_and_confirm("VE", berth="B30", pilot="P30", channel="C30")
        plan2 = make_plan("VF", berth="B31", pilot="P30", channel="C31")
        self.service.create(CTRL("creator"), plan2["reference"], plan2["data"])
        rid2 = next(r["id"] for r in self.service.list_records(CTRL("d")) if r["reference"] == "VF")
        with self.assertRaises(ResourceBusy):
            self.service.act(CTRL("d"), rid2, 1, "confirm", {"pilot_id": "P30"})

    # 7. 更旧的订正序号不能覆盖新生效报文
    def test_older_correction_seq_rejected(self):
        self.service.recovery.ingest_tide_report("obs-1", tide("S1", "N-2", 0.5, seq=2))
        with self.assertRaises(Conflict):
            self.service.recovery.ingest_tide_report("obs-1", tide("S1", "N-1", 0.2, seq=1))

    # 8. HTTP 冒烟：报文录入触发运行；并发确认返回 409 version_conflict
    def test_http_smoke(self):
        import time
        httpd = create_server("127.0.0.1", 0, self.service, Path(self.temp.name))
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            def post(path, body, role, user="u1"):
                req = urllib.request.Request(
                    "http://127.0.0.1:%d%s" % (port, path),
                    data=json.dumps(body).encode("utf-8"), method="POST",
                    headers={"Content-Type": "application/json", "X-User-Id": user, "X-Role": role},
                )
                try:
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        return resp.status, json.loads(resp.read().decode("utf-8"))
                except urllib.error.HTTPError as exc:
                    return exc.code, json.loads(exc.read().decode("utf-8"))

            plan = make_plan("VG", berth="B40", pilot="P40", channel="C40")
            status, record = post("/api/records", plan, "port_controller", user="creator")
            self.assertEqual(201, status)
            status, body = post("/api/tide-reports", {"data": tide("S1", "H-1", -3.0)}, "tide_observer")
            self.assertEqual(201, status)
            self.assertIn("recompute_run_id", body)
            # 两名调度员同时确认同一版本
            status, body = post("/api/records/%d/actions/confirm" % record["id"],
                                {"expected_version": 1, "data": {"pilot_id": "P40"}},
                                "port_controller", user="disp-a")
            self.assertEqual(200, status)
            status, body = post("/api/records/%d/actions/confirm" % record["id"],
                                {"expected_version": 1, "data": {"pilot_id": "P41"}},
                                "port_controller", user="disp-b")
            self.assertEqual(409, status)
            self.assertEqual("version_conflict", body["error"])
            time.sleep(0.1)
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
