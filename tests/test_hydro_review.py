"""水文可恢复复核链路测试：报文去重、迟到/订正重算、靠泊冻结、
分段失败重试、重放幂等不重复占资源、双调度员并发裁决。"""
import tempfile
import threading
import unittest
from pathlib import Path

from app import build_hydro_service
from src.domain import Actor, Conflict, ReleaseRejected, ResourceBusy
from src.hydrology import HydroRules, evaluate

DISPATCHER = Actor("dispatcher-a", "port_controller")
DISPATCHER_B = Actor("dispatcher-b", "port_controller")
OBSERVER = Actor("tide-station", "tide_observer")


class HydroCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "hydro.db")
        self.service = build_hydro_service(self.db)
        # 录入水深 10.0 米的测次
        self.series = self.service.create_series(
            DISPATCHER, {"name": "CH-N1", "sounding_m": 10.0, "observed_hour": 22}
        )

    def tearDown(self):
        self.temp.cleanup()

    def make_plan(self, draft=9.4, channel="CH-PASS-1", shift="PILOT-NIGHT-1", name="MV-HAIYUN"):
        return self.service.create_plan(
            DISPATCHER, self.series["id"],
            {"name": name, "draft_m": draft, "channel_pass": channel, "pilot_shift": shift},
        )

    def release(self, plan, actor=DISPATCHER):
        return self.service.release(actor, plan["id"], plan["version"], {})


class RuleTest(unittest.TestCase):
    def test_effective_depth_and_provisional(self):
        no_tide = evaluate(10.0, 9.4, None)
        self.assertTrue(no_tide["provisional"])
        self.assertEqual(no_tide["effective_depth_m"], 10.0)
        self.assertEqual(no_tide["verdict"], "pass")  # 10.0 >= 9.4+0.5
        low = evaluate(10.0, 9.4, -0.2)
        self.assertFalse(low["provisional"])
        self.assertEqual(low["effective_depth_m"], 9.8)
        self.assertEqual(low["verdict"], "fail")
        high = evaluate(10.0, 9.4, 0.6)
        self.assertEqual(high["verdict"], "pass")


class IngestTest(HydroCase):
    def test_same_report_number_books_once(self):
        first = self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-001", "tide_m": 0.6, "observed_hour": 23},
        )
        self.assertFalse(first["duplicate"])
        replay = self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-001", "tide_m": 9.9, "observed_hour": 23},  # 内容被篡改也忽略
        )
        self.assertTrue(replay["duplicate"])
        reports = self.service.repository.list_reports(self.series["id"])
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["tide_m"], 0.6)  # 以首次入账为准

    def test_late_report_invalidates_provisional_verdict_and_recomputes(self):
        # 报文未到：按录入水深暂判放行，通行证/引航班次被占用
        plan = self.make_plan()
        self.release(plan)
        ledger = self.service.decisions(DISPATCHER, plan["id"])
        self.assertEqual(len(ledger), 1)
        self.assertTrue(ledger[0]["provisional"])
        self.assertEqual(ledger[0]["status"], "current")

        # 迟到报文：低潮，原暂判失效 -> 重算不通过 -> 撤回、释放资源
        result = self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-001", "tide_m": -0.2, "observed_hour": 23},
        )
        self.assertFalse(result["duplicate"])
        job = result["jobs"][0]
        self.assertEqual(job["state"], "completed")

        reloaded = self.service.repository.get_plan(plan["id"])
        self.assertEqual(reloaded["state"], "held")
        ledger = self.service.decisions(DISPATCHER, plan["id"])
        self.assertEqual([d["status"] for d in ledger], ["superseded", "current"])
        self.assertEqual(ledger[1]["verdict"], "fail")
        reservations = self.service.reservations(DISPATCHER, plan["id"])
        self.assertTrue(all(r["status"] == "released" for r in reservations))

    def test_correction_flips_back_to_pass_and_reholds_resources(self):
        plan = self.make_plan()
        self.release(plan)
        self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-001", "tide_m": -0.2, "observed_hour": 23},
        )
        self.assertEqual(self.service.repository.get_plan(plan["id"])["state"], "held")

        # 订正报文：潮位由 -0.2 订正为 +0.6，重算通过 -> 重新放行、重新占用
        corrected = self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-002", "tide_m": 0.6, "observed_hour": 23, "corrected": True},
        )
        self.assertEqual(corrected["jobs"][0]["state"], "completed")
        reloaded = self.service.repository.get_plan(plan["id"])
        self.assertEqual(reloaded["state"], "released")
        ledger = self.service.decisions(DISPATCHER, plan["id"])
        statuses = [(d["kind"], d["status"], d["verdict"], d["basis_report_no"]) for d in ledger]
        self.assertEqual(statuses, [
            ("release", "superseded", "pass", None),
            ("recompute", "superseded", "fail", "T-001"),
            ("recompute", "current", "pass", "T-002"),
        ])
        reservations = self.service.reservations(DISPATCHER, plan["id"])
        held = [r for r in reservations if r["status"] == "held"]
        self.assertEqual(len(held), 2)  # 通行证与引航班次重新占用，各一行


class BerthedReviewTest(HydroCase):
    def test_berthed_plan_keeps_basis_and_waits_review(self):
        plan = self.make_plan()
        self.release(plan)
        self.service.mark_berth(DISPATCHER, plan["id"])
        reloaded = self.service.repository.get_plan(plan["id"])
        self.assertEqual(reloaded["state"], "berthed")

        # 报文到达且结论反转：已开始靠泊，依据冻结保留、资源继续占用、挂待复核
        self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-001", "tide_m": -0.2, "observed_hour": 23},
        )
        reloaded = self.service.repository.get_plan(plan["id"])
        self.assertEqual(reloaded["state"], "review")
        ledger = self.service.decisions(DISPATCHER, plan["id"])
        frozen = [d for d in ledger if d["status"] == "frozen"]
        advisory = [d for d in ledger if d["status"] == "advisory"]
        self.assertEqual(len(frozen), 1)
        self.assertEqual(frozen[0]["verdict"], "pass")  # 当时依据原样保留
        self.assertEqual(len(advisory), 1)
        self.assertEqual(advisory[0]["verdict"], "fail")
        reservations = self.service.reservations(DISPATCHER, plan["id"])
        self.assertTrue(all(r["status"] == "held" for r in reservations))

        # 再来一份结论相同的报文：仍为 review，不再改状态，但保留新的 advisory 记录
        self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-002", "tide_m": -0.3, "observed_hour": 23},
        )
        self.assertEqual(self.service.repository.get_plan(plan["id"])["state"], "review")
        self.assertEqual(len([d for d in self.service.decisions(DISPATCHER, plan["id"]) if d["status"] == "advisory"]), 2)


class ResumeTest(HydroCase):
    def _failing_service(self, fail_plan_id):
        calls = []

        def hook(stage, plan_id):
            if int(plan_id) == int(fail_plan_id) and stage == "reconcile.before_resources":
                calls.append(1)
                if len(calls) == 1:
                    raise RuntimeError("模拟对账段中途失败（资源调整前）")

        from src.hydro_repository import HydroRepository
        from src.hydrology import HydroRules
        from src.hydro_service import HydroService
        return HydroService(HydroRepository(self.db), HydroRules(), failure_hook=hook), calls

    def test_failed_item_retries_only_unfinished_stages(self):
        plan = self.make_plan()
        self.release(plan)
        # 先入报文建作业但不自动执行：构造一个待处理作业
        ingested = self.service.repository.ingest_report(
            self.series["id"], "T-001", -0.2, 23, False, "tide-station"
        )
        job_id = ingested["jobs"][0]["id"]

        failing, calls = self._failing_service(plan["id"])
        detail = failing.run_job(job_id)
        self.assertEqual(detail["state"], "failed")
        item = detail["items"][0]
        self.assertEqual(item["decide_state"], "done")
        self.assertEqual(item["reconcile_state"], "failed")
        # 判定段失败后计划仍保持 released（资源未动）
        self.assertEqual(self.service.repository.get_plan(plan["id"])["state"], "released")

        # 恢复：只重试失败的对账段，判定段不重跑
        recovered = self.service.run_job(job_id)
        self.assertEqual(recovered["state"], "completed")
        item = recovered["items"][0]
        self.assertEqual(item["decide_state"], "done")
        self.assertEqual(item["reconcile_state"], "done")
        self.assertEqual(self.service.repository.get_plan(plan["id"])["state"], "held")

    def test_replay_does_not_double_hold_resources(self):
        plan = self.make_plan()
        self.release(plan)
        ingested = self.service.repository.ingest_report(
            self.series["id"], "T-001", 0.6, 23, False, "tide-station"  # 仍通过
        )
        job_id = ingested["jobs"][0]["id"]
        self.service.run_job(job_id)
        # 作业完成后再重放整作业：全部项已完成，无动作
        again = self.service.run_job(job_id)
        self.assertEqual(again["state"], "completed")
        reservations = self.service.reservations(DISPATCHER, plan["id"])
        self.assertEqual(len(reservations), 2)  # 通行证/引航班次各只占一行
        self.assertTrue(all(r["status"] == "held" for r in reservations))
        ledger = self.service.decisions(DISPATCHER, plan["id"])
        recomputes = [d for d in ledger if d["kind"] == "recompute"]
        self.assertEqual(len(recomputes), 1)  # 重放不会重复入账

    def test_released_resources_block_other_plan(self):
        plan = self.make_plan(name="MV-A")
        self.release(plan)
        other = self.make_plan(name="MV-B", channel="CH-PASS-2", shift="PILOT-NIGHT-2")
        # 用覆盖参数去抢已被 MV-A 占用的通行证和引航班次
        with self.assertRaises(ResourceBusy):
            self.service.release(DISPATCHER, other["id"], other["version"],
                                 {"channel_pass": "CH-PASS-1", "pilot_shift": "PILOT-NIGHT-1"})
        # 占用失败不产生判定、不留草稿状态，计划仍是 draft
        self.assertEqual(self.service.repository.get_plan(other["id"])["state"], "draft")
        self.assertEqual(self.service.decisions(DISPATCHER, other["id"]), [])


class ConcurrentConfirmTest(HydroCase):
    def test_two_dispatchers_first_wins_loser_keeps_draft(self):
        plan = self.make_plan()
        outcomes = {}
        barrier = threading.Barrier(2)

        def confirm(actor, key):
            fresh = self.service.repository.get_plan(plan["id"])
            barrier.wait()
            try:
                self.service.release(actor, plan["id"], fresh["version"], {})
                outcomes[key] = "released"
            except Conflict as exc:
                outcomes[key] = getattr(exc, "draft", None)

        t1 = threading.Thread(target=confirm, args=(DISPATCHER, "a"))
        t2 = threading.Thread(target=confirm, args=(DISPATCHER_B, "b"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        winners = [key for key, value in outcomes.items() if value == "released"]
        losers = [key for key, value in outcomes.items() if isinstance(value, dict)]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        reloaded = self.service.repository.get_plan(plan["id"])
        self.assertEqual(reloaded["state"], "released")
        self.assertEqual(reloaded["version"], 2)  # 只放行过一次
        drafts = self.service.drafts(DISPATCHER, plan["id"])
        self.assertEqual(len(drafts), 1)
        self.assertEqual(drafts[0]["actor_id"], "dispatcher-b" if losers[0] == "b" else "dispatcher-a")
        reservations = self.service.reservations(DISPATCHER, plan["id"])
        self.assertEqual(len([r for r in reservations if r["status"] == "held"]), 2)
        ledger = self.service.decisions(DISPATCHER, plan["id"])
        self.assertEqual(len(ledger), 1)  # 只有先到版本的放行依据

    def test_stale_version_rejected_without_draft(self):
        plan = self.make_plan()
        self.release(plan)
        # 已被先到确认放行（state 离开 draft）：后到者按冲突拒绝并保留草稿
        with self.assertRaises(Conflict):
            self.service.release(DISPATCHER_B, plan["id"], plan["version"], {})
        drafts = self.service.drafts(DISPATCHER, plan["id"])
        self.assertEqual(len(drafts), 1)

    def test_outright_stale_version_raises_plain_conflict(self):
        plan = self.make_plan()
        # 直接把版本号推进但状态仍为 draft（模拟期间有其他更新）：纯乐观锁冲突
        import sqlite3
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE berth_plans SET version=version+1 WHERE id=?", (plan["id"],))
        conn.commit()
        conn.close()
        with self.assertRaises(Conflict):
            self.service.release(DISPATCHER_B, plan["id"], plan["version"], {})
        self.assertEqual(self.service.drafts(DISPATCHER, plan["id"]), [])


class ReleaseGateTest(HydroCase):
    def test_low_tide_blocks_release_and_resources(self):
        plan = self.make_plan()
        self.service.ingest_tide_report(
            OBSERVER, self.series["id"],
            {"report_no": "T-001", "tide_m": -0.2, "observed_hour": 23},
        )
        # 报文已到且不通过：拒绝放行，不占资源、不写依据
        with self.assertRaises(ReleaseRejected):
            self.release(plan)
        self.assertEqual(self.service.repository.get_plan(plan["id"])["state"], "draft")
        self.assertEqual(self.service.reservations(DISPATCHER, plan["id"]), [])
        self.assertEqual(self.service.decisions(DISPATCHER, plan["id"]), [])

    def test_duplicate_report_does_not_touch_other_series(self):
        other_series = self.service.create_series(
            DISPATCHER, {"name": "CH-N2", "sounding_m": 11.0, "observed_hour": 22}
        )
        payload = {"report_no": "T-001", "tide_m": 0.6, "observed_hour": 23}
        first = self.service.ingest_tide_report(OBSERVER, self.series["id"], payload)
        second_series = self.service.ingest_tide_report(OBSERVER, other_series["id"], payload)
        self.assertFalse(first["duplicate"])
        self.assertFalse(second_series["duplicate"])  # 报文号作用域为测次


if __name__ == "__main__":
    unittest.main()
