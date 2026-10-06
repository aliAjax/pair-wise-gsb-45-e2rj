"""水文复核链路的 SQLite 存储层。

与 repository.py 共用同一个数据库文件、同一套连接约定，新增表均带 IF NOT EXISTS，
不影响既有 records 状态机。关键幂等约束全部下放到唯一索引：

- tide_reports(series_id, report_no)：同一测次按报文号只入账一次；
- decision_ledger 的唯一索引：重放同一判定只会撞键，不会产生第二份依据；
- resource_reservations 的活动占用部分索引：同一资源同一时刻只能被一个计划占用，
  而 (kind, resource_id, plan_id) 的普通唯一索引让"释放后重新占用"复用同一行。

所有事务统一走 _tx()：异常时回滚并立即关闭连接，避免持锁连接被异常回溯帧挂住。
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .domain import Conflict, NotFound, ResourceBusy
from .hydrology import PASS


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resource_busy(exc: sqlite3.IntegrityError) -> bool:
    text = str(exc)
    # 部分唯一索引报错可能只给列名，不含索引名，两种形态都识别
    return "resource_reservations" in text or "uq_reservation_active" in text


class HydroRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    @contextmanager
    def _tx(self):
        """写事务：BEGIN IMMEDIATE，正常提交、异常回滚，连接必定关闭。

        sqlite3 自带的 ``with`` 只负责提交/回滚而不关闭连接；块内抛异常时，
        异常回溯帧会把持写锁的连接引用住，后续写操作就会无限等锁。
        """
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
        except BaseException:
            connection.rollback()
            connection.close()
            raise
        else:
            connection.commit()
            connection.close()

    @contextmanager
    def _read(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _init_schema(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS hydro_series (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    sounding_m REAL NOT NULL,
                    observed_hour INTEGER NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tide_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id INTEGER NOT NULL REFERENCES hydro_series(id),
                    report_no TEXT NOT NULL,
                    tide_m REAL NOT NULL,
                    observed_hour INTEGER NOT NULL,
                    corrected INTEGER NOT NULL DEFAULT 0,
                    recv_seq INTEGER NOT NULL,
                    ingested_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(series_id, report_no)
                );
                CREATE TABLE IF NOT EXISTS berth_plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id INTEGER NOT NULL REFERENCES hydro_series(id),
                    name TEXT NOT NULL,
                    draft_m REAL NOT NULL,
                    channel_pass TEXT NOT NULL,
                    pilot_shift TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'draft',
                    version INTEGER NOT NULL DEFAULT 1,
                    current_decision_id INTEGER,
                    frozen_decision_id INTEGER,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS decision_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id INTEGER NOT NULL REFERENCES berth_plans(id),
                    series_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    basis_report_no TEXT,
                    verdict TEXT NOT NULL,
                    provisional INTEGER NOT NULL DEFAULT 0,
                    basis TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'current',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_decision_current
                    ON decision_ledger(plan_id) WHERE status='current';
                CREATE UNIQUE INDEX IF NOT EXISTS uq_decision_frozen
                    ON decision_ledger(plan_id) WHERE status='frozen';
                CREATE UNIQUE INDEX IF NOT EXISTS uq_decision_basis
                    ON decision_ledger(plan_id, kind, basis_report_no);
                CREATE TABLE IF NOT EXISTS resource_reservations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    plan_id INTEGER NOT NULL REFERENCES berth_plans(id),
                    status TEXT NOT NULL DEFAULT 'held',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(kind, resource_id, plan_id)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_reservation_active
                    ON resource_reservations(kind, resource_id) WHERE status='held';
                CREATE TABLE IF NOT EXISTS recompute_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id INTEGER NOT NULL,
                    trigger_report_no TEXT NOT NULL,
                    trigger_recv_seq INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending',
                    last_error TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS recompute_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL REFERENCES recompute_jobs(id),
                    plan_id INTEGER NOT NULL,
                    plan_state TEXT NOT NULL,
                    old_verdict TEXT,
                    frozen_verdict TEXT,
                    decide_state TEXT NOT NULL DEFAULT 'pending',
                    reconcile_state TEXT NOT NULL DEFAULT 'pending',
                    decide_result TEXT,
                    decide_attempts INTEGER NOT NULL DEFAULT 0,
                    reconcile_attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS confirmation_drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id INTEGER NOT NULL REFERENCES berth_plans(id),
                    actor_id TEXT NOT NULL,
                    expected_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    basis TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS hydro_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id INTEGER,
                    job_id INTEGER,
                    kind TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_reports_series ON tide_reports(series_id, id);
                CREATE INDEX IF NOT EXISTS idx_plans_series_state ON berth_plans(series_id, state);
                CREATE INDEX IF NOT EXISTS idx_items_job ON recompute_items(job_id, id);
                """
            )
            self._migrate(connection)
            connection.commit()
        finally:
            connection.close()

    def _migrate(self, connection: sqlite3.Connection) -> None:
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(recompute_items)").fetchall()}
        if "attempts" in columns and "decide_attempts" not in columns:
            connection.execute("ALTER TABLE recompute_items ADD COLUMN decide_attempts INTEGER NOT NULL DEFAULT 0")
            connection.execute("ALTER TABLE recompute_items ADD COLUMN reconcile_attempts INTEGER NOT NULL DEFAULT 0")
            connection.execute("UPDATE recompute_items SET decide_attempts=attempts")

    @staticmethod
    def _row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        for key in ("basis", "payload", "details", "decide_result"):
            if key in item and isinstance(item[key], str):
                item[key] = json.loads(item[key])
        for key in ("corrected", "provisional"):
            if key in item and item[key] is not None:
                item[key] = bool(item[key])
        return item

    # ---- 测次 ----

    def create_series(self, name: str, sounding_m: float, observed_hour: int, actor_id: str) -> Dict[str, Any]:
        now = _now()
        try:
            with self._tx() as connection:
                cursor = connection.execute(
                    "INSERT INTO hydro_series(name,sounding_m,observed_hour,created_by,created_at) VALUES(?,?,?,?,?)",
                    (name, sounding_m, observed_hour, actor_id, now),
                )
                row = connection.execute("SELECT * FROM hydro_series WHERE id=?", (cursor.lastrowid,)).fetchone()
        except sqlite3.IntegrityError as exc:
            raise Conflict("测次名称已存在") from exc
        return dict(row)

    def get_series(self, series_id: int) -> Dict[str, Any]:
        with self._read() as connection:
            row = connection.execute("SELECT * FROM hydro_series WHERE id=?", (series_id,)).fetchone()
        if row is None:
            raise NotFound("测次不存在")
        return dict(row)

    def list_series(self) -> List[Dict[str, Any]]:
        with self._read() as connection:
            rows = connection.execute("SELECT * FROM hydro_series ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    # ---- 潮位报文收件箱（同一测次报文号去重 + 入队复核作业，单事务） ----

    def ingest_report(self, series_id: int, report_no: str, tide_m: float, observed_hour: int,
                      corrected: bool, actor_id: str) -> Dict[str, Any]:
        now = _now()
        with self._tx() as connection:
            series = connection.execute("SELECT * FROM hydro_series WHERE id=?", (series_id,)).fetchone()
            if series is None:
                raise NotFound("测次不存在")
            existing = connection.execute(
                "SELECT * FROM tide_reports WHERE series_id=? AND report_no=?", (series_id, report_no)
            ).fetchone()
            if existing is not None:
                # 同一测次按报文号只入账一次：直接短路，不产生第二条报文、不触发作业
                return {"duplicate": True, "report": self._row(existing), "jobs": []}
            recv_seq = int(connection.execute(
                "SELECT COALESCE(MAX(recv_seq),0)+1 AS seq FROM tide_reports"
            ).fetchone()["seq"])
            cursor = connection.execute(
                "INSERT INTO tide_reports(series_id,report_no,tide_m,observed_hour,corrected,recv_seq,ingested_by,created_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (series_id, report_no, tide_m, observed_hour, 1 if corrected else 0, recv_seq, actor_id, now),
            )
            report = self._row(connection.execute(
                "SELECT * FROM tide_reports WHERE id=?", (cursor.lastrowid,)
            ).fetchone())
            plans = connection.execute(
                "SELECT * FROM berth_plans WHERE series_id=? AND state IN ('released','held','berthed','review') ORDER BY id",
                (series_id,),
            ).fetchall()
            job: Optional[Dict[str, Any]] = None
            if plans:
                job_cursor = connection.execute(
                    "INSERT INTO recompute_jobs(series_id,trigger_report_no,trigger_recv_seq,state,created_by,created_at,updated_at)"
                    " VALUES(?,?,?,'pending',?,?,?)",
                    (series_id, report_no, recv_seq, actor_id, now, now),
                )
                job_id = int(job_cursor.lastrowid)
                items: List[Dict[str, Any]] = []
                for plan in plans:
                    frozen = connection.execute(
                        "SELECT verdict FROM decision_ledger WHERE id=?", (plan["frozen_decision_id"],)
                    ).fetchone() if plan["frozen_decision_id"] is not None else None
                    current = connection.execute(
                        "SELECT verdict FROM decision_ledger WHERE id=?", (plan["current_decision_id"],)
                    ).fetchone() if plan["current_decision_id"] is not None else None
                    ic = connection.execute(
                        "INSERT INTO recompute_items(job_id,plan_id,plan_state,old_verdict,frozen_verdict,created_at,updated_at)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (job_id, plan["id"], plan["state"],
                         current["verdict"] if current else None,
                         frozen["verdict"] if frozen else None, now, now),
                    )
                    items.append({"id": int(ic.lastrowid), "plan_id": plan["id"]})
                connection.execute(
                    "INSERT INTO hydro_events(plan_id,job_id,kind,actor_id,details,created_at) VALUES(?,?,?,?,?,?)",
                    (None, job_id, "report_received", actor_id,
                     json.dumps({"report_no": report_no, "tide_m": tide_m, "corrected": bool(corrected),
                                 "affected": len(plans)}, ensure_ascii=False, sort_keys=True), now),
                )
                job = {"id": job_id, "items": items}
        return {"duplicate": False, "report": report, "jobs": [job] if job else []}

    def latest_report(self, series_id: int) -> Optional[Dict[str, Any]]:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM tide_reports WHERE series_id=? ORDER BY id DESC LIMIT 1", (series_id,)
            ).fetchone()
        return self._row(row) if row else None

    def get_report_by_no(self, series_id: int, report_no: str) -> Dict[str, Any]:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM tide_reports WHERE series_id=? AND report_no=?", (series_id, report_no)
            ).fetchone()
        if row is None:
            raise NotFound("潮位报文不存在")
        return self._row(row)

    def list_reports(self, series_id: int) -> List[Dict[str, Any]]:
        self.get_series(series_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM tide_reports WHERE series_id=? ORDER BY id", (series_id,)
            ).fetchall()
        return [self._row(row) for row in rows]

    # ---- 靠泊计划 ----

    def create_plan(self, series_id: int, name: str, draft_m: float, channel_pass: str,
                    pilot_shift: str, actor_id: str) -> Dict[str, Any]:
        now = _now()
        with self._tx() as connection:
            if connection.execute("SELECT 1 FROM hydro_series WHERE id=?", (series_id,)).fetchone() is None:
                raise NotFound("测次不存在")
            cursor = connection.execute(
                "INSERT INTO berth_plans(series_id,name,draft_m,channel_pass,pilot_shift,state,version,created_by,updated_by,created_at,updated_at)"
                " VALUES(?,?,?,?,?,'draft',1,?,?,?,?)",
                (series_id, name, draft_m, channel_pass, pilot_shift, actor_id, actor_id, now, now),
            )
            row = connection.execute("SELECT * FROM berth_plans WHERE id=?", (cursor.lastrowid,)).fetchone()
        return dict(row)

    def get_plan(self, plan_id: int) -> Dict[str, Any]:
        with self._read() as connection:
            row = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFound("计划不存在")
        return dict(row)

    def list_plans(self, series_id: Optional[int] = None, state: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM berth_plans"
        clauses, params = [], []
        if series_id is not None:
            clauses.append("series_id=?")
            params.append(series_id)
        if state:
            clauses.append("state=?")
            params.append(state)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        with self._read() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    # ---- 判定台账 ----

    def insert_decision(self, plan_id: int, series_id: int, kind: str, basis_report_no: Optional[str],
                        verdict: str, provisional: bool, basis: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        now = _now()
        try:
            with self._tx() as connection:
                cursor = connection.execute(
                    "INSERT INTO decision_ledger(plan_id,series_id,kind,basis_report_no,verdict,provisional,basis,status,created_by,created_at)"
                    " VALUES(?,?,?,?,?,?,?, 'current', ?,?)",
                    (plan_id, series_id, kind, basis_report_no, verdict, 1 if provisional else 0,
                     json.dumps(basis, ensure_ascii=False, sort_keys=True), actor_id, now),
                )
                row = connection.execute("SELECT * FROM decision_ledger WHERE id=?", (cursor.lastrowid,)).fetchone()
        except sqlite3.IntegrityError as exc:
            raise Conflict("判定重复入账，已按幂等忽略") from exc
        return self._row(row)

    def find_decision(self, plan_id: int, kind: str, basis_report_no: Optional[str]) -> Optional[Dict[str, Any]]:
        with self._read() as connection:
            if basis_report_no is None:
                row = connection.execute(
                    "SELECT * FROM decision_ledger WHERE plan_id=? AND kind=? AND basis_report_no IS NULL ORDER BY id",
                    (plan_id, kind),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM decision_ledger WHERE plan_id=? AND kind=? AND basis_report_no=?",
                    (plan_id, kind, basis_report_no),
                ).fetchone()
        return self._row(row) if row else None

    def find_status_decision(self, plan_id: int, status: str) -> Optional[Dict[str, Any]]:
        with self._read() as connection:
            row = connection.execute(
                "SELECT * FROM decision_ledger WHERE plan_id=? AND status=? ORDER BY id DESC LIMIT 1",
                (plan_id, status),
            ).fetchone()
        return self._row(row) if row else None

    def list_decisions(self, plan_id: int) -> List[Dict[str, Any]]:
        self.get_plan(plan_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM decision_ledger WHERE plan_id=? ORDER BY id", (plan_id,)
            ).fetchall()
        return [self._row(row) for row in rows]

    # ---- 资源占用 ----

    def hold_reservation(self, kind: str, resource_id: str, plan_id: int) -> Dict[str, Any]:
        """占用（或复用释放过的本计划行）。撞活动占用唯一索引即 ResourceBusy。"""
        now = _now()
        try:
            with self._tx() as connection:
                connection.execute(
                    "INSERT INTO resource_reservations(kind,resource_id,plan_id,status,created_at,updated_at)"
                    " VALUES(?,?,?,'held',?,?)"
                    " ON CONFLICT(kind,resource_id,plan_id) DO UPDATE SET"
                    " status='held', updated_at=excluded.updated_at",
                    (kind, resource_id, plan_id, now, now),
                )
                row = connection.execute(
                    "SELECT * FROM resource_reservations WHERE kind=? AND resource_id=? AND plan_id=?",
                    (kind, resource_id, plan_id),
                ).fetchone()
        except sqlite3.IntegrityError as exc:
            if _resource_busy(exc):
                raise ResourceBusy("%s %s 已被其他计划占用" % (kind, resource_id)) from exc
            raise
        return dict(row)

    def release_plan_tx(self, plan: Dict[str, Any], decision_basis: Dict[str, Any],
                        channel_pass: str, pilot_shift: str, actor_id: str) -> Dict[str, Any]:
        """确认放行整事务：版本CAS -> 写放行依据 -> 占用两类资源 -> released。

        任一步失败整体回滚，不会留下"已放行但没占资源"或"只占了一半"的中间态。
        """
        now = _now()
        plan_id = int(plan["id"])
        series_id = int(plan["series_id"])
        expected_version = int(plan["expected_version"])
        provisional = bool(decision_basis["provisional"])
        with self._tx() as connection:
            row = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
            if row is None:
                raise NotFound("计划不存在")
            if int(row["version"]) != expected_version:
                if row["state"] == "draft":
                    # 计划尚未被确认、只是版本号过期：普通乐观锁冲突，调用方刷新可重试
                    return {"outcome": "stale"}
                # 状态已离开 draft：被并发的先到确认放行/滞留，后到版本保留为冲突草稿
                return {"outcome": "conflict", "plan": dict(row)}
            if row["state"] != "draft":
                return {"outcome": "conflict", "plan": dict(row)}
            cursor = connection.execute(
                "INSERT INTO decision_ledger(plan_id,series_id,kind,basis_report_no,verdict,provisional,basis,status,created_by,created_at)"
                " VALUES(?,?,?,?,?,?,?, 'current', ?,?)",
                (plan_id, series_id, "release", decision_basis.get("basis_report_no"),
                 decision_basis["verdict"], 1 if provisional else 0,
                 json.dumps(decision_basis, ensure_ascii=False, sort_keys=True), actor_id, now),
            )
            decision_id = int(cursor.lastrowid)
            for kind, resource_id in (("channel_pass", channel_pass), ("pilot_shift", pilot_shift)):
                try:
                    connection.execute(
                        "INSERT INTO resource_reservations(kind,resource_id,plan_id,status,created_at,updated_at)"
                        " VALUES(?,?,?,'held',?,?)"
                        " ON CONFLICT(kind,resource_id,plan_id) DO UPDATE SET"
                        " status='held', updated_at=excluded.updated_at",
                        (kind, resource_id, plan_id, now, now),
                    )
                except sqlite3.IntegrityError as exc:
                    if _resource_busy(exc):
                        raise ResourceBusy("航道通行证或引航员班次已被占用") from exc
                    raise
            connection.execute(
                "UPDATE berth_plans SET state='released', version=version+1, current_decision_id=?,"
                " updated_by=?, updated_at=? WHERE id=?",
                (decision_id, actor_id, now, plan_id),
            )
            connection.execute(
                "INSERT INTO hydro_events(plan_id,job_id,kind,actor_id,details,created_at) VALUES(?,?,?,?,?,?)",
                (plan_id, None, "released", actor_id,
                 json.dumps({"decision_id": decision_id, "channel_pass": channel_pass,
                             "pilot_shift": pilot_shift, "provisional": provisional},
                            ensure_ascii=False, sort_keys=True), now),
            )
            result = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
        return {"outcome": "released", "plan": dict(result), "decision_id": decision_id}

    def lifecycle_tx(self, plan_id: int, expected_states, new_state: str, actor_id: str,
                     action: str, release_resources: bool = False,
                     freeze: bool = False, clear_frozen: bool = False) -> Optional[Dict[str, Any]]:
        """靠泊/离泊/取消共用的状态CAS事务；状态不符返回 None。"""
        now = _now()
        with self._tx() as connection:
            row = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
            if row is None:
                raise NotFound("计划不存在")
            if row["state"] not in expected_states:
                return None
            sets = ["state=?", "version=version+1", "updated_by=?", "updated_at=?"]
            params: List[Any] = [new_state, actor_id, now]
            if freeze:
                # 开始靠泊：把当时的放行依据冻结（current -> frozen），保留待复核
                connection.execute(
                    "UPDATE decision_ledger SET status='frozen' WHERE plan_id=? AND status='current'",
                    (plan_id,),
                )
                sets.append("frozen_decision_id=current_decision_id")
                sets.append("current_decision_id=NULL")
            if clear_frozen:
                sets.append("frozen_decision_id=NULL")
            if release_resources:
                connection.execute(
                    "UPDATE resource_reservations SET status='released', updated_at=? WHERE plan_id=? AND status='held'",
                    (now, plan_id),
                )
            params.append(plan_id)
            connection.execute("UPDATE berth_plans SET %s WHERE id=?" % ", ".join(sets), params)
            connection.execute(
                "INSERT INTO hydro_events(plan_id,job_id,kind,actor_id,details,created_at) VALUES(?,?,?,?,?,?)",
                (plan_id, None, action, actor_id,
                 json.dumps({"from": row["state"], "to": new_state,
                             "resources_released": release_resources}, ensure_ascii=False, sort_keys=True), now),
            )
            result = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
        return dict(result)

    def reconcile_item_tx(self, item: Dict[str, Any], decision_basis: Dict[str, Any],
                          actor_id: str, job_id: int, failure_hook=None) -> Dict[str, Any]:
        """重算对账段整事务（未靠泊分支）：原判定失效 -> 按新判定改状态/调资源。

        靠泊分支的建议性判定在判定段直接写库；本事务只处理 not_berthed 计划。
        decision_id 幂等：(plan, kind=recompute, basis_report_no) 已存在则整段跳过，
        保证作业重放不会重复占用资源。
        """
        now = _now()
        plan_id = int(item["plan_id"])
        series_id = int(item["series_id"])
        report_no = decision_basis.get("basis_report_no")
        verdict = decision_basis["verdict"]
        with self._tx() as connection:
            existing = connection.execute(
                "SELECT * FROM decision_ledger WHERE plan_id=? AND kind='recompute' AND basis_report_no=?",
                (plan_id, report_no),
            ).fetchone()
            if existing is not None:
                # 同一报文的对账已落过账：整段重放为 noop，资源不会再动一次
                return {"outcome": "replayed", "plan_id": plan_id}
            plan_row = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
            if plan_row is None:
                raise NotFound("计划不存在")
            stage_state = plan_row["state"]
            if stage_state in ("departed", "cancelled"):
                return {"outcome": "noop", "plan_id": plan_id, "state": stage_state}
            if stage_state in ("berthed", "review"):
                # 靠泊分支不应进入本事务（判定段已处理）
                return {"outcome": "advisory_only", "plan_id": plan_id, "state": stage_state}
            # 让未靠泊计划的原放行/滞留判定失效
            connection.execute(
                "UPDATE decision_ledger SET status='superseded' WHERE plan_id=? AND status='current'",
                (plan_id,),
            )
            cursor = connection.execute(
                "INSERT INTO decision_ledger(plan_id,series_id,kind,basis_report_no,verdict,provisional,basis,status,created_by,created_at)"
                " VALUES(?,?,?,?,?,?,?, 'current', ?,?)",
                (plan_id, series_id, "recompute", report_no, verdict,
                 1 if bool(decision_basis["provisional"]) else 0,
                 json.dumps(decision_basis, ensure_ascii=False, sort_keys=True), actor_id, now),
            )
            decision_id = int(cursor.lastrowid)
            if failure_hook is not None:
                # 模拟"原判定已失效、资源尚未调整"的中途失败；事务回滚后可整段重放
                failure_hook("reconcile.before_resources", plan_id)
            if verdict == PASS:
                for kind, resource_id in (("channel_pass", plan_row["channel_pass"]),
                                          ("pilot_shift", plan_row["pilot_shift"])):
                    try:
                        connection.execute(
                            "INSERT INTO resource_reservations(kind,resource_id,plan_id,status,created_at,updated_at)"
                            " VALUES(?,?,?,'held',?,?)"
                            " ON CONFLICT(kind,resource_id,plan_id) DO UPDATE SET"
                            " status='held', updated_at=excluded.updated_at",
                            (kind, resource_id, plan_id, now, now),
                        )
                    except sqlite3.IntegrityError as exc:
                        if "uq_reservation_active" in str(exc):
                            raise ResourceBusy("航道通行证或引航员班次已被占用") from exc
                        raise
                connection.execute(
                    "UPDATE berth_plans SET state='released', version=version+1, current_decision_id=?,"
                    " updated_by=?, updated_at=? WHERE id=?",
                    (decision_id, actor_id, now, plan_id),
                )
                outcome = "released"
            else:
                connection.execute(
                    "UPDATE resource_reservations SET status='released', updated_at=? WHERE plan_id=? AND status='held'",
                    (now, plan_id),
                )
                connection.execute(
                    "UPDATE berth_plans SET state='held', version=version+1, current_decision_id=?,"
                    " updated_by=?, updated_at=? WHERE id=?",
                    (decision_id, actor_id, now, plan_id),
                )
                outcome = "held"
            connection.execute(
                "INSERT INTO hydro_events(plan_id,job_id,kind,actor_id,details,created_at) VALUES(?,?,?,?,?,?)",
                (plan_id, job_id, "recomputed", actor_id,
                 json.dumps({"report_no": report_no, "verdict": verdict, "outcome": outcome,
                             "decision_id": decision_id}, ensure_ascii=False, sort_keys=True), now),
            )
            result = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
        return {"outcome": outcome, "plan_id": plan_id, "state": dict(result)["state"], "decision_id": decision_id}

    def advisory_decision_tx(self, item: Dict[str, Any], decision_basis: Dict[str, Any],
                             actor_id: str, job_id: int) -> Dict[str, Any]:
        """靠泊分支：保留当时依据（frozen），新判定只作 advisory；结论反转才挂待复核。"""
        now = _now()
        plan_id = int(item["plan_id"])
        series_id = int(item["series_id"])
        report_no = decision_basis.get("basis_report_no")
        with self._tx() as connection:
            existing = connection.execute(
                "SELECT * FROM decision_ledger WHERE plan_id=? AND kind='recompute' AND basis_report_no=?",
                (plan_id, report_no),
            ).fetchone() if report_no else None
            if existing is not None:
                return {"outcome": "replayed", "plan_id": plan_id}
            plan_row = connection.execute("SELECT * FROM berth_plans WHERE id=?", (plan_id,)).fetchone()
            if plan_row is None:
                raise NotFound("计划不存在")
            cursor = connection.execute(
                "INSERT INTO decision_ledger(plan_id,series_id,kind,basis_report_no,verdict,provisional,basis,status,created_by,created_at)"
                " VALUES(?,?,?,?,?,?,?, 'advisory', ?,?)",
                (plan_id, series_id, "recompute", report_no, decision_basis["verdict"],
                 1 if bool(decision_basis["provisional"]) else 0,
                 json.dumps(decision_basis, ensure_ascii=False, sort_keys=True), actor_id, now),
            )
            decision_id = int(cursor.lastrowid)
            frozen_verdict = None
            if plan_row["frozen_decision_id"] is not None:
                frozen = connection.execute(
                    "SELECT * FROM decision_ledger WHERE id=?", (plan_row["frozen_decision_id"],)
                ).fetchone()
                frozen_verdict = frozen["verdict"] if frozen is not None else None
            needs_review = frozen_verdict != decision_basis["verdict"]
            new_state = plan_row["state"]
            if needs_review and new_state == "berthed":
                new_state = "review"
                connection.execute(
                    "UPDATE berth_plans SET state='review', version=version+1, updated_by=?, updated_at=? WHERE id=?",
                    (actor_id, now, plan_id),
                )
            connection.execute(
                "INSERT INTO hydro_events(plan_id,job_id,kind,actor_id,details,created_at) VALUES(?,?,?,?,?,?)",
                (plan_id, job_id, "advisory", actor_id,
                 json.dumps({"report_no": report_no, "verdict": decision_basis["verdict"],
                             "frozen_verdict": frozen_verdict, "needs_review": needs_review,
                             "decision_id": decision_id}, ensure_ascii=False, sort_keys=True), now),
            )
        return {"outcome": "advisory", "plan_id": plan_id, "state": new_state, "needs_review": needs_review,
                "decision_id": decision_id}

    def release_reservations_for_plan(self, plan_id: int) -> int:
        now = _now()
        with self._tx() as connection:
            cursor = connection.execute(
                "UPDATE resource_reservations SET status='released', updated_at=? WHERE plan_id=? AND status='held'",
                (now, plan_id),
            )
            return cursor.rowcount

    def list_reservations(self, plan_id: int) -> List[Dict[str, Any]]:
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM resource_reservations WHERE plan_id=? ORDER BY id", (plan_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    # ---- 复核作业（可恢复 saga：每计划一项，判定/对账两段，分段断点） ----

    def get_job(self, job_id: int) -> Dict[str, Any]:
        with self._read() as connection:
            row = connection.execute("SELECT * FROM recompute_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFound("复核作业不存在")
        return dict(row)

    def list_jobs(self, series_id: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._read() as connection:
            if series_id is None:
                rows = connection.execute("SELECT * FROM recompute_jobs ORDER BY id").fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM recompute_jobs WHERE series_id=? ORDER BY id", (series_id,)
                ).fetchall()
        return [dict(row) for row in rows]

    def list_items(self, job_id: int) -> List[Dict[str, Any]]:
        self.get_job(job_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM recompute_items WHERE job_id=? ORDER BY id", (job_id,)
            ).fetchall()
        return [self._row(row) for row in rows]

    def claim_item(self, job_id: int) -> Optional[Dict[str, Any]]:
        """领取一个仍有未完成段的作业项，并把待跑段置 running。"""
        now = _now()
        with self._tx() as connection:
            row = connection.execute(
                "SELECT * FROM recompute_items WHERE job_id=?"
                " AND (decide_state IN ('pending','failed') OR reconcile_state IN ('pending','failed'))"
                " ORDER BY id LIMIT 1",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            item = self._row(row)
            if item["decide_state"] in ("pending", "failed"):
                connection.execute(
                    "UPDATE recompute_items SET decide_state='running',"
                    " decide_attempts=decide_attempts+1, last_error=NULL, updated_at=? WHERE id=?",
                    (now, row["id"]),
                )
                item["_stage"] = "decide"
            else:
                connection.execute(
                    "UPDATE recompute_items SET reconcile_state='running',"
                    " reconcile_attempts=reconcile_attempts+1, last_error=NULL, updated_at=? WHERE id=?",
                    (now, row["id"]),
                )
                item["_stage"] = "reconcile"
            connection.execute("UPDATE recompute_jobs SET state='running', updated_at=? WHERE id=?", (now, job_id))
        return item

    def finish_stage(self, item_id: int, stage: str, state: str, result: Optional[Dict[str, Any]] = None,
                     error: Optional[str] = None) -> None:
        now = _now()
        column = "decide_state" if stage == "decide" else "reconcile_state"
        with self._tx() as connection:
            if stage == "decide" and result is not None:
                connection.execute(
                    "UPDATE recompute_items SET %s=?, decide_result=?, last_error=?, updated_at=? WHERE id=?" % column,
                    (state, json.dumps(result, ensure_ascii=False, sort_keys=True), error, now, item_id),
                )
            else:
                connection.execute(
                    "UPDATE recompute_items SET %s=?, last_error=?, updated_at=? WHERE id=?" % column,
                    (state, error, now, item_id),
                )
            self._refresh_job_state(connection, item_id, now)

    @staticmethod
    def _refresh_job_state(connection: sqlite3.Connection, item_id: Optional[int], now: str) -> None:
        if item_id is None:
            job_rows = connection.execute("SELECT id FROM recompute_jobs").fetchall()
            job_ids = [int(job["id"]) for job in job_rows]
        else:
            job_row = connection.execute("SELECT job_id FROM recompute_items WHERE id=?", (item_id,)).fetchone()
            job_ids = [int(job_row["job_id"])] if job_row else []
        for job_id in job_ids:
            pending = connection.execute(
                "SELECT COUNT(*) AS n FROM recompute_items WHERE job_id=?"
                " AND (decide_state IN ('pending','running','failed') OR reconcile_state IN ('pending','running','failed'))",
                (job_id,),
            ).fetchone()["n"]
            failed = connection.execute(
                "SELECT COUNT(*) AS n FROM recompute_items WHERE job_id=?"
                " AND (decide_state='failed' OR reconcile_state='failed')",
                (job_id,),
            ).fetchone()["n"]
            new_state = "completed" if pending == 0 else ("failed" if failed else "running")
            connection.execute(
                "UPDATE recompute_jobs SET state=?, updated_at=? WHERE id=?",
                (new_state, now, job_id),
            )

    def refresh_all_jobs(self) -> None:
        now = _now()
        with self._tx() as connection:
            self._refresh_job_state(connection, None, now)

    # ---- 冲突草稿 ----

    def insert_draft(self, plan_id: int, actor_id: str, expected_version: int,
                     payload: Dict[str, Any], basis: Dict[str, Any], reason: str) -> Dict[str, Any]:
        now = _now()
        with self._tx() as connection:
            cursor = connection.execute(
                "INSERT INTO confirmation_drafts(plan_id,actor_id,expected_version,payload,basis,reason,created_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (plan_id, actor_id, expected_version,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 json.dumps(basis, ensure_ascii=False, sort_keys=True), reason, now),
            )
            row = connection.execute("SELECT * FROM confirmation_drafts WHERE id=?", (cursor.lastrowid,)).fetchone()
        return self._row(row)

    def list_drafts(self, plan_id: int) -> List[Dict[str, Any]]:
        self.get_plan(plan_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM confirmation_drafts WHERE plan_id=? ORDER BY id", (plan_id,)
            ).fetchall()
        return [self._row(row) for row in rows]

    # ---- 事件 ----

    def add_event(self, kind: str, actor_id: str, details: Dict[str, Any],
                  plan_id: Optional[int] = None, job_id: Optional[int] = None) -> None:
        with self._tx() as connection:
            connection.execute(
                "INSERT INTO hydro_events(plan_id,job_id,kind,actor_id,details,created_at) VALUES(?,?,?,?,?,?)",
                (plan_id, job_id, kind, actor_id,
                 json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
            )

    def list_events(self, plan_id: int) -> List[Dict[str, Any]]:
        self.get_plan(plan_id)
        with self._read() as connection:
            rows = connection.execute(
                "SELECT * FROM hydro_events WHERE plan_id=? ORDER BY id", (plan_id,)
            ).fetchall()
        return [self._row(row) for row in rows]
