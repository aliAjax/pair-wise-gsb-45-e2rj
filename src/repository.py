"""SQLite 表结构与事务访问。"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .domain import Conflict, NotFound, ResourceBusy, VersionConflict


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reference TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER REFERENCES records(id) ON DELETE CASCADE,
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_records_state ON records(state);
                CREATE INDEX IF NOT EXISTS idx_audit_record ON audit_events(record_id, id);

                CREATE TABLE IF NOT EXISTS tide_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id TEXT NOT NULL,
                    message_no TEXT NOT NULL,
                    correction_seq INTEGER NOT NULL DEFAULT 1,
                    level_m REAL NOT NULL,
                    observed_hour INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    ingested_by TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    UNIQUE(series_id, message_no)
                );
                CREATE INDEX IF NOT EXISTS idx_tide_series ON tide_reports(series_id, status);

                CREATE TABLE IF NOT EXISTS clearance_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    result TEXT NOT NULL,
                    status TEXT NOT NULL,
                    basis_kind TEXT NOT NULL,
                    basis_message_id INTEGER,
                    series_id TEXT NOT NULL DEFAULT '',
                    correction_seq INTEGER NOT NULL DEFAULT 0,
                    chart_depth_m REAL NOT NULL,
                    tide_level_m REAL,
                    available_depth_m REAL NOT NULL,
                    required_depth_m REAL NOT NULL,
                    run_id INTEGER,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    superseded_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_decisions_plan ON clearance_decisions(plan_id, id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_decision_open
                    ON clearance_decisions(plan_id) WHERE status IN ('active', 'held');

                CREATE TABLE IF NOT EXISTS resource_bookings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_id INTEGER NOT NULL REFERENCES clearance_decisions(id) ON DELETE CASCADE,
                    plan_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    resource_type TEXT NOT NULL,
                    resource_key TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'held',
                    created_at TEXT NOT NULL,
                    released_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_bookings_plan ON resource_bookings(plan_id, status);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bookings_resource_held
                    ON resource_bookings(resource_type, resource_key) WHERE status='held';

                CREATE TABLE IF NOT EXISTS recompute_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trigger_message_id INTEGER NOT NULL REFERENCES tide_reports(id),
                    trigger_kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'running',
                    total_count INTEGER NOT NULL DEFAULT 0,
                    done_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS recompute_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES recompute_runs(id) ON DELETE CASCADE,
                    plan_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT '',
                    from_decision_id INTEGER,
                    to_decision_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(run_id, plan_id)
                );
                CREATE INDEX IF NOT EXISTS idx_items_run ON recompute_items(run_id, status);

                CREATE TABLE IF NOT EXISTS conflict_drafts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    actor_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_drafts_status ON conflict_drafts(status, id);

                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    decision_id INTEGER REFERENCES clearance_decisions(id) ON DELETE CASCADE,
                    run_id INTEGER REFERENCES recompute_runs(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    basis TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    resolution TEXT NOT NULL DEFAULT '',
                    resolved_by TEXT NOT NULL DEFAULT '',
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews(status, id);
                """
            )

    @contextmanager
    def transaction(self):
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["payload"] = json.loads(item["payload"])
        return item

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    # ---------- 靠泊计划（records） ----------

    def create(self, reference: str, state: str, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        now = _now()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "INSERT INTO records(reference,state,version,payload,created_by,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    (reference, state, 1, self._json(payload), actor_id, actor_id, now, now),
                )
                record_id = int(cursor.lastrowid)
                connection.execute(
                    "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                    (record_id, "created", actor_id, 1, self._json({"state": state}), now),
                )
                row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        except sqlite3.IntegrityError as exc:
            raise Conflict("reference已存在") from exc
        return self._row(row)

    def get(self, record_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFound("记录不存在")
        return self._row(row)

    def list_records(self, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            if state:
                rows = connection.execute("SELECT * FROM records WHERE state=? ORDER BY id DESC LIMIT ?", (state, limit)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM records ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [self._row(row) for row in rows]

    def mutate(self, record_id: int, expected_version: int, state: str, payload: Dict[str, Any], actor_id: str, action: str, details: Dict[str, Any]) -> Dict[str, Any]:
        with self.transaction() as connection:
            result = self.update_record_locked(
                connection, record_id, expected_version, state, payload, actor_id, action, details
            )
        return result

    def update_record_locked(self, connection: sqlite3.Connection, record_id: int, expected_version: int,
                             state: str, payload: Dict[str, Any], actor_id: str, action: str,
                             details: Dict[str, Any], allowed_from: Optional[set] = None) -> Dict[str, Any]:
        row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFound("记录不存在")
        # 版本检查必须先于状态检查：并发提交时后到者拿到的是版本冲突，从而保留冲突草稿
        if int(row["version"]) != int(expected_version):
            raise VersionConflict("版本冲突，请刷新后重试")
        if allowed_from is not None and row["state"] not in allowed_from:
            raise Conflict("当前状态%s不允许执行%s" % (row["state"], action))
        version = int(expected_version) + 1
        now = _now()
        connection.execute(
            "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
            (state, version, self._json(payload), actor_id, now, record_id),
        )
        connection.execute(
            "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
            (record_id, action, actor_id, version, self._json(details), now),
        )
        result = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return self._row(result)

    def add_audit(self, record_id: int, actor_id: str, action: str, details: Dict[str, Any]) -> None:
        with self._connect() as connection:
            row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
            if row is None:
                raise NotFound("记录不存在")
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                (record_id, action, actor_id, int(row["version"]), self._json(details), _now()),
            )

    def add_audit_locked(self, connection: sqlite3.Connection, record_id: Optional[int], actor_id: str, action: str,
                         details: Dict[str, Any], version: Optional[int] = None) -> None:
        if version is None and record_id:
            row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
            version = int(row["version"]) if row else 0
        connection.execute(
            "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
            (record_id, action, actor_id, version if version is not None else 0, self._json(details), _now()),
        )

    def audit_timeline(self, record_id: int) -> List[Dict[str, Any]]:
        self.get(record_id)
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM audit_events WHERE record_id=? ORDER BY id", (record_id,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"])
            result.append(item)
        return result

    def stats(self) -> Dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT state, COUNT(*) AS total FROM records GROUP BY state").fetchall()
        return {str(row["state"]): int(row["total"]) for row in rows}

    def health(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    # ---------- 潮位观测报文 ----------

    @staticmethod
    def _tide_row(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def insert_tide_report_locked(self, connection: sqlite3.Connection, report: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        now = _now()
        cursor = connection.execute(
            "INSERT INTO tide_reports(series_id,message_no,correction_seq,level_m,observed_hour,status,ingested_by,ingested_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (report["series_id"], report["message_no"], int(report["correction_seq"]),
             float(report["level_m"]), int(report["observed_hour"]), "active", actor_id, now),
        )
        row = connection.execute("SELECT * FROM tide_reports WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        return self._tide_row(row)

    def get_tide_report(self, series_id: str, message_no: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM tide_reports WHERE series_id=? AND message_no=?", (series_id, message_no)
            ).fetchone()
        return self._tide_row(row) if row else None

    def get_tide_report_by_id(self, report_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM tide_reports WHERE id=?", (report_id,)).fetchone()
        if row is None:
            raise NotFound("潮位报文不存在")
        return self._tide_row(row)

    def active_tide_report_locked(self, connection: sqlite3.Connection, series_id: str,
                                  exclude_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        if exclude_id is None:
            row = connection.execute(
                "SELECT * FROM tide_reports WHERE series_id=? AND status='active' ORDER BY id DESC",
                (series_id,),
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT * FROM tide_reports WHERE series_id=? AND status='active' AND id<>? ORDER BY id DESC",
                (series_id, exclude_id),
            ).fetchone()
        return self._tide_row(row) if row else None

    def active_tide_report(self, series_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            return self.active_tide_report_locked(connection, series_id)

    def supersede_tide_reports_locked(self, connection: sqlite3.Connection, series_id: str, keep_id: int) -> int:
        cur = connection.execute(
            "UPDATE tide_reports SET status='superseded' WHERE series_id=? AND status='active' AND id<>?",
            (series_id, keep_id),
        )
        return int(cur.rowcount or 0)

    def list_tide_reports(self, series_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            if series_id:
                rows = connection.execute(
                    "SELECT * FROM tide_reports WHERE series_id=? ORDER BY id DESC LIMIT ?", (series_id, limit)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM tide_reports ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [self._tide_row(row) for row in rows]

    def plans_for_series_locked(self, connection: sqlite3.Connection, series_id: str) -> List[Dict[str, Any]]:
        rows = connection.execute("SELECT * FROM records ORDER BY id").fetchall()
        plans = []
        for row in rows:
            item = self._row(row)
            if str(item["payload"].get("series_id") or "") == series_id and item["state"] in {"confirmed", "held", "berthed"}:
                plans.append(item)
        return plans

    # ---------- 放行判定与资源占用 ----------

    @staticmethod
    def _decision_row(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def insert_decision_locked(self, connection: sqlite3.Connection, decision: Dict[str, Any]) -> Dict[str, Any]:
        now = _now()
        cursor = connection.execute(
            "INSERT INTO clearance_decisions(plan_id,kind,result,status,basis_kind,basis_message_id,series_id,"
            "correction_seq,chart_depth_m,tide_level_m,available_depth_m,required_depth_m,run_id,created_by,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (int(decision["plan_id"]), decision["kind"], decision["result"], decision["status"],
             decision["basis_kind"], decision.get("basis_message_id"), decision.get("series_id", ""),
             int(decision.get("correction_seq", 0)), float(decision["chart_depth_m"]),
             decision.get("tide_level_m"), float(decision["available_depth_m"]),
             float(decision["required_depth_m"]), decision.get("run_id"), decision["created_by"], now),
        )
        row = connection.execute("SELECT * FROM clearance_decisions WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        return self._decision_row(row)

    def open_decision_locked(self, connection: sqlite3.Connection, plan_id: int) -> Optional[Dict[str, Any]]:
        row = connection.execute(
            "SELECT * FROM clearance_decisions WHERE plan_id=? AND status IN ('active','held','retained') ORDER BY id DESC",
            (plan_id,),
        ).fetchone()
        return self._decision_row(row) if row else None

    def list_decisions(self, plan_id: int) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM clearance_decisions WHERE plan_id=? ORDER BY id", (plan_id,)
            ).fetchall()
        return [self._decision_row(row) for row in rows]

    def set_decision_status_locked(self, connection: sqlite3.Connection, decision_id: int, status: str) -> None:
        connection.execute(
            "UPDATE clearance_decisions SET status=?, superseded_at=? WHERE id=? AND status<>?",
            (status, _now() if status in {"superseded", "completed"} else None, decision_id, status),
        )

    def insert_bookings_locked(self, connection: sqlite3.Connection, decision_id: int, plan_id: int,
                               resources: Iterable[tuple]) -> None:
        now = _now()
        for resource_type, resource_key in resources:
            try:
                connection.execute(
                    "INSERT INTO resource_bookings(decision_id,plan_id,resource_type,resource_key,status,created_at)"
                    " VALUES(?,?,?,?,'held',?)",
                    (decision_id, plan_id, resource_type, resource_key, now),
                )
            except sqlite3.IntegrityError as exc:
                holder = connection.execute(
                    "SELECT plan_id FROM resource_bookings WHERE resource_type=? AND resource_key=?",
                    (resource_type, resource_key),
                ).fetchone()
                holder_id = int(holder["plan_id"]) if holder else None
                raise ResourceBusy("资源已被占用：%s %s（计划#%s）" % (resource_type, resource_key, holder_id)) from exc

    def release_bookings_locked(self, connection: sqlite3.Connection, plan_id: int) -> int:
        cur = connection.execute(
            "UPDATE resource_bookings SET status='released', released_at=? WHERE plan_id=? AND status='held'",
            (_now(), plan_id),
        )
        return int(cur.rowcount or 0)

    def list_bookings(self, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        with self._connect() as connection:
            if status:
                rows = connection.execute(
                    "SELECT * FROM resource_bookings WHERE status=? ORDER BY id DESC LIMIT ?", (status, limit)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM resource_bookings ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    # ---------- 重算运行 ----------

    def create_recompute_run_locked(self, connection: sqlite3.Connection, trigger_message_id: int,
                                    trigger_kind: str, plan_ids: Iterable[int], actor_id: str) -> Dict[str, Any]:
        now = _now()
        plan_ids = list(plan_ids)
        cursor = connection.execute(
            "INSERT INTO recompute_runs(trigger_message_id,trigger_kind,status,total_count,done_count,failed_count,"
            "created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (trigger_message_id, trigger_kind, "running", len(plan_ids), 0, 0, actor_id, now),
        )
        run_id = int(cursor.lastrowid)
        for plan_id in plan_ids:
            connection.execute(
                "INSERT INTO recompute_items(run_id,plan_id,status,attempt_count,error,created_at,updated_at)"
                " VALUES(?,?, 'pending',0,'',?,?)",
                (run_id, int(plan_id), now, now),
            )
        row = connection.execute("SELECT * FROM recompute_runs WHERE id=?", (run_id,)).fetchone()
        return dict(row)

    def get_recompute_run(self, run_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM recompute_runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise NotFound("重算运行不存在")
        return dict(row)

    def list_recompute_runs(self, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM recompute_runs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def list_recompute_items(self, run_id: int) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM recompute_items WHERE run_id=? ORDER BY id", (run_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def get_recompute_item_locked(self, connection: sqlite3.Connection, item_id: int) -> Optional[Dict[str, Any]]:
        row = connection.execute("SELECT * FROM recompute_items WHERE id=?", (item_id,)).fetchone()
        return dict(row) if row else None

    def unfinished_items_locked(self, connection: sqlite3.Connection, run_id: int) -> List[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM recompute_items WHERE run_id=? AND status IN ('pending','failed') ORDER BY id",
            (run_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_item_locked(self, connection: sqlite3.Connection, item_id: int, status: str,
                         error: str = "", from_decision_id: Optional[int] = None,
                         to_decision_id: Optional[int] = None) -> None:
        connection.execute(
            "UPDATE recompute_items SET status=?,error=?,attempt_count=attempt_count+1,"
            "from_decision_id=COALESCE(?,from_decision_id),to_decision_id=COALESCE(?,to_decision_id),updated_at=? "
            "WHERE id=?",
            (status, error, from_decision_id, to_decision_id, _now(), item_id),
        )

    def mark_item_failed(self, item_id: int, error: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE recompute_items SET status='failed',error=?,attempt_count=attempt_count+1,updated_at=? WHERE id=?",
                (error[:500], _now(), item_id),
            )

    def refresh_run_status_locked(self, connection: sqlite3.Connection, run_id: int) -> Dict[str, Any]:
        counts = connection.execute(
            "SELECT SUM(CASE WHEN status IN ('done','skipped') THEN 1 ELSE 0 END) AS done,"
            "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,"
            "SUM(CASE WHEN status IN ('pending','failed') THEN 1 ELSE 0 END) AS pending FROM recompute_items WHERE run_id=?",
            (run_id,),
        ).fetchone()
        pending = int(counts["pending"] or 0)
        if pending == 0:
            connection.execute(
                "UPDATE recompute_runs SET status='completed',done_count=?,failed_count=?,finished_at=COALESCE(finished_at,?) WHERE id=?",
                (int(counts["done"] or 0), int(counts["failed"] or 0), _now(), run_id),
            )
        else:
            connection.execute(
                "UPDATE recompute_runs SET status='running',done_count=?,failed_count=?,finished_at=NULL WHERE id=?",
                (int(counts["done"] or 0), int(counts["failed"] or 0), run_id),
            )
        row = connection.execute("SELECT * FROM recompute_runs WHERE id=?", (run_id,)).fetchone()
        return dict(row)

    # ---------- 冲突草稿 ----------

    def save_conflict_draft(self, plan_id: int, actor_id: str, action: str, base_version: int,
                            payload: Dict[str, Any], reason: str) -> Dict[str, Any]:
        now = _now()
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO conflict_drafts(plan_id,actor_id,action,base_version,payload,reason,status,created_at)"
                " VALUES(?,?,?,?,?,?,'open',?)",
                (plan_id, actor_id, action, int(base_version), self._json(payload or {}), reason, now),
            )
            row = connection.execute("SELECT * FROM conflict_drafts WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        item = dict(row)
        item["payload"] = json.loads(item["payload"])
        return item

    def list_conflict_drafts(self, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            if status:
                rows = connection.execute(
                    "SELECT * FROM conflict_drafts WHERE status=? ORDER BY id DESC LIMIT ?", (status, limit)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM conflict_drafts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            items.append(item)
        return items

    def get_conflict_draft(self, draft_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM conflict_drafts WHERE id=?", (draft_id,)).fetchone()
        if row is None:
            raise NotFound("冲突草稿不存在")
        item = dict(row)
        item["payload"] = json.loads(item["payload"])
        return item

    def resolve_draft_locked(self, connection: sqlite3.Connection, draft_id: int, status: str) -> None:
        connection.execute(
            "UPDATE conflict_drafts SET status=?, resolved_at=? WHERE id=? AND status='open'",
            (status, _now(), draft_id),
        )

    # ---------- 复核 ----------

    def insert_review_locked(self, connection: sqlite3.Connection, plan_id: int, decision_id: int,
                             run_id: Optional[int], kind: str, basis: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        cursor = connection.execute(
            "INSERT INTO reviews(plan_id,decision_id,run_id,kind,status,basis,created_by,created_at)"
            " VALUES(?,?,?,?,'open',?,?,?)",
            (plan_id, decision_id, run_id, kind, self._json(basis), actor_id, _now()),
        )
        row = connection.execute("SELECT * FROM reviews WHERE id=?", (int(cursor.lastrowid),)).fetchone()
        item = dict(row)
        item["basis"] = json.loads(item["basis"])
        return item

    def list_reviews(self, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            if status:
                rows = connection.execute(
                    "SELECT * FROM reviews WHERE status=? ORDER BY id DESC LIMIT ?", (status, limit)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM reviews ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["basis"] = json.loads(item["basis"])
            items.append(item)
        return items

    def get_review(self, review_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM reviews WHERE id=?", (review_id,)).fetchone()
        if row is None:
            raise NotFound("复核记录不存在")
        item = dict(row)
        item["basis"] = json.loads(item["basis"])
        return item

    def resolve_review(self, review_id: int, resolution: str, actor_id: str) -> Dict[str, Any]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM reviews WHERE id=?", (review_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("复核记录不存在")
            if row["status"] != "open":
                connection.rollback()
                raise Conflict("复核记录已处理")
            connection.execute(
                "UPDATE reviews SET status='resolved',resolution=?,resolved_by=?,resolved_at=? WHERE id=?",
                (resolution, actor_id, _now(), review_id),
            )
            version_row = connection.execute("SELECT version FROM records WHERE id=?", (int(row["plan_id"]),)).fetchone()
            version = int(version_row["version"]) if version_row else 0
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                (int(row["plan_id"]), "review_resolved", actor_id, version,
                 self._json({"review_id": review_id, "resolution": resolution}), _now()),
            )
            out = connection.execute("SELECT * FROM reviews WHERE id=?", (review_id,)).fetchone()
            connection.commit()
        item = dict(out)
        item["basis"] = json.loads(item["basis"])
        return item
