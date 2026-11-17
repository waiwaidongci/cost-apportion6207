"""SQLite 连接与结构管理（WAL + 严格内存预算）。

设计要点：
- WAL 模式：补传落盘与核算读取互不阻塞，接收期间核算始终读到完整旧版。
- synchronous=FULL：每次提交都 fsync，崩溃后已确认的批次记录不丢。
- 关闭 mmap、限制 cache_size、临时表落盘：十万条补传额外内存控制在 128MB 内。
- busy_timeout：并发批次提交时由 SQLite 写锁串行化，等待而不是立即失败。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 版本表：current 全局唯一；building 为某批次的候选版；retired 永久保留。
CREATE TABLE IF NOT EXISTS versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    state       TEXT NOT NULL CHECK (state IN ('building','current','retired')),
    batch_id    INTEGER,
    created_at  REAL NOT NULL,
    switched_at REAL
);
CREATE INDEX IF NOT EXISTS idx_versions_state ON versions(state);

-- 当前指针：单行表；切换在同一事务内更新，读不到中间态。
CREATE TABLE IF NOT EXISTS current_version (
    id       INTEGER PRIMARY KEY CHECK (id=1),
    version_id INTEGER NOT NULL
);

-- 版本化底稿。同一 (version_id, source, voucher_no) 唯一。
-- 撤销以 is_cancel=1 的墓碑表达，修订号随墓碑保留，不做物理删除。
CREATE TABLE IF NOT EXISTS expenses (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id   INTEGER NOT NULL,
    source       TEXT NOT NULL,
    voucher_no   TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    is_cancel    INTEGER NOT NULL DEFAULT 0,
    amount_cents INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    UNIQUE (version_id, source, voucher_no)
);
CREATE INDEX IF NOT EXISTS idx_expenses_version ON expenses(version_id);

-- 补传批次。checkpoint_seq 为已持久化的最大连续序号，断点续跑据此跳过。
CREATE TABLE IF NOT EXISTS batches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    state         TEXT NOT NULL CHECK (state IN
                    ('receiving','committing','committed','rejected','failed')),
    expected_count INTEGER,
    received_count INTEGER NOT NULL DEFAULT 0,
    checkpoint_seq INTEGER NOT NULL DEFAULT 0,
    candidate_version_id INTEGER,
    base_version_id INTEGER,
    reject_reason TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);

-- 批次明细：(batch_id, seq) 唯一保证重传不重复入账；seq 从 1 起严格连续。
CREATE TABLE IF NOT EXISTS batch_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL,
    seq          INTEGER NOT NULL,
    source       TEXT NOT NULL,
    voucher_no   TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    is_cancel    INTEGER NOT NULL DEFAULT 0,
    amount_cents INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    UNIQUE (batch_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_batch_records_lookup
    ON batch_records(batch_id, source, voucher_no);

-- 核算任务在排队那一刻固定一个版本，之后无论是否切换都读同一完整版本。
CREATE TABLE IF NOT EXISTS accounting_pins (
    job_id     TEXT PRIMARY KEY,
    version_id INTEGER NOT NULL,
    created_at REAL NOT NULL
);

-- 报告按版本保留，旧版报告不被新切换覆盖。
CREATE TABLE IF NOT EXISTS reports (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id INTEGER NOT NULL,
    job_id     TEXT,
    kind       TEXT NOT NULL,
    body       TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audits (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    at         REAL NOT NULL,
    actor      TEXT NOT NULL,
    action     TEXT NOT NULL,
    target     TEXT,
    detail     TEXT
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # 每个连接都要设置的 PRAGMA（WAL 是持久属性，其余为连接级）。
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    # 内存预算：页缓存约 8MB，不用 mmap，排序/临时结果走磁盘。
    conn.execute("PRAGMA cache_size=-8000")
    conn.execute("PRAGMA mmap_size=0")
    conn.execute("PRAGMA temp_store=FILE")
    return conn


def initialize(conn: sqlite3.Connection) -> None:
    # executescript 会隐式提交，因此 DDL 先单独执行（CREATE IF NOT EXISTS 幂等），
    # 种子数据再用显式事务包裹。
    conn.executescript(SCHEMA)
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('schema_version',?)",
                     (str(SCHEMA_VERSION),))
        # 空库的 current 指针：第 0 版空底稿，保证核算始终有版本可读。
        row = conn.execute("SELECT value FROM meta WHERE key='current_version_id'").fetchone()
        if row is None:
            now = conn.execute("SELECT strftime('%s','now') + 0.0").fetchone()[0]
            cur = conn.execute(
                "INSERT INTO versions(state,created_at) VALUES('current',?)", (now,))
            vid = cur.lastrowid
            conn.execute("INSERT INTO current_version(id,version_id) VALUES(1,?)", (vid,))
            conn.execute("INSERT INTO meta(key,value) VALUES('current_version_id',?)",
                         (str(vid),))
        conn.execute("COMMIT")
    except Exception:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


