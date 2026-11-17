"""批次补传引擎核心。

语义清单（对应需求）：
1. 批次接口：open_batch / append_chunk / commit_batch / get_batch，按来源+凭证号识别费用。
2. 乱序记录取最高修订：合并按 revision 降序，每键只留一行。
3. 凭证撤销留住修订号：is_cancel 墓碑带 revision，永久保留。
4. 旧记录晚到仍作废：低 revision 不会覆盖高 revision（LWW）。
5. 修订相同却内容冲突则拒收全批：同 (键,revision) 出现不同 content_hash 即整批 rejected，
   候选版回滚、当前版一个字节都不变。
6. 十万条分批落盘 + 128MB：流式 append，每块独立小事务；SQLite 限缓存、禁 mmap。
7. 断点续跑不重复入账：(batch_id,seq) 唯一，重传被 INSERT OR IGNORE 跳过并校验一致性。
8. 接收期间核算读完整旧版：候选版是独立 building 版本，current 直到切换前不变。
9. 校验完才原子切换：合并、冲突检查、版本翻转、指针更新在单个事务内完成。
10. 并发批次重叠凭证按修订合并：提交串行化，后提交者基于最新 current 做 rebase。
11. 切换崩溃同成同败：单事务 + WAL FULL，recover 清理半成品 building 版本。
12. 排队核算固定完整新旧一版：accounting_pins 在入队时固定 version_id。
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .db import connect, initialize
from .errors import (BatchNotFound, BatchRejected, BatchStateError, FaultInjected,
                     PinError, RecordValidationError, SequenceGap)
from .records import normalize

# 故障注入点。测试通过环境变量 FINFAULT=<点> 或 store.fault 钩子触发。
# after_chunk:<n>      持久化第 n 块后崩溃（进程退出）
# commit_pre_merge     提交事务开始前崩溃
# commit_post_build    候选版构建后、指针切换前崩溃
# commit_post_switch   指针切换后、提交返回前崩溃
FAULT_HOOK = os.environ.get("FINFAULT", "")


class LedgerStore:
    def __init__(self, db_path: str | Path, *, fault: str | None = None,
                 chunk_size: int = 1000):
        self.path = str(db_path)
        self.conn = connect(self.path)
        initialize(self.conn)
        # 本进程内的故障点（字符串，环境变量 FINFAULT 与之取或）。
        self._fault = fault or FAULT_HOOK
        self.chunk_size = chunk_size

    # ---------- 基础工具 ----------
    def close(self) -> None:
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _now(self) -> float:
        return time.time()

    def _die(self, point: str) -> None:
        """模拟进程崩溃：os._exit 不跑任何 finally/缓冲刷新，最接近真实掉电。

        支持 after_chunk:<n> 这类带序号的故障点，按前缀匹配。
        不做 wal_checkpoint：提交事务内 checkpoint 会拿锁失败；而
        synchronous=FULL 已保证已提交事务的 WAL 帧落盘，直接硬退出即可。
        """
        if point and (point == self._fault or self._fault.startswith(point + ":")):
            os._exit(86)

    def current_version_id(self) -> int:
        return self.conn.execute(
            "SELECT version_id FROM current_version WHERE id=1").fetchone()[0]

    def audit(self, actor: str, action: str, target: str = "", detail: Any = None) -> None:
        self.conn.execute(
            "INSERT INTO audits(at,actor,action,target,detail) VALUES(?,?,?,?,?)",
            (self._now(), actor, action, target,
             json.dumps(detail, ensure_ascii=False, sort_keys=True) if detail is not None else None))

    def _get_batch(self, batch_id: int) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise BatchNotFound(f"批次 {batch_id} 不存在")
        return row

    # ---------- 批次生命周期 ----------
    def open_batch(self, *, expected_count: int | None = None,
                   actor: str = "system") -> int:
        if expected_count is not None and (not isinstance(expected_count, int)
                                           or expected_count <= 0):
            raise RecordValidationError("expected_count 必须为正整数")
        now = self._now()
        cur = self.conn.execute(
            "INSERT INTO batches(state,expected_count,base_version_id,created_at,updated_at)"
            " VALUES('receiving',?,?,?,?)",
            (expected_count, self.current_version_id(), now, now))
        batch_id = cur.lastrowid
        self.audit(actor, "batch.open", str(batch_id),
                   {"expected_count": expected_count})
        return batch_id

    def append_chunk(self, batch_id: int, records: list[dict],
                     *, first_seq: int | None = None, actor: str = "system") -> dict:
        """流式写入一块记录。

        records 为本块的原始记录（乱序允许）；first_seq 为本块起始序号（1 起）。
        重跑时允许从任意已持久化位置重发：(batch_id,seq) 命中即跳过，
        但会校验指纹，指纹不一致说明同一序号被塞了不同内容，直接拒绝。
        返回 {accepted, duplicated, next_seq, checkpoint_seq}。
        """
        batch = self._get_batch(batch_id)
        if batch["state"] != "receiving":
            raise BatchStateError(
                f"批次状态为 {batch['state']}，仅 receiving 可追加",
                details={"batch_id": batch_id, "state": batch["state"]})

        if not records:
            return {"accepted": 0, "duplicated": 0,
                    "next_seq": batch["checkpoint_seq"] + 1,
                    "checkpoint_seq": batch["checkpoint_seq"]}

        # 序号解析：显式 first_seq 优先；否则从 checkpoint+1 续。
        start = first_seq if first_seq is not None else None
        if start is not None and (not isinstance(start, int) or start < 1):
            raise SequenceGap("first_seq 必须为 >=1 的整数")

        normalized: list[dict] = []
        for offset, raw in enumerate(records):
            item = normalize(raw)  # 非法记录直接抛错，本块整体不落盘
            item["offset"] = offset
            normalized.append(item)

        # 进入写事务后再读检查点：并发追加同一批次时由 IMMEDIATE 锁串行化，
        # 避免两个线程基于过期 checkpoint 判断序号连续性。
        accepted = duplicated = 0
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            locked = self.conn.execute(
                "SELECT state,checkpoint_seq FROM batches WHERE id=?",
                (batch_id,)).fetchone()
            if locked["state"] != "receiving":
                raise BatchStateError(
                    f"批次状态为 {locked['state']}，仅 receiving 可追加",
                    details={"batch_id": batch_id, "state": locked["state"]})
            cp = locked["checkpoint_seq"]
            if start is None:
                start = cp + 1
            elif start > cp + 1:
                raise SequenceGap(
                    f"序号断层：期望从 {cp + 1} 开始，收到 {start}",
                    details={"expected": cp + 1, "got": start})
            for item in normalized:
                seq = start + item["offset"]
                item["seq"] = seq
                if seq <= cp:
                    # 断点重传：必须与已落盘内容一致，否则是客户端用错了序号。
                    old = self.conn.execute(
                        "SELECT content_hash FROM batch_records WHERE batch_id=? AND seq=?",
                        (batch_id, seq)).fetchone()
                    if old is None:
                        raise SequenceGap(f"序号 {seq} 早于检查点但无落盘记录")
                    if old["content_hash"] != item["content_hash"]:
                        raise BatchRejected(
                            f"序号 {seq} 重传内容与首次不一致",
                            details={"seq": seq, "batch_id": batch_id})
                    duplicated += 1
                    continue
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO batch_records"
                    "(batch_id,seq,source,voucher_no,revision,is_cancel,amount_cents,"
                    " payload,content_hash) VALUES(?,?,?,?,?,?,?,?,?)",
                    (batch_id, seq, item["source"], item["voucher_no"], item["revision"],
                     1 if item["is_cancel"] else 0, item["amount_cents"],
                     json.dumps(item["payload"], ensure_ascii=False, sort_keys=True),
                     item["content_hash"]))
                if cur.rowcount:
                    accepted += 1
                else:
                    # 同事务内此前未提交却撞唯一键，只能是同 seq 不同内容。
                    old = self.conn.execute(
                        "SELECT content_hash FROM batch_records WHERE batch_id=? AND seq=?",
                        (batch_id, seq)).fetchone()
                    if old and old["content_hash"] != item["content_hash"]:
                        raise BatchRejected(
                            f"序号 {seq} 内容冲突", details={"seq": seq})
                    duplicated += 1

            max_seq = self.conn.execute(
                "SELECT COALESCE(MAX(seq),0) FROM batch_records WHERE batch_id=?",
                (batch_id,)).fetchone()[0]
            count = self.conn.execute(
                "SELECT COUNT(*) FROM batch_records WHERE batch_id=?",
                (batch_id,)).fetchone()[0]
            # 序号集合必须严格 1..N 连续（由 append 顺序 + 上面的断层检查共同保证）。
            self.conn.execute(
                "UPDATE batches SET received_count=?, checkpoint_seq=?, updated_at=? WHERE id=?",
                (count, max_seq, self._now(), batch_id))
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

        # 仅当本块真正写入了新记录时才触发崩溃点；重跑时重复块不再重复崩溃，
        # 这样同一个故障命令可以直接再跑一遍完成续传。
        if accepted:
            chunk_no = (max_seq - 1) // self.chunk_size + 1
            self._die(f"after_chunk:{chunk_no}")
        return {"accepted": accepted, "duplicated": duplicated,
                "next_seq": max_seq + 1, "checkpoint_seq": max_seq}

    def append_stream(self, batch_id: int, iter_records, *, actor: str = "system") -> dict:
        """把流式记录按 chunk_size 切块落盘，调用方只看到总进度（内存友好）。

        序号按记录在流中的自然位置（1 起）分配，因此重跑同一个完整文件时，
        已持久化的序号走 append_chunk 的幂等分支，新序号继续追加——天然断点续传。
        """
        total_acc = total_dup = 0
        chunk: list[dict] = []
        chunk_start = 1
        pos = 0
        last_next = 1
        for raw in iter_records:
            if pos % self.chunk_size == 0:
                chunk_start = pos + 1
            chunk.append(raw)
            pos += 1
            if len(chunk) >= self.chunk_size:
                r = self.append_chunk(batch_id, chunk, first_seq=chunk_start, actor=actor)
                total_acc += r["accepted"]
                total_dup += r["duplicated"]
                last_next = r["next_seq"]
                chunk = []
        if chunk:
            r = self.append_chunk(batch_id, chunk, first_seq=chunk_start, actor=actor)
            total_acc += r["accepted"]
            total_dup += r["duplicated"]
            last_next = r["next_seq"]
        return {"accepted": total_acc, "duplicated": total_dup,
                "next_seq": last_next, "total_seen": pos}

    # ---------- 提交：校验 + 合并 + 原子切换 ----------
    def commit_batch(self, batch_id: int, *, actor: str = "system") -> dict:
        batch = self._get_batch(batch_id)
        if batch["state"] not in ("receiving", "committing", "failed"):
            raise BatchStateError(
                f"批次状态 {batch['state']} 不可提交",
                details={"batch_id": batch_id, "state": batch["state"]})
        if batch["expected_count"] is not None and \
                batch["received_count"] != batch["expected_count"]:
            raise BatchStateError(
                "收齐数量与预期不符，拒绝提交",
                details={"expected": batch["expected_count"],
                         "received": batch["received_count"]})
        if batch["received_count"] == 0:
            raise BatchStateError("空批次不可提交")

        # 故障点：进入提交流程前崩溃；恢复后批次仍是 receiving，可无损重试。
        self._die("commit_pre_merge")

        # 整个切换在一个 IMMEDIATE 事务里：同成同败，读端永远看不到半成品。
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            now = self._now()
            self.conn.execute(
                "UPDATE batches SET state='committing', updated_at=? WHERE id=?",
                (now, batch_id))

            base_version = self.current_version_id()
            # 候选版：复制基线全部行（含墓碑），保证新版本是完整一版而非增量。
            ver = self.conn.execute(
                "INSERT INTO versions(state,batch_id,created_at) VALUES('building',?,?)",
                (batch_id, now))
            new_vid = ver.lastrowid
            self.conn.execute(
                "UPDATE batches SET candidate_version_id=?, base_version_id=?, updated_at=?"
                " WHERE id=?", (new_vid, base_version, now, batch_id))
            self.conn.execute(
                "INSERT INTO expenses(version_id,source,voucher_no,revision,is_cancel,"
                "amount_cents,payload,content_hash)"
                " SELECT ?,source,voucher_no,revision,is_cancel,amount_cents,payload,content_hash"
                " FROM expenses WHERE version_id=?", (new_vid, base_version))

            # 冲突一：批内同一 (键,revision) 出现不同内容指纹 -> 全批拒收。
            intra = self.conn.execute(
                "SELECT source,voucher_no,revision,COUNT(DISTINCT content_hash) AS kinds"
                " FROM batch_records WHERE batch_id=?"
                " GROUP BY source,voucher_no,revision HAVING kinds > 1 LIMIT 5",
                (batch_id,)).fetchall()
            if intra:
                self._reject_locked(batch_id, new_vid,
                                    "批内同修订号内容冲突",
                                    [dict(r) for r in intra])
                self.conn.execute("COMMIT")
                raise BatchRejected("修订相同却内容冲突，全批拒收",
                                    details={"conflicts": [dict(r) for r in intra]})

            # 冲突二：与当前版同修订号但内容不同（老底稿已有的 rev 不允许被悄悄改内容）。
            cross = self.conn.execute(
                "SELECT br.source,br.voucher_no,br.revision"
                " FROM batch_records br"
                " JOIN expenses e ON e.version_id=? AND e.source=br.source"
                "  AND e.voucher_no=br.voucher_no AND e.revision=br.revision"
                " WHERE br.batch_id=? AND e.content_hash<>br.content_hash"
                " GROUP BY br.source,br.voucher_no,br.revision LIMIT 5",
                (base_version, batch_id)).fetchall()
            if cross:
                self._reject_locked(batch_id, new_vid,
                                    "与当前版同修订号内容冲突",
                                    [dict(r) for r in cross])
                self.conn.execute("COMMIT")
                raise BatchRejected("修订相同却内容冲突，全批拒收",
                                    details={"conflicts": [dict(r) for r in cross]})

            # 合并：把批内每键最高修订并入候选版。
            # 候选版已含基线全量行，因此用 UPSERT：
            #  - 新键：直接插入；
            #  - 已存在键：仅当批内最高修订严格高于基线修订才覆盖（旧记录晚到作废）；
            #  - 批内同键同 rev 同指纹的重复行由 GROUP BY/ROW_NUMBER 折叠。
            self.conn.execute(
                "INSERT INTO expenses(version_id,source,voucher_no,revision,is_cancel,"
                "amount_cents,payload,content_hash)"
                " SELECT ?,t.source,t.voucher_no,t.revision,t.is_cancel,t.amount_cents,"
                "  t.payload,t.content_hash FROM ("
                "  SELECT source,voucher_no,revision,is_cancel,amount_cents,payload,"
                "   content_hash,ROW_NUMBER() OVER ("
                "     PARTITION BY source,voucher_no ORDER BY revision DESC, content_hash ASC"
                "   ) AS rn"
                "  FROM batch_records WHERE batch_id=?"
                " ) t WHERE t.rn=1 AND t.revision > ("
                "  SELECT COALESCE(MAX(revision),-1) FROM expenses"
                "  WHERE version_id=? AND source=t.source AND voucher_no=t.voucher_no)"
                " ON CONFLICT(version_id,source,voucher_no) DO UPDATE SET"
                "  revision=excluded.revision,is_cancel=excluded.is_cancel,"
                "  amount_cents=excluded.amount_cents,payload=excluded.payload,"
                "  content_hash=excluded.content_hash"
                " WHERE excluded.revision > expenses.revision",
                (new_vid, batch_id, base_version))

            # 故障点：候选版已建好、指针未动 -> 崩溃后由 recover 清理 building，
            # current 保持完整旧版，批次可重新提交。
            self._die("commit_post_build")

            # 原子翻转：旧 current -> retired，building -> current，指针同事务更新。
            self.conn.execute(
                "UPDATE versions SET state='retired', switched_at=? WHERE id=?",
                (now, base_version))
            self.conn.execute(
                "UPDATE versions SET state='current', switched_at=? WHERE id=?",
                (now, new_vid))
            self.conn.execute(
                "UPDATE current_version SET version_id=? WHERE id=1", (new_vid,))
            self.conn.execute(
                "UPDATE batches SET state='committed', updated_at=? WHERE id=?",
                (now, batch_id))
            self.audit(actor, "batch.commit", str(batch_id),
                       {"base_version": base_version, "new_version": new_vid})

            # 故障点：切换语句已在事务内执行但尚未提交 -> 崩溃回滚，对外仍是旧版。
            self._die("commit_post_switch")
            self.conn.execute("COMMIT")
        except BatchRejected:
            raise
        except Exception:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            # 记录失败态但不破坏已落盘的批次明细，允许修复后重试。
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute(
                    "UPDATE batches SET state='failed', updated_at=? WHERE id=? AND state='committing'",
                    (self._now(), batch_id))
                self.conn.execute("COMMIT")
            except sqlite3.Error:
                if self.conn.in_transaction:
                    self.conn.execute("ROLLBACK")
            raise

        return {"batch_id": batch_id, "state": "committed",
                "version_id": new_vid, "base_version_id": base_version}

    def _reject_locked(self, batch_id: int, candidate_vid: int,
                       reason: str, conflicts: list) -> None:
        """调用方已持有事务：回滚候选版并把批次置为 rejected（与回滚同事务提交）。"""
        self.conn.execute("DELETE FROM expenses WHERE version_id=?", (candidate_vid,))
        self.conn.execute("DELETE FROM versions WHERE id=?", (candidate_vid,))
        self.conn.execute(
            "UPDATE batches SET state='rejected', reject_reason=?, candidate_version_id=NULL,"
            " updated_at=? WHERE id=?",
            (reason + ":" + json.dumps(conflicts, ensure_ascii=False), self._now(), batch_id))
        self.audit("system", "batch.reject", str(batch_id),
                   {"reason": reason, "conflicts": conflicts})

    # ---------- 崩溃恢复 ----------
    def recover(self) -> dict:
        """启动时调用：崩溃后让批次状态与底稿版本回到一致的可用状态。

        - 事务原子性保证：指针要么没切（current 旧版），要么已切（current 新版）。
        - 孤儿 building 版本：删除（其批次回到 receiving/failed，可重新提交）。
        - 卡在 committing 的批次：回到 receiving（明细都在，重提即可，不会重复入账）。
        """
        self.conn.execute("BEGIN IMMEDIATE")
        result = {"orphan_versions": [], "reset_batches": []}
        try:
            current = self.current_version_id()
            orphans = self.conn.execute(
                "SELECT id,batch_id FROM versions WHERE state='building'").fetchall()
            for row in orphans:
                self.conn.execute("DELETE FROM expenses WHERE version_id=?", (row["id"],))
                self.conn.execute("DELETE FROM versions WHERE id=?", (row["id"],))
                result["orphan_versions"].append(row["id"])
            # 卡在 committing/failed 的批次（含孤儿版所属批次）统一回到 receiving。
            stuck = self.conn.execute(
                "SELECT id FROM batches WHERE state IN ('committing','failed')").fetchall()
            for row in stuck:
                self.conn.execute(
                    "UPDATE batches SET state='receiving', candidate_version_id=NULL,"
                    " updated_at=? WHERE id=?", (self._now(), row["id"]))
                result["reset_batches"].append(row["id"])
            # 指针健康检查：current 必须存在且唯一。
            vrow = self.conn.execute(
                "SELECT id,state FROM versions WHERE id=?", (current,)).fetchone()
            if vrow is None or vrow["state"] != "current":
                self.conn.execute("ROLLBACK")
                raise FaultInjected("current 指针损坏，需要人工介入")
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        return result

    # ---------- 读侧：核算固定版本 ----------
    def pin_version(self, job_id: str, version_id: int | None = None) -> int:
        vid = version_id if version_id is not None else self.current_version_id()
        row = self.conn.execute("SELECT id FROM versions WHERE id=? AND state IN"
                                " ('current','retired')", (vid,)).fetchone()
        if row is None:
            raise PinError(f"版本 {vid} 不存在或不是完整版本（building 不可被核算固定）")
        self.conn.execute(
            "INSERT OR IGNORE INTO accounting_pins(job_id,version_id,created_at)"
            " VALUES(?,?,?)", (job_id, vid, self._now()))
        return vid

    def pinned_version(self, job_id: str) -> int:
        row = self.conn.execute(
            "SELECT version_id FROM accounting_pins WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise PinError(f"核算任务 {job_id} 尚未固定版本")
        return row["version_id"]

    def read_expense(self, version_id: int, source: str, voucher_no: str):
        row = self.conn.execute(
            "SELECT * FROM expenses WHERE version_id=? AND source=? AND voucher_no=?",
            (version_id, source, voucher_no)).fetchone()
        return dict(row) if row else None

    def iter_version(self, version_id: int, *, include_canceled: bool = True):
        """流式输出版本全部费用，核算侧逐行消费，不把整版载入内存。"""
        sql = ("SELECT source,voucher_no,revision,is_cancel,amount_cents,payload,content_hash"
               " FROM expenses WHERE version_id=? ORDER BY source,voucher_no")
        cur = self.conn.execute(sql, (version_id,))
        for row in cur:
            d = dict(row)
            if not include_canceled and d["is_cancel"]:
                continue
            d["payload"] = json.loads(d["payload"])
            yield d

    def version_totals(self, version_id: int) -> dict:
        row = self.conn.execute(
            "SELECT COUNT(*) AS rows, COALESCE(SUM(amount_cents),0) AS amount,"
            " SUM(is_cancel) AS canceled FROM expenses WHERE version_id=?",
            (version_id,)).fetchone()
        return {"version_id": version_id, "rows": row["rows"],
                "amount_cents": row["amount"], "canceled": row["canceled"]}

    def list_versions(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM versions ORDER BY id").fetchall()]

    def get_batch(self, batch_id: int) -> dict:
        return dict(self._get_batch(batch_id))

    def list_batches(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM batches ORDER BY id").fetchall()]

    # ---------- 报告（旧版永久保留） ----------
    def save_report(self, version_id: int, kind: str, body: Any,
                    *, job_id: str | None = None) -> int:
        cur = self.conn.execute(
            "INSERT INTO reports(version_id,job_id,kind,body,created_at) VALUES(?,?,?,?,?)",
            (version_id, job_id, kind,
             json.dumps(body, ensure_ascii=False, sort_keys=True), self._now()))
        return cur.lastrowid

    def get_report(self, report_id: int) -> dict:
        row = self.conn.execute("SELECT * FROM reports WHERE id=?", (report_id,)).fetchone()
        if row is None:
            raise BatchNotFound(f"报告 {report_id} 不存在")
        d = dict(row)
        d["body"] = json.loads(d["body"])
        return d

    def list_reports(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT id,version_id,job_id,kind,created_at FROM reports ORDER BY id").fetchall()]

    def list_audits(self, limit: int = 100) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM audits ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]
