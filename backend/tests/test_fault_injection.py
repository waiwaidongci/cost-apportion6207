"""故障注入：进程级崩溃后的断点续跑与原子切换验证。

用 os._exit(86) 模拟硬崩溃（不跑 finally、不刷 Python 缓冲），
每个用例都在独立子进程中触发，再开新进程验证磁盘状态。
"""
import json
import unittest

from src.ledger.store import LedgerStore

from .support import fresh_db, make_record, run_cli, write_jsonl


def gen_records(n):
    return [make_record(f"V{i:06d}", 1, 100 + i) for i in range(n)]


class CrashResumeTest(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db()
        self.jsonl = self.db.with_suffix(".jsonl")
        write_jsonl(self.jsonl, gen_records(1000))

    def _open(self, expected=1000):
        proc = run_cli("--db", str(self.db), "open", "--expected", str(expected))
        return json.loads(proc.stdout)["batch_id"]

    def _status(self):
        proc = run_cli("--db", str(self.db), "status")
        return json.loads(proc.stdout)

    def test_01_crash_during_upload_then_resume_no_duplicate(self):
        bid = self._open()
        # 第 3 块（每块 200 条）写完后硬崩溃。
        proc = run_cli("--db", str(self.db), "upload", "--batch", str(bid),
                       "--file", str(self.jsonl), "--chunk-size", "200",
                       env={"FINFAULT": "after_chunk:3"}, check=False)
        self.assertEqual(proc.returncode, 86)

        status = self._status()
        batch = next(b for b in status["batches"] if b["id"] == bid)
        self.assertEqual(batch["checkpoint_seq"], 600)
        self.assertEqual(batch["state"], "receiving")

        # 新进程先 recover（幂等），再用同一个完整文件续传。
        run_cli("--db", str(self.db), "recover")
        run_cli("--db", str(self.db), "upload", "--batch", str(bid),
                "--file", str(self.jsonl), "--chunk-size", "200")
        status = self._status()
        batch = next(b for b in status["batches"] if b["id"] == bid)
        self.assertEqual(batch["received_count"], 1000)
        self.assertEqual(batch["checkpoint_seq"], 1000)

        # 提交并验证不重复入账。
        run_cli("--db", str(self.db), "commit", "--batch", str(bid))
        with LedgerStore(self.db) as s:
            totals = s.version_totals(s.current_version_id())
            self.assertEqual(totals["rows"], 1000)
            self.assertEqual(totals["amount_cents"],
                             sum(100 + i for i in range(1000)))

    def test_02_crash_before_commit_leaves_old_version_intact(self):
        bid = self._open()
        run_cli("--db", str(self.db), "upload", "--batch", str(bid),
                "--file", str(self.jsonl), "--chunk-size", "500")
        before = self._status()["current_version"]
        proc = run_cli("--db", str(self.db), "commit", "--batch", str(bid),
                       env={"FINFAULT": "commit_pre_merge"}, check=False)
        self.assertEqual(proc.returncode, 86)

        status = self._status()
        self.assertEqual(status["current_version"], before)  # 旧版仍是 current
        batch = next(b for b in status["batches"] if b["id"] == bid)
        self.assertEqual(batch["state"], "receiving")

        # 恢复后重新提交成功。
        run_cli("--db", str(self.db), "recover")
        run_cli("--db", str(self.db), "commit", "--batch", str(bid))
        with LedgerStore(self.db) as s:
            self.assertEqual(s.version_totals(s.current_version_id())["rows"], 1000)

    def test_03_crash_after_candidate_built_recovers_and_recommits(self):
        bid = self._open()
        run_cli("--db", str(self.db), "upload", "--batch", str(bid),
                "--file", str(self.jsonl), "--chunk-size", "500")
        before = self._status()["current_version"]
        proc = run_cli("--db", str(self.db), "commit", "--batch", str(bid),
                       env={"FINFAULT": "commit_post_build"}, check=False)
        self.assertEqual(proc.returncode, 86)

        # 新进程恢复：未提交事务随进程死亡整体回滚，building 候选版从未落盘
        # （这正是"同成同败"——磁盘上不存在半成品版本），current 旧版完好。
        rec = json.loads(run_cli("--db", str(self.db), "recover").stdout)
        self.assertEqual(rec["orphan_versions"], [])
        status = self._status()
        self.assertEqual(status["current_version"], before)
        batch = next(b for b in status["batches"] if b["id"] == bid)
        self.assertEqual(batch["state"], "receiving")
        self.assertIsNone(batch["candidate_version_id"])
        # 版本表里只有初始 current 版，没有任何 building 残留。
        self.assertEqual([v["state"] for v in status["versions"]], ["current"])

        run_cli("--db", str(self.db), "commit", "--batch", str(bid))
        with LedgerStore(self.db) as s:
            totals = s.version_totals(s.current_version_id())
            self.assertEqual(totals["rows"], 1000)
            # 不存在残留 building 版。
            self.assertFalse(any(v["state"] == "building"
                                 for v in s.list_versions()))

    def test_04_crash_after_switch_statement_before_commit(self):
        bid = self._open()
        run_cli("--db", str(self.db), "upload", "--batch", str(bid),
                "--file", str(self.jsonl), "--chunk-size", "500")
        before = self._status()["current_version"]
        proc = run_cli("--db", str(self.db), "commit", "--batch", str(bid),
                       env={"FINFAULT": "commit_post_switch"}, check=False)
        self.assertEqual(proc.returncode, 86)

        # 事务未提交 -> 整体回滚，对外仍是完整旧版。
        run_cli("--db", str(self.db), "recover")
        status = self._status()
        self.assertEqual(status["current_version"], before)
        with LedgerStore(self.db) as s:
            self.assertEqual(s.version_totals(before)["rows"], 0)
            self.assertFalse(any(v["state"] == "building"
                                 for v in s.list_versions()))

        # 重提一次，成功切换。
        run_cli("--db", str(self.db), "commit", "--batch", str(bid))
        with LedgerStore(self.db) as s:
            self.assertEqual(s.version_totals(s.current_version_id())["rows"], 1000)
            self.assertEqual(s.get_batch(bid)["state"], "committed")

    def test_05_recover_is_idempotent(self):
        bid = self._open()
        run_cli("--db", str(self.db), "upload", "--batch", str(bid),
                "--file", str(self.jsonl), "--chunk-size", "500")
        r1 = json.loads(run_cli("--db", str(self.db), "recover").stdout)
        r2 = json.loads(run_cli("--db", str(self.db), "recover").stdout)
        self.assertEqual(r2, {"orphan_versions": [], "reset_batches": []})
        self.assertEqual(r1["orphan_versions"], [])

    def test_06_recover_cleans_committed_orphan_building_version(self):
        # 模拟更极端的场景：候选版已在独立事务落盘（building），
        # 但切换未发生进程就没了。recover 必须删掉孤儿版并放行批次重提。
        bid = self._open()
        run_cli("--db", str(self.db), "upload", "--batch", str(bid),
                "--file", str(self.jsonl), "--chunk-size", "500")
        import sqlite3
        from src.ledger.db import connect
        conn = connect(self.db)
        now = 1.0
        cur = conn.execute(
            "INSERT INTO versions(state,batch_id,created_at) VALUES('building',?,?)",
            (bid, now))
        orphan_vid = cur.lastrowid
        conn.execute(
            "INSERT INTO expenses(version_id,source,voucher_no,revision,is_cancel,"
            "amount_cents,payload,content_hash) VALUES(?,?,?,?,?,?,?,?)",
            (orphan_vid, "ERP", "GHOST", 1, 0, 1,
             '{"amount_cents":1}', "deadbeef"))
        conn.execute(
            "UPDATE batches SET state='committing', candidate_version_id=? WHERE id=?",
            (orphan_vid, bid))
        conn.close()

        rec = json.loads(run_cli("--db", str(self.db), "recover").stdout)
        self.assertEqual(rec["orphan_versions"], [orphan_vid])
        self.assertEqual(rec["reset_batches"], [bid])
        # 孤儿费用行随版本一起删除。
        conn = connect(self.db)
        ghost = conn.execute(
            "SELECT 1 FROM expenses WHERE version_id=?", (orphan_vid,)).fetchone()
        self.assertIsNone(ghost)
        conn.close()
        # 批次可重新提交成功。
        run_cli("--db", str(self.db), "commit", "--batch", str(bid))
        with LedgerStore(self.db) as s:
            self.assertEqual(s.version_totals(s.current_version_id())["rows"], 1000)


if __name__ == "__main__":
    unittest.main()
