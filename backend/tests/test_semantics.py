"""批次补传引擎核心语义测试（同进程）。

覆盖需求：按来源+凭证号识别、乱序最高修订、撤销墓碑留修订号、
旧记录晚到作废、同修订冲突全批拒收、断点幂等、版本固定、旧报告保留。
"""
import unittest

from src.ledger.errors import (BatchRejected, BatchStateError,
                                RecordValidationError, SequenceGap)
from src.ledger.store import LedgerStore

from .support import fresh_db, make_record


class SemanticsTest(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db()
        self.store = LedgerStore(self.db)

    def tearDown(self):
        self.store.close()

    def test_01_empty_ledger_has_readable_current_version(self):
        # 空库即有完整可读的第 0 版，核算任何时候都不读到半成品。
        vid = self.store.current_version_id()
        self.assertEqual(self.store.version_totals(vid)["rows"], 0)
        self.assertEqual(
            [v["state"] for v in self.store.list_versions()].count("current"), 1)

    def test_02_out_of_order_keeps_highest_revision(self):
        bid = self.store.open_batch(expected_count=3)
        # 乱序：rev 1 -> rev 5 -> rev 3
        self.store.append_chunk(bid, [
            make_record("V1", 1, 100),
            make_record("V1", 5, 500),
            make_record("V1", 3, 300),
        ])
        r = self.store.commit_batch(bid)
        row = self.store.read_expense(r["version_id"], "ERP", "V1")
        self.assertEqual(row["revision"], 5)
        self.assertEqual(row["amount_cents"], 500)

    def test_03_cancel_keeps_revision_number_as_tombstone(self):
        bid = self.store.open_batch(expected_count=2)
        self.store.append_chunk(bid, [
            make_record("V2", 2, 200),
            make_record("V2", 3, 0, cancel=True),
        ])
        r = self.store.commit_batch(bid)
        row = self.store.read_expense(r["version_id"], "ERP", "V2")
        self.assertEqual(row["is_cancel"], 1)
        self.assertEqual(row["revision"], 3)  # 修订号随墓碑保留
        self.assertEqual(row["amount_cents"], 0)
        # 汇总不把墓碑计入金额，但行仍在。
        totals = self.store.version_totals(r["version_id"])
        self.assertEqual(totals["canceled"], 1)
        self.assertEqual(totals["amount_cents"], 0)

    def test_04_late_old_record_is_void(self):
        b1 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b1, [make_record("V1", 4, 400)])
        v1 = self.store.commit_batch(b1)["version_id"]

        # 晚到的 rev=2 不能覆盖已生效的 rev=4
        b2 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b2, [make_record("V1", 2, 999)])
        v2 = self.store.commit_batch(b2)["version_id"]
        row = self.store.read_expense(v2, "ERP", "V1")
        self.assertEqual(row["revision"], 4)
        self.assertEqual(row["amount_cents"], 400)
        # 旧版数据原封不动（完整旧版仍可查）。
        self.assertEqual(
            self.store.read_expense(v1, "ERP", "V1")["amount_cents"], 400)

    def test_05_same_revision_content_conflict_rejects_whole_batch(self):
        b1 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b1, [make_record("V1", 2, 100)])
        self.store.commit_batch(b1)
        before = self.store.current_version_id()

        b2 = self.store.open_batch(expected_count=2)
        self.store.append_chunk(b2, [
            make_record("V1", 2, 101),  # 同 rev 不同内容（跨版本冲突）
            make_record("V9", 1, 50),   # 无关更正也不得生效
        ])
        with self.assertRaises(BatchRejected):
            self.store.commit_batch(b2)
        # 全批拒收：current 不变，V9 不存在，批次 rejected 且无候选版残留。
        self.assertEqual(self.store.current_version_id(), before)
        self.assertEqual(self.store.get_batch(b2)["state"], "rejected")
        self.assertIsNone(self.store.get_batch(b2)["candidate_version_id"])
        self.assertIsNone(self.store.read_expense(before, "ERP", "V9"))
        buildings = [v for v in self.store.list_versions() if v["state"] == "building"]
        self.assertEqual(buildings, [])

    def test_06_intra_batch_same_rev_conflict_rejected(self):
        bid = self.store.open_batch(expected_count=2)
        self.store.append_chunk(bid, [
            make_record("V1", 3, 100),
            make_record("V1", 3, 200),  # 同键同 rev，不同金额
        ])
        with self.assertRaises(BatchRejected):
            self.store.commit_batch(bid)
        self.assertEqual(self.store.get_batch(bid)["state"], "rejected")

    def test_07_resume_is_idempotent_no_double_entry(self):
        bid = self.store.open_batch(expected_count=4)
        self.store.append_chunk(bid, [make_record(f"V{i}", 1, i) for i in range(2)],
                                first_seq=1)
        # 重传前两条（断点重发）+ 后两条
        r = self.store.append_chunk(
            bid, [make_record("V0", 1, 0), make_record("V1", 1, 1),
                  make_record("V2", 1, 2), make_record("V3", 1, 3)],
            first_seq=1)
        self.assertEqual(r["duplicated"], 2)
        self.assertEqual(r["accepted"], 2)
        r = self.store.commit_batch(bid)
        totals = self.store.version_totals(r["version_id"])
        self.assertEqual(totals["rows"], 4)
        self.assertEqual(totals["amount_cents"], 0 + 1 + 2 + 3)

    def test_08_sequence_gap_is_refused(self):
        bid = self.store.open_batch()
        with self.assertRaises(SequenceGap):
            self.store.append_chunk(bid, [make_record("V1", 1, 1)], first_seq=5)

    def test_09_replay_with_different_content_at_same_seq_rejected(self):
        bid = self.store.open_batch()
        self.store.append_chunk(bid, [make_record("V1", 1, 100)], first_seq=1)
        with self.assertRaises(BatchRejected):
            self.store.append_chunk(bid, [make_record("V1", 1, 200)], first_seq=1)

    def test_10_invalid_record_does_not_persist_chunk(self):
        bid = self.store.open_batch()
        before = self.store.get_batch(bid)["checkpoint_seq"]
        with self.assertRaises(RecordValidationError):
            self.store.append_chunk(bid, [
                make_record("V1", 1, 100),
                {"source": "ERP", "voucher_no": "V2", "revision": -1,
                 "period": "2026-11", "department": "d", "category": "c",
                 "amount_cents": 1},
            ], first_seq=1)
        # 整块回滚，检查点不动。
        self.assertEqual(self.store.get_batch(bid)["checkpoint_seq"], before)

    def test_11_committed_batch_cannot_append_or_recommit(self):
        bid = self.store.open_batch(expected_count=1)
        self.store.append_chunk(bid, [make_record("V1", 1, 100)])
        self.store.commit_batch(bid)
        with self.assertRaises(BatchStateError):
            self.store.append_chunk(bid, [make_record("V2", 1, 100)])
        with self.assertRaises(BatchStateError):
            self.store.commit_batch(bid)

    def test_12_accounting_pins_a_complete_version(self):
        from src.ledger import accounting
        b1 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b1, [make_record("V1", 1, 100)])
        v1 = self.store.commit_batch(b1)["version_id"]

        job = accounting.enqueue(self.store, job_id="job-A")
        self.assertEqual(job["version_id"], v1)
        report_a = accounting.run_job(self.store, "job-A")
        self.assertEqual(report_a["summary"]["amount_cents"], 100)

        # 之后再切换版本，旧任务仍读固定旧版，报告按版本留存。
        b2 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b2, [make_record("V1", 2, 888)])
        v2 = self.store.commit_batch(b2)["version_id"]
        self.assertEqual(self.store.pinned_version("job-A"), v1)
        report_again = accounting.run_job(self.store, "job-A")
        self.assertEqual(report_again["version_id"], v1)
        self.assertEqual(report_again["summary"]["amount_cents"], 100)
        # 新任务固定新版。
        job_b = accounting.enqueue(self.store, job_id="job-B")
        self.assertEqual(job_b["version_id"], v2)
        self.assertEqual(
            accounting.run_job(self.store, "job-B")["summary"]["amount_cents"], 888)
        # 两份报告都在，旧报告未被覆盖。
        reports = self.store.list_reports()
        self.assertGreaterEqual(len(reports), 3)
        versions_used = {self.store.get_report(r["id"])["version_id"]
                         for r in reports}
        self.assertIn(v1, versions_used)
        self.assertIn(v2, versions_used)

    def test_13_revision_equal_content_equal_is_idempotent_not_conflict(self):
        # 同 rev 同内容重复送达属于幂等重发，不算冲突。
        b1 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b1, [make_record("V1", 2, 100)])
        self.store.commit_batch(b1)
        b2 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b2, [make_record("V1", 2, 100)])
        r = self.store.commit_batch(b2)  # 不抛错
        # 内容没变，候选版里该键与基线一致（rev 不高于基线 -> 不并入）。
        self.assertEqual(
            self.store.read_expense(r["version_id"], "ERP", "V1")["revision"], 2)

    def test_14_higher_revision_can_cancel_then_revive(self):
        b1 = self.store.open_batch(expected_count=2)
        self.store.append_chunk(b1, [
            make_record("V1", 1, 100),
            make_record("V1", 2, 0, cancel=True),
        ])
        v1 = self.store.commit_batch(b1)["version_id"]
        self.assertEqual(self.store.read_expense(v1, "ERP", "V1")["is_cancel"], 1)
        # 更高修订可以"复活"凭证。
        b2 = self.store.open_batch(expected_count=1)
        self.store.append_chunk(b2, [make_record("V1", 3, 150)])
        v2 = self.store.commit_batch(b2)["version_id"]
        row = self.store.read_expense(v2, "ERP", "V1")
        self.assertEqual(row["is_cancel"], 0)
        self.assertEqual(row["revision"], 3)
        self.assertEqual(row["amount_cents"], 150)

    def test_15_identification_includes_source(self):
        # 同凭证号不同来源是两条费用。
        bid = self.store.open_batch(expected_count=2)
        self.store.append_chunk(bid, [
            make_record("V1", 1, 100, source="ERP"),
            make_record("V1", 1, 200, source="OA"),
        ])
        r = self.store.commit_batch(bid)
        self.assertEqual(
            self.store.read_expense(r["version_id"], "ERP", "V1")["amount_cents"], 100)
        self.assertEqual(
            self.store.read_expense(r["version_id"], "OA", "V1")["amount_cents"], 200)


if __name__ == "__main__":
    unittest.main()
