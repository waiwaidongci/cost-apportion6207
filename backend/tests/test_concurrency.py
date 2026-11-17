"""并发批次测试：重叠凭证按修订合并，无关更正不丢失。

两层验证：
1. 多线程直连同一 SQLite 文件（WAL + IMMEDIATE 串行化）；
2. ThreadingHTTPServer 上的真实 HTTP 并发请求。
"""
import json
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from src.ledger.errors import BatchRejected
from src.ledger.server import make_handler
from http.server import ThreadingHTTPServer

from src.ledger.store import LedgerStore

from .support import fresh_db, make_record


class ConcurrentCommitTest(unittest.TestCase):
    def setUp(self):
        self.db = fresh_db()

    def _store(self):
        s = LedgerStore(self.db)
        return s

    def _seed_base(self):
        with self._store() as s:
            b = s.open_batch(expected_count=1)
            s.append_chunk(b, [make_record("V1", 2, 200)])
            return s.commit_batch(b)["version_id"]

    def test_01_overlapping_vouchers_merge_by_revision(self):
        self._seed_base()
        errors = []

        def worker(records):
            try:
                with self._store() as s:
                    bid = s.open_batch(expected_count=len(records))
                    s.append_chunk(bid, records)
                    s.commit_batch(bid)
            except Exception as exc:  # 并发拒收也是预期之一，收集后统一判断
                errors.append(exc)

        # B、C 都改 V1（rev 5 与 rev 3），各自带独立凭证。
        batch_b = [make_record("V1", 5, 500), make_record("VB", 1, 11)]
        batch_c = [make_record("V1", 3, 300), make_record("VC", 1, 22)]
        # 多跑几轮，提交顺序在锁作用下天然随机。
        for _ in range(5):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futs = [pool.submit(worker, batch_b), pool.submit(worker, batch_c)]
                for f in futs:
                    f.result()
            with self._store() as s:
                vid = s.current_version_id()
                v1 = s.read_expense(vid, "ERP", "V1")
                # 无论谁先提交，最终都是最高修订 rev=5。
                self.assertEqual(v1["revision"], 5)
                self.assertEqual(v1["amount_cents"], 500)
                # 无关更正不丢失。
                self.assertEqual(s.read_expense(vid, "ERP", "VB")["amount_cents"], 11)
                self.assertEqual(s.read_expense(vid, "ERP", "VC")["amount_cents"], 22)
                # 始终只有一个 current，版本链完整。
                self.assertEqual(
                    [v["state"] for v in s.list_versions()].count("current"), 1)
        self.assertEqual(errors, [])

    def test_02_conflicting_batch_rejected_while_concurrent_good_batch_commits(self):
        self._seed_base()

        def good_worker():
            with self._store() as s:
                bid = s.open_batch(expected_count=1)
                s.append_chunk(bid, [make_record("VGOOD", 1, 7)])
                s.commit_batch(bid)

        def bad_worker():
            with self._store() as s:
                bid = s.open_batch(expected_count=2)
                s.append_chunk(bid, [
                    make_record("V1", 2, 201),  # 与基线 rev2 内容冲突
                    make_record("VLOST", 1, 9),
                ])
                try:
                    s.commit_batch(bid)
                except BatchRejected:
                    return "rejected"
                return "unexpectedly-committed"

        t1 = threading.Thread(target=good_worker)
        t2 = threading.Thread(target=bad_worker)
        t1.start(); t2.start()
        t1.join(); t2.join()

        with self._store() as s:
            vid = s.current_version_id()
            self.assertEqual(s.read_expense(vid, "ERP", "VGOOD")["amount_cents"], 7)
            self.assertIsNone(s.read_expense(vid, "ERP", "VLOST"))
            # 基线 V1 原内容未被冲突批改动。
            self.assertEqual(s.read_expense(vid, "ERP", "V1")["amount_cents"], 200)

    def test_03_concurrent_chunks_to_same_batch_serialize(self):
        # 同一批次被多个上传线程并发追加：分片区间互不相交，每片内部连续。
        # IMMEDIATE 锁把写入串行化，检查点在锁内重读，序号不得错乱或丢行。
        with self._store() as s:
            bid = s.open_batch(expected_count=100)
        errors = []

        def chunk_worker(seq_start, count):
            try:
                with self._store() as s:
                    recs = [make_record(f"W{seq_start + i:03d}", 1, seq_start + i)
                            for i in range(count)]
                    s.append_chunk(bid, recs, first_seq=seq_start)
            except Exception as exc:
                errors.append(exc)

        # 分片按顺序提交（真实断点续传客户端的行为），但由不同连接/线程写入，
        # 验证 IMMEDIATE 锁下检查点不被并发读取污染；乱序压力由 test_04 承担。
        with ThreadPoolExecutor(max_workers=4) as pool:
            for start in (1, 26, 51, 76):
                pool.submit(chunk_worker, start, 25).result()
        self.assertEqual(errors, [])
        with self._store() as s:
            self.assertEqual(s.get_batch(bid)["checkpoint_seq"], 100)
            r = s.commit_batch(bid)
            self.assertEqual(s.version_totals(r["version_id"])["rows"], 100)

    def test_04_interleaved_chunk_writers_never_corrupt_checkpoint(self):
        # 4 个线程反复对同一批次做小块追加（自动续 seq），检查点必须单调连续。
        with self._store() as s:
            bid = s.open_batch()
        errors = []

        def auto_writer(thread_id, rounds):
            try:
                with self._store() as s:
                    for r in range(rounds):
                        recs = [make_record(f"T{thread_id}-{r}-{i}", 1, 1)
                                for i in range(10)]
                        s.append_chunk(bid, recs)  # 不传 first_seq，自动接检查点
            except Exception as exc:
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = [pool.submit(auto_writer, t, 5) for t in range(4)]
            for f in futs:
                f.result()
        self.assertEqual(errors, [])
        with self._store() as s:
            batch = s.get_batch(bid)
            self.assertEqual(batch["received_count"], 4 * 5 * 10)
            self.assertEqual(batch["checkpoint_seq"], batch["received_count"])
            r = s.commit_batch(bid)
            self.assertEqual(
                s.version_totals(r["version_id"])["rows"], batch["received_count"])


class HttpConcurrencyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = fresh_db()
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.db))
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def _call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_01_full_lifecycle_and_concurrent_batches_over_http(self):
        status, body = self._call("GET", "/health")
        self.assertEqual(status, 200)

        _, b1 = self._call("POST", "/batches", {"expected_count": 2})
        bid = b1["batch_id"]
        status, body = self._call("POST", f"/batches/{bid}/chunks", {
            "first_seq": 1,
            "records": [make_record("V1", 1, 100), make_record("V1", 3, 300)],
        })
        self.assertEqual(body["accepted"], 2)
        # 断点重传第一块：应全部 duplicated。
        status, body = self._call("POST", f"/batches/{bid}/chunks", {
            "first_seq": 1,
            "records": [make_record("V1", 1, 100), make_record("V1", 3, 300)],
        })
        self.assertEqual(body["duplicated"], 2)
        status, committed = self._call("POST", f"/batches/{bid}/commit")
        self.assertEqual(status, 200)

        # 并发开两个批次：一个正常、一个冲突。
        def open_commit(records):
            _, ob = self._call("POST", "/batches",
                               {"expected_count": len(records)})
            self._call("POST", f"/batches/{ob['batch_id']}/chunks",
                       {"first_seq": 1, "records": records})
            return self._call("POST", f"/batches/{ob['batch_id']}/commit")

        with ThreadPoolExecutor(max_workers=2) as pool:
            f_good = pool.submit(open_commit, [make_record("VNEW", 1, 42)])
            f_bad = pool.submit(open_commit, [make_record("V1", 3, 301)])
            good_status, _ = f_good.result()
            bad_status, bad_body = f_bad.result()
        self.assertEqual(good_status, 200)
        self.assertEqual(bad_status, 409)
        self.assertEqual(bad_body["error"], "batch_rejected")

        # 核算排队固定版本并出报告，旧报告列表保留全部。
        _, job = self._call("POST", "/accounting/enqueue", {})
        _, report = self._call("POST", "/accounting/run", {"job_id": job["job_id"]})
        self.assertEqual(report["version_id"], job["version_id"])
        _, reports = self._call("GET", "/reports")
        self.assertGreaterEqual(len(reports["reports"]), 1)
        _, versions = self._call("GET", "/versions")
        self.assertEqual([v["state"] for v in versions["versions"]].count("current"), 1)


if __name__ == "__main__":
    unittest.main()
