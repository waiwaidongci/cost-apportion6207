"""十万条补传规模与 128MB 内存预算验证。

通过 /proc/<pid>/status 采样压测子进程的 VmRSS 峰值；
另含断点续传在十万级数据上的端到端验证与磁盘文件检查。
"""
import json
import os
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

from src.ledger.store import LedgerStore

from .support import BACKEND_ROOT, fresh_db, make_record, run_cli, write_jsonl

MEMORY_LIMIT_MB = 128
N = 100_000


def sample_rss(pid, samples, stop):
    """轮询 /proc/<pid>/status 取 VmRSS（kB）。"""
    while not stop.is_set():
        try:
            with open(f"/proc/{pid}/status", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        samples.append(int(line.split()[1]))
                        break
        except (FileNotFoundError, ProcessLookupError):
            return
        time.sleep(0.02)


class LargeScaleTest(unittest.TestCase):
    def _run_driver(self, db_path, n=N):
        proc = subprocess.Popen(
            [sys.executable, "-m", "tests._driver_large", str(db_path), str(n)],
            cwd=BACKEND_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=dict(os.environ))
        samples: list[int] = []
        stop = threading.Event()
        t = threading.Thread(target=sample_rss, args=(proc.pid, samples, stop))
        t.start()
        out, err = proc.communicate()
        stop.set(); t.join()
        self.assertEqual(proc.returncode, 0, msg=f"driver failed: {err}")
        result = json.loads(out.strip().splitlines()[-1])
        result["peak_rss_kb"] = max(samples) if samples else 0
        return result

    def test_01_100k_rows_within_128mb(self):
        db = fresh_db()
        result = self._run_driver(db)
        self.assertEqual(result["rows"], N)
        # 金额守恒：每笔 100 + i%1000。
        expected = sum(100 + (i % 1000) for i in range(N))
        self.assertEqual(result["amount_cents"], expected)
        peak_mb = result["peak_rss_kb"] / 1024
        print(f"\n十万条补传：耗时 {result['elapsed_seconds']}s，"
              f"进程峰值 RSS {peak_mb:.1f}MB（预算 {MEMORY_LIMIT_MB}MB）")
        self.assertLess(peak_mb, MEMORY_LIMIT_MB,
                        f"峰值内存 {peak_mb:.1f}MB 超出 {MEMORY_LIMIT_MB}MB 预算")

    def test_02_resume_on_large_file_no_double_entry(self):
        db = fresh_db()
        jsonl = db.with_suffix(".jsonl")
        # 流式写文件，同样不把十万条放进内存。
        def rows():
            for i in range(N):
                yield make_record(f"V{i:06d}", 1, 100 + i)
        with open(jsonl, "w", encoding="utf-8") as fh:
            for r in rows():
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")

        bid = json.loads(run_cli("--db", str(db), "open",
                                 "--expected", str(N)).stdout)["batch_id"]
        # 在第 25 块崩溃（每块 2000 条 -> 5 万块）。
        crashed = run_cli("--db", str(db), "upload", "--batch", str(bid),
                          "--file", str(jsonl), "--chunk-size", "2000",
                          env={"FINFAULT": "after_chunk:25"}, check=False)
        self.assertEqual(crashed.returncode, 86)

        run_cli("--db", str(db), "recover")
        # 用同一完整文件续传；前 5 万条全部走幂等分支。
        resumed = json.loads(run_cli(
            "--db", str(db), "upload", "--batch", str(bid),
            "--file", str(jsonl), "--chunk-size", "2000").stdout)
        self.assertEqual(resumed["accepted"], N - 50_000)
        self.assertEqual(resumed["duplicated"], 50_000)
        run_cli("--db", str(db), "commit", "--batch", str(bid))

        with LedgerStore(db) as s:
            totals = s.version_totals(s.current_version_id())
            self.assertEqual(totals["rows"], N)
            self.assertEqual(totals["amount_cents"],
                             sum(100 + i for i in range(N)))
            batch = s.get_batch(bid)
            self.assertEqual(batch["state"], "committed")
        # WAL 检查点后磁盘文件可独立打开（无依赖未合并的 WAL 状态）。
        run_cli("--db", str(db), "recover")

    def test_03_second_100k_batch_merges_into_new_version(self):
        db = fresh_db()
        first = self._run_driver(db)
        # 第二批：对全部凭证给更高修订（金额 +1），应整体覆盖为新版。
        def gen2():
            for i in range(N):
                yield make_record(f"V{i:06d}", 3, 100 + (i % 1000) + 1)
        with LedgerStore(db, chunk_size=2000) as s:
            bid = s.open_batch(expected_count=N)
            s.append_stream(bid, gen2())
            r = s.commit_batch(bid)
            totals = s.version_totals(r["version_id"])
            self.assertEqual(totals["rows"], N)
            self.assertEqual(totals["amount_cents"], first["amount_cents"] + N)
            # 旧版本仍完整保留、可固定、可出报告。
            old = first["version_id"]
            self.assertEqual(s.version_totals(old)["amount_cents"],
                             first["amount_cents"])
            self.assertEqual(s.pin_version("job-old", old), old)

    def test_04_disk_artifacts_include_wal_and_db(self):
        db = fresh_db()
        self._run_driver(db, n=10_000)
        files = {p.name: p.stat().st_size for p in db.parent.iterdir()}
        self.assertIn(db.name, files)
        # 数据确实落盘而非驻留内存。
        self.assertGreater(files[db.name], 100_000)


if __name__ == "__main__":
    unittest.main()
