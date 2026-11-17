"""十万条补传压测驱动（被 test_large_scale 以子进程方式调用）。

生成 100,000 条费用（流式产出，不持有全量列表），按 1,000 条/块落盘、提交，
打印 JSON 结果：行数、金额、耗时、最终版本。
"""
import json
import sys
import time

from src.ledger.store import LedgerStore
from tests.support import make_record


def gen(n):
    # 生成器而非列表：任意时刻内存里只有当前块。
    for i in range(n):
        # 一半凭证在第二批做修订更新，另一半保持稳定。
        rev = 2 if i % 2 == 0 else 1
        yield make_record(f"V{i:06d}", rev, 100 + (i % 1000))


def main(db_path: str, n: int = 100_000):
    started = time.time()
    with LedgerStore(db_path, chunk_size=1000) as s:
        bid = s.open_batch(expected_count=n)
        r = s.append_stream(bid, gen(n))
        assert r["total_seen"] == n
        commit = s.commit_batch(bid)
        totals = s.version_totals(commit["version_id"])
        result = {
            "rows": totals["rows"],
            "amount_cents": totals["amount_cents"],
            "version_id": commit["version_id"],
            "elapsed_seconds": round(time.time() - started, 3),
            "db_bytes": 0,  # 由调用方填充
        }
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 100_000)
