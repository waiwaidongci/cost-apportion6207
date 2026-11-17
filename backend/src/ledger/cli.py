"""批次补传引擎命令行（运维与压测入口，全部走标准库）。

示例：
  python -m src.ledger.cli init --db /tmp/fin.db
  python -m src.ledger.cli open --db /tmp/fin.db --expected 100000
  python -m src.ledger.cli upload --db /tmp/fin.db --batch 1 --file records.jsonl
  python -m src.ledger.cli commit --db /tmp/fin.db --batch 1
  python -m src.ledger.cli status --db /tmp/fin.db
  python -m src.ledger.cli totals --db /tmp/fin.db
  python -m src.ledger.cli recover --db /tmp/fin.db
故障注入：FINFAULT=after_chunk:3 python -m src.ledger.cli upload ...
"""
from __future__ import annotations

import argparse
import json
import sys

from .errors import LedgerError
from .store import LedgerStore


def _iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="fin-ledger")
    p.add_argument("--db", required=True, help="SQLite 数据库路径")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="初始化结构并做崩溃恢复")
    op = sub.add_parser("open", help="开批")
    op.add_argument("--expected", type=int, default=None)
    up = sub.add_parser("upload", help="流式上传 JSONL（断点续传）")
    up.add_argument("--batch", type=int, required=True)
    up.add_argument("--file", required=True)
    up.add_argument("--chunk-size", type=int, default=1000)
    cm = sub.add_parser("commit", help="校验并原子切换版本")
    cm.add_argument("--batch", type=int, required=True)
    sub.add_parser("status", help="批次与版本状态")
    sub.add_parser("totals", help="当前版本汇总")
    rc = sub.add_parser("recover", help="崩溃恢复")
    rc.add_argument("--strict", action="store_true")
    ex = sub.add_parser("expenses", help="导出当前版本费用（NDJSON）")
    ex.add_argument("--version", type=int, default=None)
    eq = sub.add_parser("enqueue", help="核算排队（固定当前完整版本）")
    eq.add_argument("--job", default=None)
    rj = sub.add_parser("run-job", help="执行已排队的核算任务")
    rj.add_argument("--job", required=True)
    sub.add_parser("reports", help="列出全部报告（旧版永久保留）")
    rp = sub.add_parser("report", help="查看单份报告")
    rp.add_argument("--id", type=int, required=True)

    args = p.parse_args(argv)
    with LedgerStore(args.db) as store:
        if args.cmd == "init":
            print(json.dumps(store.recover(), ensure_ascii=False))
        elif args.cmd == "open":
            bid = store.open_batch(expected_count=args.expected)
            print(json.dumps({"batch_id": bid}, ensure_ascii=False))
        elif args.cmd == "upload":
            store.chunk_size = args.chunk_size
            result = store.append_stream(args.batch, _iter_jsonl(args.file))
            print(json.dumps(result, ensure_ascii=False))
        elif args.cmd == "commit":
            print(json.dumps(store.commit_batch(args.batch), ensure_ascii=False))
        elif args.cmd == "status":
            print(json.dumps({
                "current_version": store.current_version_id(),
                "versions": store.list_versions(),
                "batches": store.list_batches(),
            }, ensure_ascii=False, indent=2))
        elif args.cmd == "totals":
            print(json.dumps(store.version_totals(store.current_version_id()),
                             ensure_ascii=False, indent=2))
        elif args.cmd == "recover":
            print(json.dumps(store.recover(), ensure_ascii=False))
        elif args.cmd == "expenses":
            vid = args.version or store.current_version_id()
            for row in store.iter_version(vid):
                sys.stdout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        elif args.cmd == "enqueue":
            from src.ledger import accounting
            print(json.dumps(accounting.enqueue(store, job_id=args.job),
                             ensure_ascii=False))
        elif args.cmd == "run-job":
            from src.ledger import accounting
            print(json.dumps(accounting.run_job(store, args.job),
                             ensure_ascii=False, indent=2))
        elif args.cmd == "reports":
            print(json.dumps(store.list_reports(), ensure_ascii=False, indent=2))
        elif args.cmd == "report":
            print(json.dumps(store.get_report(args.id), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LedgerError as exc:
        sys.stderr.write(json.dumps(
            {"error": exc.code, "message": exc.message, "details": exc.details},
            ensure_ascii=False) + "\n")
        raise SystemExit(2)
