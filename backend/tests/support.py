"""测试辅助。"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]


def make_record(voucher, rev, amount, *, source="ERP", cancel=False,
                period="2026-11", department="光学研究", category="耗材"):
    rec = {
        "source": source,
        "voucher_no": voucher,
        "revision": rev,
        "period": period,
        "department": department,
        "category": category,
        "amount_cents": amount,
    }
    if cancel:
        rec["is_cancel"] = True
    return rec


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def run_cli(*args, env=None, check=True):
    """以子进程方式运行 CLI（故障注入用例需要真实的进程边界）。"""
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    proc = subprocess.run(
        [sys.executable, "-m", "src.ledger.cli", *args],
        cwd=BACKEND_ROOT, env=full_env, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise AssertionError(
            f"CLI 失败 rc={proc.returncode}\nSTDOUT:{proc.stdout}\nSTDERR:{proc.stderr}")
    return proc


def fresh_db():
    d = tempfile.mkdtemp(prefix="fin-ledger-")
    return Path(d) / "ledger.db"
