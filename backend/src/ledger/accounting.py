"""核算排队与执行：排队瞬间固定底稿版本。

- 排队（enqueue）：在当前 current 版本上打 pin，返回 job 与 version_id。
  之后任何批次切换都不影响该任务，它永远读固定的完整一版（旧版或新版）。
- 执行（run_job）：流式读取固定版本，逐行汇总，报告按版本永久保存；
  旧版报告不会被后续切换覆盖，满足"保留原报告"。
"""
from __future__ import annotations

import uuid
from collections import defaultdict

from .store import LedgerStore


def enqueue(store: LedgerStore, *, job_id: str | None = None) -> dict:
    job_id = job_id or "job-" + uuid.uuid4().hex
    version_id = store.pin_version(job_id)  # 固定此刻的 current
    return {"job_id": job_id, "version_id": version_id, "state": "queued"}


def run_job(store: LedgerStore, job_id: str) -> dict:
    version_id = store.pinned_version(job_id)
    pivot: dict[tuple, int] = defaultdict(int)
    total = 0
    active_rows = canceled_rows = 0
    for row in store.iter_version(version_id):
        if row["is_cancel"]:
            canceled_rows += 1
            continue
        active_rows += 1
        total += row["amount_cents"]
        payload = row["payload"]
        key = (payload["period"], payload["department"], payload["category"])
        pivot[key] += row["amount_cents"]

    body = {
        "job_id": job_id,
        "version_id": version_id,
        "summary": {
            "active_rows": active_rows,
            "canceled_rows": canceled_rows,
            "amount_cents": total,
        },
        "pivot": [
            {"period": p, "department": d, "category": c, "amount_cents": v}
            for (p, d, c), v in sorted(pivot.items())
        ],
    }
    report_id = store.save_report(version_id, "allocation-summary", body, job_id=job_id)
    body["report_id"] = report_id
    return body
