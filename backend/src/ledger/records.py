"""费用记录的规范化、校验与内容指纹。

识别键：(source, voucher_no)。
修订号 revision：非负整数，乱序记录取最高修订（LWW）。
内容指纹 content_hash：对规范化载荷做 SHA256，用于"修订相同却内容冲突"的判定。
撤销：is_cancel=True 写墓碑，墓碑本身带修订号，永久保留修订号不做物理删除。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .errors import RecordValidationError

# 允许进入载荷的业务字段；识别键与修订号单独存放。
PAYLOAD_FIELDS = ("period", "department", "category", "amount_cents")
# 撤销墓碑只需要金额以外的说明性字段；金额强制归零。
MAX_STR = 120


def _require_str(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > MAX_STR:
        raise RecordValidationError(f"{name} 必须为 1..{MAX_STR} 字符的非空字符串",
                                    details={"field": name})
    return value.strip()


def normalize(record: dict) -> dict:
    """把一条外部记录校验并规范化为内部结构。

    返回 {source, voucher_no, revision, is_cancel, amount_cents, payload, content_hash}。
    任何字段非法都抛 RecordValidationError，调用方负责把整批标记为校验失败。
    """
    if not isinstance(record, dict):
        raise RecordValidationError("记录必须是对象")
    try:
        source = _require_str(record.get("source"), "source 来源")
        voucher_no = _require_str(record.get("voucher_no"), "voucher_no 凭证号")
    except RecordValidationError:
        raise

    rev = record.get("revision")
    if isinstance(rev, bool) or not isinstance(rev, int) or rev < 0:
        raise RecordValidationError("revision 修订号必须为非负整数",
                                    details={"voucher_no": record.get("voucher_no")})

    is_cancel = bool(record.get("is_cancel", False))

    payload: dict[str, Any] = {}
    amount = record.get("amount_cents", 0)
    if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
        raise RecordValidationError("amount_cents 必须为非负整数分",
                                    details={"voucher_no": voucher_no})
    payload["amount_cents"] = 0 if is_cancel else amount
    for field in ("period", "department", "category"):
        payload[field] = _require_str(record.get(field), field)

    # 规范化载荷：固定键序、无多余空白语义，保证同内容一定同指纹。
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":"))
    content_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return {
        "source": source,
        "voucher_no": voucher_no,
        "revision": rev,
        "is_cancel": is_cancel,
        "amount_cents": payload["amount_cents"],
        "payload": payload,
        "content_hash": content_hash,
    }
