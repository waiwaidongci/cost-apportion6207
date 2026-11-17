"""批次补传引擎的领域异常。"""


class LedgerError(Exception):
    """引擎层异常基类，携带稳定的业务错误码。"""

    code = "ledger_error"

    def __init__(self, message, *, details=None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class BatchNotFound(LedgerError):
    code = "batch_not_found"


class BatchStateError(LedgerError):
    """批次状态不允许当前操作（已提交/已拒绝后不可再写等）。"""

    code = "batch_state"


class RecordValidationError(LedgerError):
    code = "record_invalid"


class SequenceGap(LedgerError):
    """客户端声明的 seq 不连续，防止漏传。"""

    code = "sequence_gap"


class BatchRejected(LedgerError):
    """校验失败：修订相同但内容冲突，全批拒收（不落任何更正）。"""

    code = "batch_rejected"


class FaultInjected(LedgerError):  # pragma: no cover - 仅测试触发
    code = "fault_injected"


class PinError(LedgerError):
    code = "pin_error"
