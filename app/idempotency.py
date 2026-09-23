"""写入请求的幂等键记录与并发去重。

同一 (caller, key) 下的并发重复请求：
- 第一个到达的请求进入 in_progress 并真正执行副作用；
- 其执行期间到达的同键同体请求不会重复执行，而是得到
  IDEMPOTENCY_IN_PROGRESS（可解释，调用方可稍后重试读取结果）；
- 首个请求完成后到达的重复请求直接重放首次结果（replay），不产生副作用；
- 同键但请求体不同的请求判定为 IDEMPOTENCY_KEY_CONFLICT。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Literal

from app.errors import APIError, ErrorCode

AcquireStatus = Literal["acquired", "in_progress", "replay", "conflict"]

# 首个请求仍在执行时，建议同键重复请求等待多久后重试（秒）
RETRY_AFTER_SECONDS = 1


@dataclass
class IdempotencyRecord:
    key: str
    caller_id: str
    request_fingerprint: str
    status: str = "in_progress"  # in_progress | completed
    response_status: int = 0
    response_body: dict | None = None
    resource_id: str | None = None
    lock: Lock = field(default_factory=Lock, repr=False)


@dataclass
class IdempotencyOutcome:
    status: AcquireStatus
    record: IdempotencyRecord


class IdempotencyStore:
    """按 (caller, key) 记录写入结果；并发重复请求最多一个生效。"""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], IdempotencyRecord] = {}
        self._guard = Lock()

    def acquire(
        self, caller_id: str, key: str, request_fingerprint: str
    ) -> IdempotencyOutcome:
        with self._guard:
            record = self._records.get((caller_id, key))
            if record is None:
                record = IdempotencyRecord(
                    key=key,
                    caller_id=caller_id,
                    request_fingerprint=request_fingerprint,
                )
                self._records[(caller_id, key)] = record
                return IdempotencyOutcome("acquired", record)

        # 已存在记录：加记录级锁，与首个请求的 complete/release 互斥
        with record.lock:
            if record.request_fingerprint != request_fingerprint:
                raise APIError(
                    ErrorCode.IDEMPOTENCY_KEY_CONFLICT,
                    "同一幂等键被用于不同的请求内容。",
                    status_code=422,
                    details={
                        "idempotency_key": key,
                        "hint": "请为不同的写入请求使用不同的 Idempotency-Key。",
                    },
                )
            if record.status == "in_progress":
                raise APIError(
                    ErrorCode.IDEMPOTENCY_IN_PROGRESS,
                    "该幂等键对应的首次请求仍在处理中，本次重复提交未重复执行。",
                    status_code=409,
                    headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
                    details={"idempotency_key": key},
                )
            return IdempotencyOutcome("replay", record)

    def complete(
        self,
        record: IdempotencyRecord,
        *,
        status_code: int,
        body: dict,
        resource_id: str | None,
    ) -> None:
        """首个请求处理完成：固化响应，后续重复请求将重放该结果。"""
        with record.lock:
            record.response_status = status_code
            record.response_body = body
            record.resource_id = resource_id
            record.status = "completed"

    def release(self, record: IdempotencyRecord) -> None:
        """首个请求在完成前失败：移除 in_progress 记录，允许用同键重试。"""
        with self._guard:
            existing = self._records.get((record.caller_id, record.key))
            if existing is record and record.status == "in_progress":
                del self._records[(record.caller_id, record.key)]

    def __len__(self) -> int:
        with self._guard:
            return len(self._records)
