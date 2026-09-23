"""幂等写入：同一幂等键的并发重复请求最多一个生效。

状态机（每个幂等键一条记录）::

    (不存在) --acquire(指纹一致)--> inflight --complete--> completed
                                   inflight --fail------> failed

并发到达的重复请求：
- 第一个请求占位成功（ACQUIRED），真正执行副作用；
- 其余请求得到 INFLIGHT，阻塞等待首个请求的结论；
- 首个请求完成后，等待者与后续重放均拿到同一份结论，不再产生副作用。

同一幂等键但请求体指纹不同 → IdempotencyConflictError(409)。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from gateway.errors import IdempotencyConflictError

INFLIGHT = "inflight"
COMPLETED = "completed"
FAILED = "failed"

ACQUIRED = "acquired"


@dataclass
class IdempotencyEntry:
    """幂等键的处理记录。"""

    key: str
    fingerprint: str
    status: str = INFLIGHT
    response: dict[str, Any] | None = None
    status_code: int = 200
    error_code: str | None = None
    created_at: float = 0.0
    completed_at: float | None = None
    event: threading.Event = field(default_factory=threading.Event)


class IdempotencyStore:
    """进程内幂等记录存储，单把锁保证并发下的原子占位。"""

    def __init__(self, ttl_seconds: float, clock: Callable[[], float] | None = None) -> None:
        self.ttl_seconds = ttl_seconds
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._entries: dict[str, IdempotencyEntry] = {}

    def acquire(self, key: str, fingerprint: str) -> str:
        """尝试占位。

        返回 ACQUIRED（占位成功，调用方应执行真实操作）、INFLIGHT（首个请求
        尚在处理，调用方应等待）、COMPLETED 或 FAILED（直接回放历史结论）。
        指纹不一致时抛出 IdempotencyConflictError。
        """
        now = self._clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and self._is_expired(entry, now):
                del self._entries[key]
                entry = None
            if entry is None:
                self._entries[key] = IdempotencyEntry(
                    key=key, fingerprint=fingerprint, created_at=now
                )
                return ACQUIRED
            if entry.fingerprint != fingerprint:
                raise IdempotencyConflictError(
                    f"幂等键 {key} 已绑定到另一个请求体，不能用同一键提交不同内容。",
                    details={"idempotency_key": key},
                )
            return entry.status

    def wait_for_completion(self, key: str, timeout: float | None = None) -> IdempotencyEntry | None:
        """阻塞等待某个 inflight 请求处理完成，超时返回 None。"""
        with self._lock:
            entry = self._entries.get(key)
        if entry is None:
            return None
        entry.event.wait(timeout)
        with self._lock:
            return self._entries.get(key)

    def get_result(self, key: str) -> IdempotencyEntry | None:
        with self._lock:
            return self._entries.get(key)

    def complete(self, key: str, response: dict[str, Any], status_code: int = 200) -> None:
        """记录首个请求的成功结论并放行所有等待者。"""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            entry.status = COMPLETED
            entry.response = response
            entry.status_code = status_code
            entry.completed_at = self._clock()
            entry.event.set()

    def fail(self, key: str, error_code: str, status_code: int = 500) -> None:
        """记录首个请求的失败结论并放行所有等待者（失败结论同样可重放）。"""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            entry.status = FAILED
            entry.error_code = error_code
            entry.status_code = status_code
            entry.completed_at = self._clock()
            entry.event.set()

    def purge_expired(self, now: float | None = None) -> None:
        now = now if now is not None else self._clock()
        with self._lock:
            expired = [k for k, e in self._entries.items() if self._is_expired(e, now)]
            for k in expired:
                del self._entries[k]

    def _is_expired(self, entry: IdempotencyEntry, now: float) -> bool:
        # 只有已定论（completed/failed）的记录参与 TTL 回收；
        # inflight 记录代表同进程内仍在执行的请求，不允许被新请求顶替。
        if entry.status == INFLIGHT:
            return False
        return entry.completed_at is not None and now - entry.completed_at > self.ttl_seconds


def fingerprint_payload(payload: Any) -> str:
    """对请求体计算稳定的 SHA-256 指纹（键排序、分隔符固定）。"""
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
