"""幂等写入：同一（调用方，幂等键）的并发重复请求最多一个生效。

幂等记录以 ``namespace``（调用方）+ key 作为复合标识：两个不同调用方即使
碰巧使用了同一个幂等键，也各自维护完全独立的状态机与结论，互不串用。
不传 namespace 的调用等价于单一默认命名空间（向后兼容）。

状态机（每条记录）::

    (不存在) --acquire(指纹一致)--> inflight --complete--> completed
                                   inflight --fail------> failed
                                   inflight --release---> (不存在)

并发到达的重复请求：
- 第一个请求占位成功（ACQUIRED），真正执行副作用；
- 其余请求得到 INFLIGHT，阻塞等待首个请求的结论；
- 首个请求完成后，等待者与后续重放均拿到同一份结论，不再产生副作用；
- 首个请求在产生副作用前主动放弃（如配额不足整批拒绝）时 release，
  等待者被唤醒后可重新占位，避免键被一个未生效的请求永久占住。

同一命名空间下同一键但请求体指纹不同 → IdempotencyConflictError(409)。
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

DEFAULT_NAMESPACE = "default"


@dataclass
class IdempotencyEntry:
    """幂等键的处理记录。"""

    key: str
    fingerprint: str
    namespace: str = DEFAULT_NAMESPACE
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
        # (namespace, key) -> 记录
        self._entries: dict[tuple[str, str], IdempotencyEntry] = {}

    @staticmethod
    def _scoped(namespace: str | None, key: str) -> tuple[str, str]:
        return (namespace or DEFAULT_NAMESPACE, key)

    def acquire(
        self,
        key: str,
        fingerprint: str,
        namespace: str | None = None,
    ) -> str:
        """尝试占位。

        返回 ACQUIRED（占位成功，调用方应执行真实操作）、INFLIGHT（首个请求
        尚在处理，调用方应等待）、COMPLETED 或 FAILED（直接回放历史结论）。
        指纹不一致时抛出 IdempotencyConflictError。占位按 namespace 隔离。
        """
        now = self._clock()
        scoped = self._scoped(namespace, key)
        with self._lock:
            entry = self._entries.get(scoped)
            if entry is not None and self._is_expired(entry, now):
                del self._entries[scoped]
                entry = None
            if entry is None:
                self._entries[scoped] = IdempotencyEntry(
                    key=key,
                    fingerprint=fingerprint,
                    namespace=scoped[0],
                    created_at=now,
                )
                return ACQUIRED
            if entry.fingerprint != fingerprint:
                raise IdempotencyConflictError(
                    f"幂等键 {key} 已绑定到另一个请求体，不能用同一键提交不同内容。",
                    details={"idempotency_key": key},
                )
            return entry.status

    def wait_for_completion(
        self,
        key: str,
        timeout: float | None = None,
        namespace: str | None = None,
    ) -> IdempotencyEntry | None:
        """阻塞等待某个 inflight 请求处理完成，超时返回 None；占位被释放也返回 None。"""
        scoped = self._scoped(namespace, key)
        with self._lock:
            entry = self._entries.get(scoped)
        if entry is None:
            return None
        entry.event.wait(timeout)
        with self._lock:
            return self._entries.get(scoped)

    def get_result(
        self, key: str, namespace: str | None = None
    ) -> IdempotencyEntry | None:
        with self._lock:
            return self._entries.get(self._scoped(namespace, key))

    def complete(
        self,
        key: str,
        response: dict[str, Any],
        status_code: int = 200,
        namespace: str | None = None,
    ) -> None:
        """记录首个请求的成功结论并放行所有等待者。"""
        with self._lock:
            entry = self._entries.get(self._scoped(namespace, key))
            if entry is None:
                return
            entry.status = COMPLETED
            entry.response = response
            entry.status_code = status_code
            entry.completed_at = self._clock()
            entry.event.set()

    def fail(
        self,
        key: str,
        error_code: str,
        status_code: int = 500,
        namespace: str | None = None,
    ) -> None:
        """记录首个请求的失败结论并放行所有等待者（失败结论同样可重放）。"""
        with self._lock:
            entry = self._entries.get(self._scoped(namespace, key))
            if entry is None:
                return
            entry.status = FAILED
            entry.error_code = error_code
            entry.status_code = status_code
            entry.completed_at = self._clock()
            entry.event.set()

    def release(self, key: str, namespace: str | None = None) -> None:
        """放弃一个 inflight 占位（尚未产生任何副作用）并唤醒等待者。

        等待者唤醒后查不到记录，可自行重新占位。已定论的记录不受影响。
        """
        scoped = self._scoped(namespace, key)
        with self._lock:
            entry = self._entries.get(scoped)
            if entry is None or entry.status != INFLIGHT:
                return
            del self._entries[scoped]
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
