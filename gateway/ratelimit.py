"""固定时间窗口配额限流：并发计数不漏计，窗口切换不留空档。

每个调用方在内存中维护一个 (window_start, count) 计数桶：
- 判定与自增在同一把锁内原子完成，并发请求不会漏计；
- 桶的有效期为 [window_start, window_start + window_seconds)，
  落在新窗口的第一个请求负责原子地滚动窗口，不存在“新旧窗口都放行”的空档；
- 被拒绝的请求同样计入本窗口，避免超限后用重试刷穿配额。
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from typing import Callable

from gateway.config import QuotaRule


@dataclass(frozen=True)
class QuotaDecision:
    """配额判定结论。"""

    caller: str
    allowed: bool
    limit: int
    used: int
    remaining: int
    window_seconds: float
    window_started_at: float
    reset_at: float
    retry_after_seconds: float


class RateLimiter:
    """按调用方维护固定窗口计数。"""

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        # caller -> (窗口起点, 已计次数)
        self._counters: dict[str, tuple[float, int]] = {}

    def check(
        self,
        caller: str,
        rule: QuotaRule,
        cost: int = 1,
        now: float | None = None,
    ) -> QuotaDecision:
        """计数并判定是否放行。拒绝时 retry_after_seconds 为距窗口重置的秒数。"""
        if now is None:
            now = self._clock()
        with self._lock:
            started, count = self._counters.get(caller, (now, 0))
            # 落入新窗口：原子滚动，旧计数立即失效且新窗口从 0 开始
            if now - started >= rule.window_seconds:
                started, count = now, 0
            count += cost
            self._counters[caller] = (started, count)

        reset_at = started + rule.window_seconds
        allowed = count <= rule.limit
        remaining = max(0, rule.limit - count)
        retry_after = 0.0 if allowed else max(0.0, math.ceil(reset_at - now))
        return QuotaDecision(
            caller=caller,
            allowed=allowed,
            limit=rule.limit,
            used=count,
            remaining=remaining,
            window_seconds=rule.window_seconds,
            window_started_at=started,
            reset_at=reset_at,
            retry_after_seconds=retry_after,
        )

    def current_usage(self, caller: str) -> int | None:
        """返回调用方当前窗口已计次数（主要用于可观测/测试）。"""
        with self._lock:
            bucket = self._counters.get(caller)
            return bucket[1] if bucket is not None else None

    def reset(self, caller: str | None = None) -> None:
        with self._lock:
            if caller is None:
                self._counters.clear()
            else:
                self._counters.pop(caller, None)
