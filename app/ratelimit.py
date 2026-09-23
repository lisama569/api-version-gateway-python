"""按调用方与时间窗口统计的配额限流。

采用滑动窗口日志（sliding-window log）：
- 所有计数判断与占用在同一把锁内完成，并发请求不会漏计/低估；
- 窗口随时间连续滑动，不存在固定窗口边界处“配额瞬间重置、可被反复利用”的空档；
- 超限时返回 429，并给出 Retry-After（最早一条记录何时滑出窗口）及配额说明头。
"""
from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    remaining_window: float  # 当前窗口内最早一条记录何时滑出（秒）
    retry_after: float  # 被限流时建议等待秒数；放行时为 0.0


class RateLimiter:
    def __init__(
        self,
        default_limit: int,
        window_seconds: float,
        overrides: dict[str, int] | None = None,
        *,
        clock: callable | None = None,
    ) -> None:
        self.default_limit = default_limit
        self.window_seconds = window_seconds
        self.overrides = dict(overrides or {})
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()
        self._clock = clock

    def _now(self) -> float:
        return self._clock() if self._clock is not None else __import__("time").monotonic()

    def limit_for(self, caller_id: str) -> int:
        return self.overrides.get(caller_id, self.default_limit)

    def check_and_consume(self, caller_id: str, now: float | None = None) -> RateLimitDecision:
        if now is None:
            now = self._now()
        boundary = now - self.window_seconds
        limit = self.limit_for(caller_id)

        with self._lock:
            log = self._hits[caller_id]
            # 1) 裁剪掉窗口外的旧记录
            while log and log[0] <= boundary:
                log.popleft()

            used = len(log)
            if used >= limit:
                # 最早一条记录滑出窗口的时刻，即最早可重试时刻
                earliest = log[0]
                retry_after = max(0.0, (earliest + self.window_seconds) - now)
                return RateLimitDecision(
                    allowed=False,
                    limit=limit,
                    remaining=0,
                    remaining_window=max(0.0, earliest + self.window_seconds - now),
                    retry_after=retry_after,
                )

            # 2) 判定与占用在同一临界区内：并发请求无法同时“看到剩余名额”
            log.append(now)
            return RateLimitDecision(
                allowed=True,
                limit=limit,
                remaining=limit - used - 1,
                remaining_window=self.window_seconds,
                retry_after=0.0,
            )

    def reset(self, caller_id: str | None = None) -> None:
        with self._lock:
            if caller_id is None:
                self._hits.clear()
            else:
                self._hits.pop(caller_id, None)
