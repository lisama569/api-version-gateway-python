"""滑动时间窗口配额限流：任意窗口长度内成功占用的额度都不超限。

每个调用方在内存中维护一条“成功占用记录”日志（时间戳 + 占用条数）：
- 判定与记账在同一把锁内原子完成，并发请求不会把额度用超；
- 窗口为滑动窗口：只统计落在 (now - window_seconds, now] 内的成功占用，
  因此任意一段 window_seconds 长度的时间内的占用合计都不超过配额，
  窗口边缘的密集请求也无法利用“切窗”翻倍额度（固定窗口的缺陷）；
- 只有成功占用的请求才记账：被限流拒绝的请求不消耗额度，
  超限后的重试既不会刷穿配额，也不会影响窗口内已有的占用；
- 支持按条数计费（cost>1）：批量请求按实际生效条数一次原子扣减，
  剩余额度不足以覆盖整批时整体拒绝，不会先扣一部分再报超限。
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Tuple

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
    # 本次需要的条数本身已超过窗口配额上限：无论等待多久、窗口怎么滚动都放不下，
    # 调用方必须拆成更小的批次，而不是按 Retry-After 反复重试。
    exceeds_limit: bool = False


class RateLimiter:
    """按调用方维护滑动窗口内的成功占用记录。"""

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        # caller -> 按时间升序的 (占用时间, 占用条数) 日志，仅含成功占用
        self._usage: dict[str, Deque[Tuple[float, int]]] = {}

    def check(
        self,
        caller: str,
        rule: QuotaRule,
        cost: int = 1,
        now: float | None = None,
    ) -> QuotaDecision:
        """判定并（放行时）原子记账。拒绝时 retry_after_seconds 为预计可重试的秒数。

        cost 为本次请求实际要占用的条数（批量场景为批内条数）；
        只有放行才会记账，被拒绝的请求不消耗任何额度。
        """
        if cost < 1:
            raise ValueError("cost 必须为正整数")
        if now is None:
            now = self._clock()
        with self._lock:
            entries = self._usage.setdefault(caller, deque())
            self._evict(entries, rule.window_seconds, now)
            active = sum(amount for _, amount in entries)
            exceeds_limit = cost > rule.limit
            allowed = active + cost <= rule.limit
            if allowed:
                entries.append((now, cost))
                active_after = active + cost
            else:
                active_after = active

        used = active + cost  # 含本次尝试，便于观测“超限到了什么程度”
        # remaining 始终表示当前真实剩余额度：放行时为扣减后的余量，拒绝时为现有余量
        remaining = max(0, rule.limit - active_after)
        window_started_at = entries[0][0] if entries else now
        reset_at = (entries[0][0] + rule.window_seconds) if entries else now + rule.window_seconds
        if allowed:
            retry_after = 0.0
        elif exceeds_limit:
            # 永远放不下：等待无意义，retry_after 为 0，上层应提示拆分批次
            retry_after = 0.0
        else:
            retry_after = self._retry_after(entries, rule, cost, now)
        return QuotaDecision(
            caller=caller,
            allowed=allowed,
            limit=rule.limit,
            used=used,
            remaining=remaining,
            window_seconds=rule.window_seconds,
            window_started_at=window_started_at,
            reset_at=reset_at,
            retry_after_seconds=retry_after,
            exceeds_limit=exceeds_limit,
        )

    def current_usage(self, caller: str, rule: QuotaRule | None = None) -> int | None:
        """返回调用方当前窗口内成功占用的条数（主要用于可观测/测试）。

        传入 rule 时先按窗口长度做一次过期清理，否则返回原始日志合计。
        """
        with self._lock:
            entries = self._usage.get(caller)
            if entries is None:
                return None
            if rule is not None:
                self._evict(entries, rule.window_seconds, self._clock())
            return sum(amount for _, amount in entries)

    def reset(self, caller: str | None = None) -> None:
        with self._lock:
            if caller is None:
                self._usage.clear()
            else:
                self._usage.pop(caller, None)

    @staticmethod
    def _evict(entries: Deque[Tuple[float, int]], window_seconds: float, now: float) -> None:
        """移除已滑出窗口的记录：窗口为 (now - window_seconds, now]。"""
        cutoff = now - window_seconds
        while entries and entries[0][0] <= cutoff:
            entries.popleft()

    @staticmethod
    def _retry_after(
        entries: Deque[Tuple[float, int]], rule: QuotaRule, cost: int, now: float
    ) -> float:
        """估算还需等待多少秒才有足够额度：按时间顺序累计即将过期释放的条数。

        若 cost 本身已超过配额上限（等多久都不够），返回 0.0 交由上层提示拆分。
        """
        if cost > rule.limit:
            return 0.0
        active = sum(amount for _, amount in entries)
        need_to_free = active + cost - rule.limit
        freed = 0
        for ts, amount in entries:
            freed += amount
            if freed >= need_to_free:
                return max(0.0, math.ceil(ts + rule.window_seconds - now))
        return 0.0  # pragma: no cover - 防御：理论上有 need_to_free 必能凑齐
