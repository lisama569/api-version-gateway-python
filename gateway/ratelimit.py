"""滑动窗口配额限流：任意窗口长度内的占用合计不超过配额。

每个调用方在内存中维护一条按时间排序的占用事件日志：
- ``check``：按请求计数（沿用单笔请求语义），判定与记账在同一把锁内原子完成，
  被拒绝的请求同样记账，避免超限后用重试刷穿配额；
- ``reserve``：按"实际占用条数"预留额度，只有整笔成本都能容纳时才原子记账，
  余额不足以覆盖整批时拒绝且不留下任何占用（不会先占一部分）；
- ``refund``：业务侧原子写入失败时，按预留凭据原样退还，额度视同从未占用；
- ``snapshot``：纯观测，不计数，供响应头/日志反映最新占用情况。

配额判定基于滑动窗口：任意时刻 T，窗口 (T-window, T] 内所有事件的成本之和
不得超过上限。固定窗口在切换边界允许两个相邻窗口各打满一份（瞬时两倍流量），
滑动日志不存在该空档——窗口边缘的密集请求同样计入同一窗口。
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
    requested_cost: int = 1
    reservation_id: int | None = None
    # reserve 被拒绝时：最早重试可获得足够额度的时刻（None 表示单笔成本超过上限，
    # 任何窗口都无法容纳）
    earliest_fit_at: float | None = None


@dataclass(frozen=True)
class _Event:
    """一条占用/计数事件。"""

    seq: int
    at: float
    cost: int


class RateLimiter:
    """按调用方维护滑动窗口占用日志。"""

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        # caller -> 按时间升序的事件列表
        self._events: dict[str, list[_Event]] = {}
        self._seq = 0

    # ------------------------------------------------------------------
    # 单笔请求：按次计数（拒绝也计数，沿用既有语义）
    # ------------------------------------------------------------------
    def check(
        self,
        caller: str,
        rule: QuotaRule,
        cost: int = 1,
        now: float | None = None,
    ) -> QuotaDecision:
        """计数并判定是否放行。拒绝时 retry_after_seconds 为距最早额度释放的秒数。"""
        if now is None:
            now = self._clock()
        with self._lock:
            events = self._active(caller, rule, now)
            event = self._append(caller, events, now, cost)
            return self._decision(caller, rule, events, now, cost, event)

    # ------------------------------------------------------------------
    # 批量请求：按实际占用条数预留，整笔容纳不下则不记任何账
    # ------------------------------------------------------------------
    def reserve(
        self,
        caller: str,
        rule: QuotaRule,
        cost: int,
        now: float | None = None,
    ) -> QuotaDecision:
        """原子预留 cost 个额度。

        成功：记账并返回 reservation_id（业务失败时须 refund）。
        失败：不记账，used 反映当前占用，earliest_fit_at 给出最早可重试时刻。
        """
        if now is None:
            now = self._clock()
        with self._lock:
            events = self._active(caller, rule, now)
            used = sum(e.cost for e in events)
            if used + cost <= rule.limit:
                event = self._append(caller, events, now, cost)
                return self._decision(caller, rule, events, now, cost, event)

            earliest_fit = self._earliest_fit(events, rule, cost, now)
            return self._decision(
                caller, rule, events, now, cost, None,
                allowed=False, earliest_fit_at=earliest_fit,
            )

    def refund(self, caller: str, reservation_id: int) -> None:
        """按预留凭据退还额度（事件可能已随窗口过期被清理，此时为 no-op）。"""
        with self._lock:
            events = self._events.get(caller)
            if not events:
                return
            self._events[caller] = [e for e in events if e.seq != reservation_id]

    def snapshot(
        self,
        caller: str,
        rule: QuotaRule,
        now: float | None = None,
    ) -> QuotaDecision:
        """纯观测当前占用，不计数、不预留。"""
        if now is None:
            now = self._clock()
        with self._lock:
            events = self._active(caller, rule, now)
            return self._decision(caller, rule, events, now, 0, None)

    def current_usage(self, caller: str) -> int | None:
        """返回调用方当前全部（未按窗口过滤）记账成本，主要用于可观测/测试。"""
        with self._lock:
            events = self._events.get(caller)
            return sum(e.cost for e in events) if events is not None else None

    def reset(self, caller: str | None = None) -> None:
        with self._lock:
            if caller is None:
                self._events.clear()
            else:
                self._events.pop(caller, None)

    # ------------------------------------------------------------------
    # 内部实现（调用方须已持有 self._lock）
    # ------------------------------------------------------------------
    def _active(self, caller: str, rule: QuotaRule, now: float) -> list[_Event]:
        """取出并落 caller 的窗口内事件（左开右闭：at <= now-window 即过期）。"""
        alive = [
            e for e in self._events.get(caller, ())
            if now - e.at < rule.window_seconds
        ]
        self._events[caller] = alive
        return alive

    def _append(
        self, caller: str, events: list[_Event], now: float, cost: int
    ) -> _Event:
        self._seq += 1
        event = _Event(seq=self._seq, at=now, cost=cost)
        events.append(event)
        self._events[caller] = events
        return event

    def _decision(
        self,
        caller: str,
        rule: QuotaRule,
        events: list[_Event],
        now: float,
        cost: int,
        event: _Event | None,
        *,
        allowed: bool | None = None,
        earliest_fit_at: float | None = None,
    ) -> QuotaDecision:
        used = sum(e.cost for e in events)
        if allowed is None:
            allowed = event is not None and used <= rule.limit
        remaining = max(0, rule.limit - used)
        if events:
            window_started_at = events[0].at
            reset_at = events[0].at + rule.window_seconds
        else:
            # 没有占用：窗口观测起点取当前，重置时刻为一个完整窗口之后
            window_started_at = now
            reset_at = now + rule.window_seconds
        if allowed:
            retry_after = 0.0
        elif earliest_fit_at is not None:
            retry_after = max(0.0, math.ceil(earliest_fit_at - now))
        else:
            retry_after = max(0.0, math.ceil(reset_at - now))
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
            requested_cost=cost,
            reservation_id=event.seq if event is not None else None,
            earliest_fit_at=earliest_fit_at,
        )

    def _earliest_fit(
        self,
        events: list[_Event],
        rule: QuotaRule,
        cost: int,
        now: float,
    ) -> float | None:
        """求最早时刻 T（T > now）使窗口 (T-window, T] 内占用 + cost <= limit。

        只有事件逐个过期才会释放额度，故只需按过期顺序（即事件时间升序）
        检查每个候选时刻 at_i + window。所有事件过期后仍装不下则返回 None。
        """
        total = sum(e.cost for e in events)
        for e in events:
            total -= e.cost  # 该事件在 e.at + window 时刻过期
            if total + cost <= rule.limit:
                return e.at + rule.window_seconds
        return None
