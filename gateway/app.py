"""FastAPI 应用装配：版本协商 → 配额限流 → 幂等写入 三段式管道。

所有组件均为进程内实现，不依赖外部服务或真实凭据。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from gateway.config import Settings, VersionStatus, default_settings
from gateway.errors import (
    BatchValidationError,
    GatewayError,
    IdempotencyKeyMissingError,
    QuotaExceededError,
)
from gateway.idempotency import (
    ACQUIRED,
    COMPLETED,
    FAILED,
    INFLIGHT,
    IdempotencyStore,
    fingerprint_payload,
)
from gateway.ratelimit import QuotaDecision, RateLimiter
from gateway.resources import OrderService
from gateway.versioning import API_VERSION_HEADER, negotiate_version

logger = logging.getLogger("api_gateway")

CALLER_HEADER = "X-Caller-Id"
IDEMPOTENCY_HEADER = "Idempotency-Key"
ANONYMOUS_CALLER = "anonymous"
IDEMPOTENCY_WAIT_SECONDS = 10.0
BATCH_ORDERS_PATH = "/orders/batch"

# 不需要版本声明/配额的公共端点
_EXEMPT_PATHS = {
    "/",
    "/health",
    "/versions",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/favicon.ico",
}


class CreateOrderBody(BaseModel):
    name: str
    quantity: int = 1
    amount: int = 0


class BatchCreateOrdersBody(BaseModel):
    """批量下单请求体：orders 至少 1 条，每条与单笔下单字段一致。"""

    orders: list[CreateOrderBody]


def resolve_quota_rule(settings: Settings, caller: str):
    """按调用方解析配额规则（具名配置 > 匿名规则 > 默认规则）。"""
    return (
        settings.quotas.get(caller)
        or (
            settings.anonymous_quota
            if caller == ANONYMOUS_CALLER
            else settings.default_quota
        )
    )


@dataclass
class GatewayState:
    settings: Settings
    idempotency: IdempotencyStore
    limiter: RateLimiter
    orders: OrderService


def configure_logging(level: int = logging.INFO) -> None:
    """配置单行结构化日志（key=value，便于检索版本/幂等/配额判定依据）。"""
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s event=%(event)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    )
    root = logging.getLogger("api_gateway")
    root.handlers[:] = [handler]
    root.setLevel(level)
    root.propagate = True


def log_event(event: str, **fields: Any) -> None:
    """打印一条结构化事件日志。"""
    parts = [f"{k}={_render(v)}" for k, v in fields.items() if v is not None]
    logger.info(" ".join(parts), extra={"event": event})


def _render(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.3f}"
    text = str(value).replace(" ", "_")
    return text


def create_app(
    settings: Settings | None = None,
    *,
    clock: Callable[[], float] | None = None,
) -> FastAPI:
    """创建网关应用。clock 可注入（测试用虚拟时钟），默认单调时钟。"""
    configure_logging()
    settings = settings or default_settings()
    clock = clock or time.monotonic

    state = GatewayState(
        settings=settings,
        idempotency=IdempotencyStore(settings.idempotency_ttl_seconds, clock=clock),
        limiter=RateLimiter(clock=clock),
        orders=OrderService(),
    )

    app = FastAPI(title="API Version Gateway", version="0.1.0")
    app.state.gateway = state
    _register_error_handlers(app)
    _register_middleware(app, state, clock)
    _register_routes(app, state)
    return app


def _error_payload(code: str, message: str, details: dict[str, Any]) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details}}


def _register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(GatewayError)
    async def _on_gateway_error(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=_error_payload(exc.error_code, exc.message, exc.details),
        )

    @app.exception_handler(RequestValidationError)
    async def _on_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=_error_payload(
                "invalid_request", "请求体或参数不合法。", {"errors": exc.errors()}
            ),
        )


def _register_middleware(app: FastAPI, state: GatewayState, clock: Callable[[], float]) -> None:
    @app.middleware("http")
    async def gateway_pipeline(request: Request, call_next: Any) -> JSONResponse | Any:
        path = request.url.path
        if path in _EXEMPT_PATHS:
            return await call_next(request)

        # 1) 版本协商：未声明 / 未知 / 已下线在此被拒绝，原因互不相同
        requested = request.headers.get(API_VERSION_HEADER)
        try:
            decision = negotiate_version(requested, state.settings)
        except GatewayError as exc:
            log_event(
                "version_decision",
                result="rejected",
                reason=exc.error_code,
                requested=requested,
                path=path,
            )
            return JSONResponse(
                status_code=exc.status_code,
                content=_error_payload(exc.error_code, exc.message, exc.details),
            )

        info = decision.version
        log_event(
            "version_decision",
            result=info.status.value,
            requested=info.version,
            path=path,
            sunset_at=info.sunset_at,
            successor=info.successor,
        )

        # 2) 调用方识别 + 配额限流
        caller = request.headers.get(CALLER_HEADER) or ANONYMOUS_CALLER
        rule = resolve_quota_rule(state.settings, caller)
        is_batch = path == BATCH_ORDERS_PATH
        if is_batch:
            # 批量下单的配额按"实际占用条数"在路由内 reserve，中间件不按请求次数记账
            request.state.api_version = info.version
            request.state.caller = caller
            request.state.quota_decision = None
            response = await call_next(request)
            quota: QuotaDecision | None = getattr(
                request.state, "quota_decision", None
            )
            if quota is None:
                # 路由在计费前结束（如 400 校验失败）：用纯观测快照填充限流头
                quota = state.limiter.snapshot(caller, rule, now=clock())
        else:
            # 单笔/读取路径：每次请求都计数，包括重放与被拒请求
            quota = state.limiter.check(caller, rule, now=clock())
            log_event(
                "quota_decision",
                caller=caller,
                allowed=str(quota.allowed).lower(),
                used=quota.used,
                limit=quota.limit,
                window_seconds=quota.window_seconds,
                reset_in_seconds=quota.retry_after_seconds,
            )
            if not quota.allowed:
                exc = QuotaExceededError(
                    f"调用方 {caller} 已超出 {quota.window_seconds:g}s 窗口配额"
                    f"（{quota.limit} 次），请在窗口重置后重试。",
                    details={
                        "caller": caller,
                        "limit": quota.limit,
                        "window_seconds": quota.window_seconds,
                        "retry_after_seconds": quota.retry_after_seconds,
                    },
                )
                response = JSONResponse(
                    status_code=exc.status_code,
                    content=_error_payload(exc.error_code, exc.message, exc.details),
                    headers={"Retry-After": str(int(quota.retry_after_seconds))},
                )
            else:
                request.state.api_version = info.version
                request.state.caller = caller
                response = await call_next(request)

        # 限流元信息 + 弃用提示（无论业务成功与否都附加，保证提示可观测）
        response.headers["X-RateLimit-Limit"] = str(quota.limit)
        response.headers["X-RateLimit-Remaining"] = str(quota.remaining)
        response.headers["X-RateLimit-Reset"] = str(
            max(0, int(quota.reset_at - clock() + 0.9999))
        )
        response.headers[API_VERSION_HEADER] = info.version
        if info.status is VersionStatus.DEPRECATED:
            response.headers["Deprecation"] = "true"
            response.headers["X-API-Deprecated"] = "true"
            if info.sunset_at:
                response.headers["Sunset"] = info.sunset_at
            if info.successor:
                response.headers["X-API-Successor"] = info.successor
            warning = (
                f'299 - "API version {info.version} is deprecated and will be '
                f'retired on {info.sunset_at}; please migrate to {info.successor}."'
            )
            response.headers["Warning"] = warning
        return response


def _register_routes(app: FastAPI, state: GatewayState) -> None:
    @app.get("/")
    async def root() -> dict[str, str]:
        return {"service": "api-version-gateway", "docs": "/docs", "versions": "/versions"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/versions")
    async def list_versions() -> dict[str, Any]:
        """版本目录：公开端点，无需声明版本。"""
        return {
            "recommended": next(
                (v.version for v in state.settings.versions.values()
                 if v.status is VersionStatus.SUPPORTED),
                None,
            ),
            "versions": [
                {
                    "version": v.version,
                    "status": v.status.value,
                    "sunset_at": v.sunset_at,
                    "successor": v.successor,
                    "notice": v.notice,
                }
                for v in state.settings.versions.values()
            ],
        }

    @app.post("/orders", status_code=201)
    async def create_order(request: Request) -> JSONResponse:
        caller: str = request.state.caller
        version: str = request.state.api_version

        key = request.headers.get(IDEMPOTENCY_HEADER)
        if not key or not key.strip():
            raise IdempotencyKeyMissingError(
                "写请求必须携带 Idempotency-Key 头以保证幂等。",
                details={"header": IDEMPOTENCY_HEADER},
            )
        key = key.strip()

        try:
            raw_body = await request.json()
            body = CreateOrderBody.model_validate(raw_body)
        except ValidationError as exc:
            raise RequestValidationError(exc.errors()) from exc
        except ValueError as exc:
            raise RequestValidationError(
                [{"type": "json_invalid", "loc": ["body"], "msg": "请求体必须是合法 JSON。"}]
            ) from exc
        payload = body.model_dump()
        fingerprint = fingerprint_payload(payload)

        outcome = state.idempotency.acquire(key, fingerprint, namespace=caller)
        log_event(
            "idempotency_decision",
            idempotency_key=key,
            fingerprint=fingerprint[:12],
            outcome=outcome,
            caller=caller,
        )

        if outcome == ACQUIRED:
            return _execute_with_idempotency(state, key, caller, version, payload)

        if outcome == INFLIGHT:
            log_event(
                "idempotency_decision",
                idempotency_key=key,
                outcome="waiting",
                timeout_seconds=IDEMPOTENCY_WAIT_SECONDS,
                caller=caller,
            )
            entry = state.idempotency.wait_for_completion(
                key, timeout=IDEMPOTENCY_WAIT_SECONDS, namespace=caller
            )
            if entry is None or entry.status == INFLIGHT:
                return JSONResponse(
                    status_code=409,
                    content=_error_payload(
                        "idempotency_inflight",
                        f"幂等键 {key} 的首个请求仍在处理中，本次未产生副作用；"
                        "请稍后携带同一幂等键重试以获取最终结论。",
                        {"idempotency_key": key},
                    ),
                )
            outcome = entry.status

        return _replay_idempotent_result(key, outcome, state, caller)

    @app.post(BATCH_ORDERS_PATH, status_code=201)
    async def create_orders_batch(request: Request) -> JSONResponse:
        return await _handle_batch_create(request, state)

    @app.get("/orders/{order_id}")
    async def read_order(order_id: str, request: Request) -> dict[str, Any]:
        caller: str = request.state.caller
        order = state.orders.get_order(order_id)
        log_event(
            "resource_read",
            resource="order",
            resource_id=order_id,
            caller=caller,
            version=request.state.api_version,
        )
        return {"order": order.to_dict(), "api_version": request.state.api_version}


def _execute_with_idempotency(
    state: GatewayState,
    key: str,
    caller: str,
    version: str,
    payload: dict[str, Any],
) -> JSONResponse:
    """幂等占位赢家路径：执行唯一一次真实副作用并记录结论。"""
    try:
        order = state.orders.create_order(caller, payload)
    except GatewayError as exc:
        state.idempotency.fail(key, exc.error_code, exc.status_code, namespace=caller)
        log_event(
            "idempotency_decision",
            idempotency_key=key,
            outcome="failed",
            error_code=exc.error_code,
            caller=caller,
        )
        raise
    except Exception as exc:  # pragma: no cover - 防御性记录
        state.idempotency.fail(key, "internal_error", 500, namespace=caller)
        log_event(
            "idempotency_decision",
            idempotency_key=key,
            outcome="failed",
            error_code="internal_error",
            caller=caller,
        )
        raise

    body = {"order": order.to_dict(), "api_version": version}
    state.idempotency.complete(key, body, 201, namespace=caller)
    log_event(
        "idempotency_decision",
        idempotency_key=key,
        outcome="completed",
        resource_id=order.id,
        side_effects=state.orders.side_effects,
        caller=caller,
    )
    return JSONResponse(
        status_code=201,
        content=body,
        headers={"Idempotency-Key": key, "Idempotent-Replay": "false"},
    )


def _replay_idempotent_result(
    key: str, outcome: str, state: GatewayState, caller: str
) -> JSONResponse:
    """重复提交路径：回放首个请求的结论，不产生任何副作用。"""
    entry = state.idempotency.get_result(key, namespace=caller)
    log_event(
        "idempotency_decision",
        idempotency_key=key,
        outcome="replay",
        stored_outcome=outcome,
        caller=caller,
    )
    if outcome == COMPLETED and entry is not None:
        return JSONResponse(
            status_code=entry.status_code,
            content=entry.response,
            headers={"Idempotency-Key": key, "Idempotent-Replay": "true"},
        )
    # FAILED：回放失败结论，明确说明未产生副作用
    error_code = entry.error_code if entry is not None else "unknown"
    status_code = entry.status_code if entry is not None else 409
    return JSONResponse(
        status_code=status_code,
        content=_error_payload(
            f"idempotent_replay_{error_code}",
            f"幂等键 {key} 的首次请求此前已失败（{error_code}），未产生任何副作用；"
            "如需重新提交请更换新的幂等键。",
            {"idempotency_key": key, "original_error_code": error_code},
        ),
        headers={"Idempotency-Key": key, "Idempotent-Replay": "true"},
    )


def _replay_batch_result(
    key: str,
    outcome: str,
    state: GatewayState,
    caller: str,
) -> JSONResponse:
    """批量请求的重复提交路径：回放首批结论，不产生任何副作用与额度占用。"""
    entry = state.idempotency.get_result(key, namespace=caller)
    log_event(
        "batch_idempotency_decision",
        idempotency_key=key,
        outcome="replay",
        stored_outcome=outcome,
        caller=caller,
    )
    if outcome == COMPLETED and entry is not None:
        return JSONResponse(
            status_code=entry.status_code,
            content=entry.response,
            headers={"Idempotency-Key": key, "Idempotent-Replay": "true"},
        )
    error_code = entry.error_code if entry is not None else "unknown"
    status_code = entry.status_code if entry is not None else 409
    return JSONResponse(
        status_code=status_code,
        content=_error_payload(
            f"idempotent_replay_{error_code}",
            f"幂等键 {key} 的首批请求此前已失败（{error_code}），未产生任何副作用；"
            "如需重新提交请更换新的幂等键。",
            {"idempotency_key": key, "original_error_code": error_code},
        ),
        headers={"Idempotency-Key": key, "Idempotent-Replay": "true"},
    )


def _quota_exceeded_response(
    caller: str, decision: QuotaDecision, key: str
) -> JSONResponse:
    """构造批量配额不足响应：说清需要多少、还剩多少、最早何时可重试。"""
    required = decision.requested_cost
    remaining = decision.remaining
    earliest = decision.earliest_fit_at
    headers = {"Idempotency-Key": key, "Idempotent-Replay": "false"}
    if earliest is None:
        # 单笔成本超过窗口上限：任何窗口都装不下，重试无意义，不给 Retry-After
        retry_after_seconds: float | None = None
        message = (
            f"本批需要 {required} 条额度，超过调用方 {caller} 的窗口配额上限"
            f"（{decision.limit} 条/{decision.window_seconds:g}s），"
            "任何时刻都无法整批受理，请拆分后提交。"
        )
    else:
        retry_after_seconds = decision.retry_after_seconds
        message = (
            f"本批需要 {required} 条额度，当前窗口剩余 {remaining} 条，"
            f"不足以覆盖整批；预计 {int(retry_after_seconds)} 秒后有足够额度，"
            "请整批重试。"
        )
        if retry_after_seconds > 0:
            headers["Retry-After"] = str(int(retry_after_seconds))
    details = {
        "caller": caller,
        "required": required,
        "remaining": remaining,
        "limit": decision.limit,
        "used": decision.used,
        "window_seconds": decision.window_seconds,
        "retry_after_seconds": retry_after_seconds,
    }
    log_event(
        "batch_quota_decision",
        caller=caller,
        allowed="false",
        required=required,
        remaining=remaining,
        used=decision.used,
        limit=decision.limit,
        retry_after_seconds=retry_after_seconds,
    )
    return JSONResponse(
        status_code=429,
        content=_error_payload("quota_exceeded", message, details),
        headers=headers,
    )


async def _handle_batch_create(request: Request, state: GatewayState) -> JSONResponse:
    """批量下单：整批原子 + 幂等回放 + 按条数预留配额。"""
    caller: str = request.state.caller
    version: str = request.state.api_version

    key = request.headers.get(IDEMPOTENCY_HEADER)
    if not key or not key.strip():
        raise IdempotencyKeyMissingError(
            "批量写请求必须携带 Idempotency-Key 头以保证幂等。",
            details={"header": IDEMPOTENCY_HEADER},
        )
    key = key.strip()

    try:
        raw_body = await request.json()
        body = BatchCreateOrdersBody.model_validate(raw_body)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc
    except ValueError as exc:
        raise RequestValidationError(
            [{"type": "json_invalid", "loc": ["body"], "msg": "请求体必须是合法 JSON。"}]
        ) from exc

    items = [item.model_dump() for item in body.orders]
    if not items:
        raise BatchValidationError(
            "批量下单至少需要 1 条订单。",
            details={"index": None, "reason": "empty_batch"},
        )
    # 业务校验：逐张检查，整批拒绝并指出是哪一条
    for index, item in enumerate(items):
        if not str(item.get("name", "")).strip():
            raise BatchValidationError(
                f"第 {index} 条订单的 name 不能为空。",
                details={"index": index, "field": "name", "reason": "empty_name"},
            )
        if int(item.get("quantity", 1)) <= 0:
            raise BatchValidationError(
                f"第 {index} 条订单的 quantity 必须为正整数。",
                details={"index": index, "field": "quantity",
                         "reason": "non_positive", "value": item.get("quantity")},
            )

    cost = len(items)
    rule = resolve_quota_rule(state.settings, caller)
    payload_fingerprint_input = {"orders": items}
    fingerprint = fingerprint_payload(payload_fingerprint_input)

    # 幂等占位循环：占位赢家在产生副作用前若放弃（配额不足），会 release，
    # 等待者被唤醒后重新占位，由后续请求再试一次。
    outcome = state.idempotency.acquire(key, fingerprint, namespace=caller)
    log_event(
        "batch_idempotency_decision",
        idempotency_key=key,
        fingerprint=fingerprint[:12],
        outcome=outcome,
        caller=caller,
        batch_size=cost,
    )

    while True:
        if outcome == COMPLETED or outcome == FAILED:
            return _replay_batch_result(key, outcome, state, caller)

        if outcome == INFLIGHT:
            entry = state.idempotency.wait_for_completion(
                key, timeout=IDEMPOTENCY_WAIT_SECONDS, namespace=caller
            )
            if entry is None:
                # 占位被前一个请求释放（它未产生副作用）：重新占位再试
                outcome = state.idempotency.acquire(key, fingerprint, namespace=caller)
                log_event(
                    "batch_idempotency_decision",
                    idempotency_key=key,
                    outcome="reacquired_after_release",
                    caller=caller,
                )
                continue
            if entry.status == INFLIGHT:
                return JSONResponse(
                    status_code=409,
                    content=_error_payload(
                        "idempotency_inflight",
                        f"幂等键 {key} 的首批请求仍在处理中，本次未产生副作用；"
                        "请稍后携带同一幂等键重试以获取最终结论。",
                        {"idempotency_key": key},
                    ),
                )
            return _replay_batch_result(key, entry.status, state, caller)

        # outcome == ACQUIRED：本请求是占位赢家
        break

    # 1) 先按整批条数原子预留额度：容纳不下则整批拒绝，不写任何一条
    decision = state.limiter.reserve(caller, rule, cost=cost)
    request.state.quota_decision = decision
    log_event(
        "batch_quota_decision",
        caller=caller,
        allowed=str(decision.allowed).lower(),
        required=cost,
        used=decision.used,
        remaining=decision.remaining,
        limit=decision.limit,
        window_seconds=decision.window_seconds,
        retry_after_seconds=decision.retry_after_seconds,
    )
    if not decision.allowed:
        # 未产生任何副作用：释放占位，让后续携同键请求可以重新尝试
        state.idempotency.release(key, namespace=caller)
        return _quota_exceeded_response(caller, decision, key)

    reservation_id = decision.reservation_id
    try:
        # 2) 原子写入：全部成功提交，或内部失败时一条都不留
        orders = state.orders.create_orders_batch(caller, items)
    except GatewayError as exc:
        state.limiter.refund(caller, reservation_id)
        state.idempotency.fail(key, exc.error_code, exc.status_code, namespace=caller)
        raise
    except Exception:
        # 防御：写入异常时退还额度并记录失败结论，保证不泄露额度/不留半成品
        state.limiter.refund(caller, reservation_id)
        state.idempotency.fail(key, "internal_error", 500, namespace=caller)
        raise

    response_body = {
        "orders": [o.to_dict() for o in orders],
        "created_count": len(orders),
        "batch_size": cost,
        "api_version": version,
    }
    state.idempotency.complete(key, response_body, 201, namespace=caller)
    request.state.quota_decision = state.limiter.snapshot(caller, rule)
    log_event(
        "batch_idempotency_decision",
        idempotency_key=key,
        outcome="completed",
        caller=caller,
        batch_size=cost,
        created_count=len(orders),
        side_effects=state.orders.side_effects,
    )
    return JSONResponse(
        status_code=201,
        content=response_body,
        headers={"Idempotency-Key": key, "Idempotent-Replay": "false"},
    )


app = create_app()
