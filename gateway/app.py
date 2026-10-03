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
from pydantic import BaseModel, Field, ValidationError

from gateway.config import QuotaRule, Settings, VersionStatus, default_settings
from gateway.errors import (
    GatewayError,
    IdempotencyKeyMissingError,
    QuotaExceededError,
)
from gateway.idempotency import (
    ACQUIRED,
    COMPLETED,
    INFLIGHT,
    IdempotencyEntry,
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
MAX_BATCH_SIZE = 100

# 批量端点：占用条数取决于请求体，配额判定下沉到路由内按条数原子执行
_DEFERRED_QUOTA_PATHS = {"/orders/batch"}

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


class BatchCreateOrderBody(BaseModel):
    """批量下单请求体：整批校验，任一条不合法则整批拒绝。"""

    orders: list[CreateOrderBody] = Field(min_length=1, max_length=MAX_BATCH_SIZE)


def scoped_idempotency_key(caller: str, key: str) -> str:
    """幂等记录按调用方隔离：不同调用方使用同一幂等键互不影响。"""
    return f"{caller}\x1f{key}"


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

        # 2) 调用方识别 + 配额限流（每次请求都计数，包括重放与被拒请求）
        caller = request.headers.get(CALLER_HEADER) or ANONYMOUS_CALLER
        rule = (
            state.settings.quotas.get(caller)
            or (
                state.settings.anonymous_quota
                if caller == ANONYMOUS_CALLER
                else state.settings.default_quota
            )
        )
        quota: QuotaDecision | None = None
        if path in _DEFERRED_QUOTA_PATHS:
            # 批量端点：额度按批内实际条数在路由内原子扣减（见 create_orders_batch），
            # 此处只做版本协商与身份识别；判定结论经 request.state 回传以统一写响应头。
            request.state.api_version = info.version
            request.state.caller = caller
            request.state.quota_rule = rule
            response = await call_next(request)
            quota = getattr(request.state, "quota_decision", None)
        else:
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
        if quota is not None:
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
        scoped_key = scoped_idempotency_key(caller, key)

        outcome = state.idempotency.acquire(scoped_key, fingerprint)
        log_event(
            "idempotency_decision",
            idempotency_key=key,
            fingerprint=fingerprint[:12],
            outcome=outcome,
            caller=caller,
        )

        if outcome == ACQUIRED:
            return _execute_with_idempotency(state, scoped_key, key, caller, version, payload)

        if outcome == INFLIGHT:
            waited = _wait_for_inflight(state, scoped_key, key)
            if isinstance(waited, JSONResponse):
                return waited
            outcome = waited.status

        return _replay_idempotent_result(scoped_key, key, outcome, state)

    @app.post("/orders/batch", status_code=201)
    async def create_orders_batch(request: Request) -> JSONResponse:
        """批量下单：整批原子生效；配额按批内条数计；同键重放复用首次结论。"""
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
            body = BatchCreateOrderBody.model_validate(raw_body)
        except ValidationError as exc:
            # errors 的 loc 含条目下标（如 ["orders", 2, "name"]），可定位到具体哪一条
            raise RequestValidationError(exc.errors()) from exc
        except ValueError as exc:
            raise RequestValidationError(
                [{"type": "json_invalid", "loc": ["body"], "msg": "请求体必须是合法 JSON。"}]
            ) from exc
        payloads = [item.model_dump() for item in body.orders]
        fingerprint = fingerprint_payload({"orders": payloads})
        scoped_key = scoped_idempotency_key(caller, key)

        outcome = state.idempotency.acquire(scoped_key, fingerprint)
        log_event(
            "idempotency_decision",
            idempotency_key=key,
            fingerprint=fingerprint[:12],
            outcome=outcome,
            caller=caller,
            batch_size=len(payloads),
        )

        if outcome == ACQUIRED:
            return _execute_batch(state, request, scoped_key, key, caller, version, payloads)

        if outcome == INFLIGHT:
            waited = _wait_for_inflight(state, scoped_key, key)
            if isinstance(waited, JSONResponse):
                return waited
            outcome = waited.status

        return _replay_idempotent_result(scoped_key, key, outcome, state)

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


def _wait_for_inflight(
    state: GatewayState, scoped_key: str, raw_key: str
) -> IdempotencyEntry | JSONResponse:
    """首个请求仍在处理时阻塞等待其结论。

    落定后返回该记录（调用方走回放路径）；超时仍未结束返回 409，
    明确说明本次未产生副作用，不会静默丢弃也不会让第二个请求生效。
    """
    log_event(
        "idempotency_decision",
        idempotency_key=raw_key,
        outcome="waiting",
        timeout_seconds=IDEMPOTENCY_WAIT_SECONDS,
    )
    entry = state.idempotency.wait_for_completion(
        scoped_key, timeout=IDEMPOTENCY_WAIT_SECONDS
    )
    if entry is None or entry.status == INFLIGHT:
        return JSONResponse(
            status_code=409,
            content=_error_payload(
                "idempotency_inflight",
                f"幂等键 {raw_key} 的首个请求仍在处理中，本次未产生副作用；"
                "请稍后携带同一幂等键重试以获取最终结论。",
                {"idempotency_key": raw_key},
            ),
        )
    return entry


def _execute_batch(
    state: GatewayState,
    request: Request,
    scoped_key: str,
    raw_key: str,
    caller: str,
    version: str,
    payloads: list[dict[str, Any]],
) -> JSONResponse:
    """批量幂等占位赢家路径：先按条数原子扣减额度，再一次原子写入整批。

    顺序保证：只有占位赢家才会扣额度（重放不重复计费）；
    额度不足时整批拒绝、一条不写，并把失败结论记入幂等记录。
    """
    cost = len(payloads)
    rule: QuotaRule = request.state.quota_rule
    quota = state.limiter.check(caller, rule, cost=cost)
    request.state.quota_decision = quota
    log_event(
        "quota_decision",
        caller=caller,
        allowed=str(quota.allowed).lower(),
        cost=cost,
        used=quota.used,
        limit=quota.limit,
        remaining=quota.remaining,
        window_seconds=quota.window_seconds,
        reset_in_seconds=quota.retry_after_seconds,
    )
    if not quota.allowed:
        state.idempotency.fail(scoped_key, QuotaExceededError.error_code, 429)
        log_event(
            "batch_decision",
            idempotency_key=raw_key,
            caller=caller,
            result="rejected",
            reason="quota_exceeded",
            needed=cost,
            remaining=quota.remaining,
            retry_after_seconds=quota.retry_after_seconds,
        )
        return JSONResponse(
            status_code=429,
            content=_error_payload(
                QuotaExceededError.error_code,
                f"调用方 {caller} 剩余额度不足以覆盖整批：本次需要 {cost} 条，"
                f"当前剩余 {quota.remaining} 条（窗口配额 {quota.limit} 条/"
                f"{quota.window_seconds:g}s），约 {quota.retry_after_seconds:.0f} 秒后可重试。",
                {
                    "caller": caller,
                    "needed": cost,
                    "remaining": quota.remaining,
                    "limit": quota.limit,
                    "window_seconds": quota.window_seconds,
                    "retry_after_seconds": quota.retry_after_seconds,
                },
            ),
            headers={"Retry-After": str(int(quota.retry_after_seconds))},
        )

    try:
        orders = state.orders.create_orders_batch(caller, payloads)
    except GatewayError as exc:
        state.idempotency.fail(scoped_key, exc.error_code, exc.status_code)
        raise
    except Exception:  # pragma: no cover - 防御性记录
        state.idempotency.fail(scoped_key, "internal_error", 500)
        raise

    body = {
        "orders": [order.to_dict() for order in orders],
        "applied": len(orders),
        "api_version": version,
    }
    state.idempotency.complete(scoped_key, body, 201)
    log_event(
        "batch_decision",
        idempotency_key=raw_key,
        caller=caller,
        result="applied",
        applied=len(orders),
        side_effects=state.orders.side_effects,
    )
    return JSONResponse(
        status_code=201,
        content=body,
        headers={"Idempotency-Key": raw_key, "Idempotent-Replay": "false"},
    )


def _execute_with_idempotency(
    state: GatewayState,
    scoped_key: str,
    raw_key: str,
    caller: str,
    version: str,
    payload: dict[str, Any],
) -> JSONResponse:
    """幂等占位赢家路径：执行唯一一次真实副作用并记录结论。"""
    try:
        order = state.orders.create_order(caller, payload)
    except GatewayError as exc:
        state.idempotency.fail(scoped_key, exc.error_code, exc.status_code)
        log_event(
            "idempotency_decision",
            idempotency_key=raw_key,
            outcome="failed",
            error_code=exc.error_code,
        )
        raise
    except Exception as exc:  # pragma: no cover - 防御性记录
        state.idempotency.fail(scoped_key, "internal_error", 500)
        log_event(
            "idempotency_decision",
            idempotency_key=raw_key,
            outcome="failed",
            error_code="internal_error",
        )
        raise

    body = {"order": order.to_dict(), "api_version": version}
    state.idempotency.complete(scoped_key, body, 201)
    log_event(
        "idempotency_decision",
        idempotency_key=raw_key,
        outcome="completed",
        resource_id=order.id,
        side_effects=state.orders.side_effects,
    )
    return JSONResponse(
        status_code=201,
        content=body,
        headers={"Idempotency-Key": raw_key, "Idempotent-Replay": "false"},
    )


def _replay_idempotent_result(
    scoped_key: str, raw_key: str, outcome: str, state: GatewayState
) -> JSONResponse:
    """重复提交路径：回放首个请求的结论，不产生任何副作用。"""
    entry = state.idempotency.get_result(scoped_key)
    log_event(
        "idempotency_decision",
        idempotency_key=raw_key,
        outcome="replay",
        stored_outcome=outcome,
    )
    if outcome == COMPLETED and entry is not None:
        return JSONResponse(
            status_code=entry.status_code,
            content=entry.response,
            headers={"Idempotency-Key": raw_key, "Idempotent-Replay": "true"},
        )
    # FAILED：回放失败结论，明确说明未产生副作用
    error_code = entry.error_code if entry is not None else "unknown"
    status_code = entry.status_code if entry is not None else 409
    return JSONResponse(
        status_code=status_code,
        content=_error_payload(
            f"idempotent_replay_{error_code}",
            f"幂等键 {raw_key} 的首次请求此前已失败（{error_code}），未产生任何副作用；"
            "如需重新提交请更换新的幂等键。",
            {"idempotency_key": raw_key, "original_error_code": error_code},
        ),
        headers={"Idempotency-Key": raw_key, "Idempotent-Replay": "true"},
    )


app = create_app()
