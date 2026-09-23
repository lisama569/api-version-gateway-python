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
    GatewayError,
    IdempotencyKeyMissingError,
    QuotaExceededError,
)
from gateway.idempotency import (
    ACQUIRED,
    COMPLETED,
    INFLIGHT,
    IdempotencyStore,
    fingerprint_payload,
)
from gateway.ratelimit import RateLimiter
from gateway.resources import OrderService
from gateway.versioning import API_VERSION_HEADER, negotiate_version

logger = logging.getLogger("api_gateway")

CALLER_HEADER = "X-Caller-Id"
IDEMPOTENCY_HEADER = "Idempotency-Key"
ANONYMOUS_CALLER = "anonymous"
IDEMPOTENCY_WAIT_SECONDS = 10.0

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

        outcome = state.idempotency.acquire(key, fingerprint)
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
            )
            entry = state.idempotency.wait_for_completion(
                key, timeout=IDEMPOTENCY_WAIT_SECONDS
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

        return _replay_idempotent_result(key, outcome, state)

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
        state.idempotency.fail(key, exc.error_code, exc.status_code)
        log_event(
            "idempotency_decision",
            idempotency_key=key,
            outcome="failed",
            error_code=exc.error_code,
        )
        raise
    except Exception as exc:  # pragma: no cover - 防御性记录
        state.idempotency.fail(key, "internal_error", 500)
        log_event(
            "idempotency_decision",
            idempotency_key=key,
            outcome="failed",
            error_code="internal_error",
        )
        raise

    body = {"order": order.to_dict(), "api_version": version}
    state.idempotency.complete(key, body, 201)
    log_event(
        "idempotency_decision",
        idempotency_key=key,
        outcome="completed",
        resource_id=order.id,
        side_effects=state.orders.side_effects,
    )
    return JSONResponse(
        status_code=201,
        content=body,
        headers={"Idempotency-Key": key, "Idempotent-Replay": "false"},
    )


def _replay_idempotent_result(
    key: str, outcome: str, state: GatewayState
) -> JSONResponse:
    """重复提交路径：回放首个请求的结论，不产生任何副作用。"""
    entry = state.idempotency.get_result(key)
    log_event(
        "idempotency_decision",
        idempotency_key=key,
        outcome="replay",
        stored_outcome=outcome,
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


app = create_app()
