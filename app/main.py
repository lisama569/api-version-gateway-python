"""FastAPI 装配：版本协商 -> 配额限流 -> 幂等写入。

每个写入请求的判定链：
1. 版本协商（X-API-Version）：未声明/未知/已下线原因可区分；已弃用给出 Sunset；
2. 配额限流（按 X-Caller-Id + 滑动窗口）：超限返回 429 与 Retry-After；
3. 幂等写入（Idempotency-Key）：并发同键最多一个生效，其余可解释返回。
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from app import applog
from app.errors import APIError, ErrorCode
from app.idempotency import IdempotencyStore
from app.ratelimit import RateLimiter
from app.resources import ResourceStore
from app.schemas import ResourceCreate, ResourceOut
from app.versioning import deprecation_headers, resolve_version

DEFAULT_CALLER = "anonymous"


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def create_app(
    *,
    default_limit: int | None = None,
    window_seconds: float | None = None,
    caller_overrides: dict[str, int] | None = None,
    clock: callable | None = None,
) -> FastAPI:
    default_limit = _int_env("QUOTA_LIMIT", 100) if default_limit is None else default_limit
    window_seconds = (
        float(_int_env("QUOTA_WINDOW_SECONDS", 60))
        if window_seconds is None
        else window_seconds
    )

    applog.configure_logging()
    rate_limiter = RateLimiter(
        default_limit=default_limit,
        window_seconds=window_seconds,
        overrides=caller_overrides,
        clock=clock,
    )
    idempotency = IdempotencyStore()
    resources = ResourceStore()
    app = FastAPI(title="API Version Gateway", version="0.1.0")
    app.state.rate_limiter = rate_limiter
    app.state.idempotency = idempotency
    app.state.resources = resources

    def _caller(request: Request) -> str:
        return request.headers.get("X-Caller-Id", DEFAULT_CALLER).strip() or DEFAULT_CALLER

    def _error_response(exc: APIError, extra_headers: dict[str, str] | None = None) -> JSONResponse:
        headers = dict(exc.headers)
        if extra_headers:
            headers.update(extra_headers)
        body = {
            "error": {
                "code": exc.code.value,
                "reason": exc.code.value,
                "message": exc.message,
                "details": exc.details,
            }
        }
        return JSONResponse(status_code=exc.status_code, content=body, headers=headers)

    @app.exception_handler(APIError)
    async def _handle_api_error(_request: Request, exc: APIError) -> JSONResponse:
        # 已解析为“已弃用”版本后发生的错误，也带上弃用提示头
        info = getattr(_request.state, "version_info", None)
        return _error_response(exc, deprecation_headers(info) if info else None)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    def _rate_limit(request: Request, caller_id: str) -> None:
        decision = rate_limiter.check_and_consume(caller_id)
        request_headers: dict[str, str] = {
            "X-RateLimit-Limit": str(decision.limit),
            "X-RateLimit-Remaining": str(decision.remaining),
            "X-RateLimit-Window-Seconds": str(window_seconds),
        }
        request.state.rate_headers = request_headers
        if not decision.allowed:
            retry_after = max(1, math.ceil(decision.retry_after))
            applog.log_quota(
                caller_id,
                "rejected",
                limit=decision.limit,
                used=decision.limit,
                remaining=0,
                window_seconds=window_seconds,
                retry_after=float(retry_after),
                reason="sliding_window_full",
            )
            raise APIError(
                ErrorCode.QUOTA_EXCEEDED,
                f"调用方 {caller_id!r} 在 {window_seconds:g} 秒窗口内已用满配额（{decision.limit} 次）。",
                status_code=429,
                headers={**request_headers, "Retry-After": str(retry_after)},
                details={
                    "caller_id": caller_id,
                    "limit": decision.limit,
                    "window_seconds": window_seconds,
                    "retry_after_seconds": retry_after,
                },
            )
        applog.log_quota(
            caller_id,
            "allowed",
            limit=decision.limit,
            used=decision.limit - decision.remaining,
            remaining=decision.remaining,
            window_seconds=window_seconds,
        )

    def _negotiate(request: Request, caller_id: str) -> Any:
        requested = request.headers.get("X-API-Version")
        try:
            info = resolve_version(requested)
        except APIError as exc:
            applog.log_version_decision(
                caller_id, requested, "rejected", reason=exc.code.value
            )
            raise
        applog.log_version_decision(
            caller_id,
            requested,
            "accepted",
            resolved=info.version,
            status=info.status,
            sunset=info.sunset.isoformat() if info.sunset else None,
        )
        request.state.version_info = info
        return info

    @app.post("/resources", response_model=ResourceOut, status_code=201)
    async def create_resource(
        request: Request, payload: ResourceCreate
    ) -> Response | ResourceOut:
        caller_id = _caller(request)
        info = _negotiate(request, caller_id)

        key = request.headers.get("Idempotency-Key", "").strip()
        if not key:
            raise APIError(
                ErrorCode.IDEMPOTENCY_KEY_MISSING,
                "写入请求必须提供 Idempotency-Key 请求头以保证幂等。",
                status_code=400,
                details={"header": "Idempotency-Key"},
            )

        fingerprint = hashlib.sha256(
            json.dumps(
                payload.model_dump(), sort_keys=True, separators=(",", ":"), default=str
            ).encode("utf-8")
        ).hexdigest()

        # 幂等判定先于配额：重复提交直接重放结果，不重复占用配额、不产生副作用
        outcome = idempotency.acquire(caller_id, key, fingerprint)

        if outcome.status == "replay":
            record = outcome.record
            applog.log_idempotency(
                caller_id, key, "replay", fingerprint=fingerprint,
                resource_id=record.resource_id,
            )
            replay_headers = {
                **deprecation_headers(info),
                "Idempotency-Replayed": "true",
            }
            return JSONResponse(
                status_code=record.response_status,
                content=record.response_body,
                headers=replay_headers,
            )

        # status == "acquired"：本请求取得执行资格，此刻才占用配额
        record = outcome.record
        try:
            _rate_limit(request, caller_id)
        except APIError:
            # 尚未产生副作用：释放 in_progress 记录，允许同键稍后重试
            idempotency.release(record)
            applog.log_idempotency(
                caller_id, key, "released", reason="quota_exceeded"
            )
            raise

        rate_headers = getattr(request.state, "rate_headers", {})
        common_headers = {**deprecation_headers(info), **rate_headers}
        applog.log_idempotency(caller_id, key, "acquired", fingerprint=fingerprint)
        try:
            item = resources.create(payload.name, payload.data)
            body = ResourceOut(
                id=item["id"], name=item["name"], data=item["data"], api_version=info.version
            )
            body_dict = body.model_dump()
            idempotency.complete(
                record, status_code=201, body=body_dict, resource_id=item["id"]
            )
            applog.log_idempotency(
                caller_id, key, "completed", resource_id=item["id"]
            )
            return JSONResponse(status_code=201, content=body_dict, headers=common_headers)
        except Exception:
            # 副作用未完成：释放 in_progress 记录，允许同键重试，随后上抛
            idempotency.release(record)
            applog.log_idempotency(caller_id, key, "released", reason="handler_error")
            raise

    @app.get("/resources/{resource_id}", response_model=ResourceOut)
    async def get_resource(request: Request, resource_id: str) -> ResourceOut:
        caller_id = _caller(request)
        info = _negotiate(request, caller_id)
        _rate_limit(request, caller_id)
        item = resources.get(resource_id)
        if item is None:
            raise APIError(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"资源 {resource_id!r} 不存在。",
                status_code=404,
                details={"resource_id": resource_id},
            )
        body = ResourceOut(
            id=item["id"], name=item["name"], data=item["data"], api_version=info.version
        )
        rate_headers = getattr(request.state, "rate_headers", {})
        return JSONResponse(
            status_code=200,
            content=body.model_dump(),
            headers={**deprecation_headers(info), **rate_headers},
        )

    return app


app = create_app()
