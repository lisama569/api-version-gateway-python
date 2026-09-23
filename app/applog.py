"""结构化日志：输出版本判定、幂等键与配额判定依据，便于解释每次决策。"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger("api_gateway")


def configure_logging(level: int = logging.INFO) -> None:
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False


def _emit(event: str, **fields: object) -> None:
    payload = {"event": event, **fields}
    logger.info(json.dumps(payload, ensure_ascii=False, default=str))


def log_version_decision(
    caller_id: str,
    requested: str | None,
    outcome: str,
    *,
    resolved: str | None = None,
    status: str | None = None,
    sunset: str | None = None,
    reason: str | None = None,
) -> None:
    _emit(
        "version_decision",
        caller_id=caller_id,
        requested_version=requested,
        outcome=outcome,
        resolved_version=resolved,
        version_status=status,
        sunset=sunset,
        reason=reason,
    )


def log_idempotency(
    caller_id: str,
    key: str,
    outcome: str,
    *,
    fingerprint: str | None = None,
    resource_id: str | None = None,
    reason: str | None = None,
) -> None:
    _emit(
        "idempotency",
        caller_id=caller_id,
        idempotency_key=key,
        outcome=outcome,
        request_fingerprint=fingerprint,
        resource_id=resource_id,
        reason=reason,
    )


def log_quota(
    caller_id: str,
    outcome: str,
    *,
    limit: int,
    used: int | None = None,
    remaining: int | None = None,
    window_seconds: float | None = None,
    retry_after: float | None = None,
    reason: str | None = None,
) -> None:
    _emit(
        "quota_decision",
        caller_id=caller_id,
        outcome=outcome,
        limit=limit,
        used=used,
        remaining=remaining,
        window_seconds=window_seconds,
        retry_after=retry_after,
        reason=reason,
    )
