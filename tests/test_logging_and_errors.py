"""日志须包含版本判定、幂等键与配额判定依据；错误统一归类可解释。"""
from __future__ import annotations

import json
import logging

import pytest


def _json_logs(caplog) -> list[dict]:
    out = []
    for record in caplog.records:
        if record.name != "api_gateway":
            continue
        try:
            out.append(json.loads(record.getMessage()))
        except json.JSONDecodeError:
            pass
    return out


def test_logs_include_version_idempotency_quota(client, caplog):
    caplog.set_level(logging.INFO, logger="api_gateway")
    h = {"X-API-Version": "2025-06-01", "X-Caller-Id": "zoe", "Idempotency-Key": "log-1"}
    client.post("/resources", json={"name": "n"}, headers=h)

    logs = _json_logs(caplog)
    events = {log["event"]: log for log in logs}

    # 版本判定
    v = events["version_decision"]
    assert v["requested_version"] == "2025-06-01"
    assert v["resolved_version"] == "2025-06-01"
    assert v["version_status"] == "current"
    assert v["outcome"] == "accepted"

    # 幂等键
    idem = [log for log in logs if log["event"] == "idempotency"]
    assert {log["idempotency_key"] for log in idem} == {"log-1"}
    assert {log["outcome"] for log in idem} >= {"acquired", "completed"}

    # 配额判定依据
    q = events["quota_decision"]
    assert q["caller_id"] == "zoe"
    assert q["limit"] == 3 and q["remaining"] == 2 and q["outcome"] == "allowed"


def test_rejected_version_logs_reason(client, caplog):
    caplog.set_level(logging.INFO, logger="api_gateway")
    client.post(
        "/resources",
        json={"name": "n"},
        headers={"X-API-Version": "2023-01-01", "Idempotency-Key": "k"},
    )
    v = [log for log in _json_logs(caplog) if log["event"] == "version_decision"][0]
    assert v["outcome"] == "rejected"
    assert v["reason"] == "VERSION_RETIRED"


def test_quota_rejection_logs_basis(client, caplog):
    caplog.set_level(logging.INFO, logger="api_gateway")
    for i in range(4):
        client.post(
            "/resources",
            json={"name": "n"},
            headers={"X-API-Version": "2025-06-01", "Idempotency-Key": f"q{i}"},
        )
    rejected = [
        log
        for log in _json_logs(caplog)
        if log["event"] == "quota_decision" and log["outcome"] == "rejected"
    ]
    assert rejected and rejected[0]["reason"] == "sliding_window_full"
    assert rejected[0]["retry_after"] >= 1


def test_error_body_shape_is_consistent(client):
    r = client.post("/resources", json={"name": "n"}, headers={"X-API-Version": "2025-06-01"})
    err = r.json()["error"]
    assert set(err.keys()) == {"code", "reason", "message", "details"}
    assert err["code"] == err["reason"] == "IDEMPOTENCY_KEY_MISSING"
    assert err["message"]


def test_validation_error_still_http_error(client, cur_headers):
    # name 为空 -> 请求体校验失败，返回 4xx 而非 500
    r = client.post(
        "/resources",
        json={"name": ""},
        headers={**cur_headers, "Idempotency-Key": "v1"},
    )
    assert r.status_code == 422
