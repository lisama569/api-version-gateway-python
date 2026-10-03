"""端到端：错误归类可区分、判定过程有日志可解释、管道顺序正确。"""

from __future__ import annotations

import logging
import threading

import pytest

from gateway.app import configure_logging
from gateway.idempotency import ACQUIRED

H = {"X-API-Version": "2024-01-01", "X-Caller-Id": "premium"}


def test_error_codes_are_distinct_categories(client):
    cases = [
        (lambda: client.post("/orders", json={"name": "x"}), 400, "version_undeclared"),
        (lambda: client.post("/orders", json={"name": "x"},
                             headers={"X-API-Version": "nope"}), 404, "version_unknown"),
        (lambda: client.post("/orders", json={"name": "x"},
                             headers={"X-API-Version": "2022-01-01"}), 410, "version_sunset"),
        (lambda: client.post("/orders", json={"name": "x"}, headers=H),
         400, "idempotency_key_missing"),
        (lambda: client.get("/orders/missing", headers=H), 404, "resource_not_found"),
    ]
    codes = set()
    for call, status, code in cases:
        r = call()
        assert r.status_code == status, (status, r.text)
        body = r.json()
        assert body["error"]["code"] == code
        assert body["error"]["message"]
        codes.add(code)
    assert len(codes) == 5  # 五类拒绝原因互不相同


def test_pipeline_order_version_before_quota_before_idempotency(client):
    """即使调用方配额已耗尽，坏版本请求仍先报版本错误。"""
    # 耗尽 premium 之外的 trial 配额
    th = {"X-API-Version": "2024-01-01", "X-Caller-Id": "trial"}
    for _ in range(3):
        client.get("/orders/x", headers=th)
    assert client.get("/orders/x", headers=th).status_code == 429
    # 同调用方不带版本 → 仍是版本错误（版本协商先于配额）
    r = client.get("/orders/x", headers={"X-Caller-Id": "trial"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "version_undeclared"


def test_logs_record_version_idempotency_and_quota_decisions(client, caplog):
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        client.post(
            "/orders",
            json={"name": "logged"},
            headers={**H, "Idempotency-Key": "log-1"},
        )
        client.post(
            "/orders",
            json={"name": "logged"},
            headers={**H, "Idempotency-Key": "log-1"},
        )
    events = {getattr(r, "event", None): r.getMessage() for r in caplog.records}
    assert "version_decision" in events
    assert "result=supported" in events["version_decision"]
    assert "requested=2024-01-01" in events["version_decision"]
    idem = [r.getMessage() for r in caplog.records if getattr(r, "event", None) == "idempotency_decision"]
    assert any("idempotency_key=log-1" in m and "outcome=completed" in m for m in idem)
    assert any("idempotency_key=log-1" in m and "outcome=replay" in m for m in idem)
    quota = events["quota_decision"]
    assert "caller=premium" in quota and "allowed=true" in quota and "limit=1000" in quota


def test_logs_distinguish_rejection_reasons(client, caplog):
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        client.post("/orders", json={"name": "x"})
        client.post("/orders", json={"name": "x"}, headers={"X-API-Version": "2022-01-01"})
    msgs = [r.getMessage() for r in caplog.records if getattr(r, "event", None) == "version_decision"]
    assert any("result=rejected" in m and "reason=version_undeclared" in m for m in msgs)
    assert any("result=rejected" in m and "reason=version_sunset" in m for m in msgs)


def test_quota_exhaustion_logged_with_denial_basis(client, caplog):
    th = {"X-API-Version": "2024-01-01", "X-Caller-Id": "trial"}
    with caplog.at_level(logging.INFO, logger="api_gateway"):
        for _ in range(4):
            client.get("/orders/x", headers=th)
    msgs = [r.getMessage() for r in caplog.records if getattr(r, "event", None) == "quota_decision"]
    assert any("allowed=false" in m and "used=4" in m and "limit=3" in m for m in msgs)


def test_inflight_duplicate_returns_explainable_conflict(client):
    """首个请求仍在处理时到达的重复请求：409 + 可解释结论，而非静默丢弃。"""
    store = client.app.state.gateway.idempotency
    from gateway.idempotency import fingerprint_payload

    fp = fingerprint_payload({"name": "slow", "quantity": 1, "amount": 0})
    assert store.acquire("slow-key", fp, namespace="premium") == ACQUIRED  # 模拟首个请求占位未完成

    def finish_later() -> None:
        store.complete("slow-key", {"order": {"id": "late"}}, 201, namespace="premium")

    timer = threading.Timer(0.05, finish_later)
    timer.start()
    # 等待者应在首个请求完成后拿到重放结论
    r = client.post(
        "/orders",
        json={"name": "slow"},
        headers={**H, "Idempotency-Key": "slow-key"},
    )
    timer.join()
    assert r.status_code == 201
    assert r.headers["Idempotent-Replay"] == "true"


def test_failed_first_attempt_replayed_with_explanation(client, monkeypatch):
    """首个请求处理失败：等待/重放方拿到失败结论，且可更换幂等键重试。"""
    state = client.app.state.gateway

    def boom(caller, payload):
        raise RuntimeError("write failed")

    monkeypatch.setattr(state.orders, "create_order", boom)
    with pytest.raises(RuntimeError):
        client.post(
            "/orders", json={"name": "x"}, headers={**H, "Idempotency-Key": "fail-key"}
        )
    entry = state.idempotency.get_result("fail-key", namespace="premium")
    assert entry.status == "failed"
    assert entry.error_code == "internal_error"

    # 稍后用同一键重放 → 得到可解释的失败回放（不再次执行副作用）
    r = client.post(
        "/orders", json={"name": "x"}, headers={**H, "Idempotency-Key": "fail-key"}
    )
    assert r.status_code == 500
    body = r.json()
    assert body["error"]["code"] == "idempotent_replay_internal_error"
    assert body["error"]["details"]["original_error_code"] == "internal_error"
    assert r.headers["Idempotent-Replay"] == "true"

    # 更换新幂等键后可重新提交
    monkeypatch.undo()
    r2 = client.post(
        "/orders", json={"name": "x"}, headers={**H, "Idempotency-Key": "fresh-key"}
    )
    assert r2.status_code == 201


def test_inflight_timeout_returns_explainable_409(client):
    """首个请求一直不结束：等待者拿到 409 idempotency_inflight，而不是静默丢弃。"""
    from gateway.idempotency import IdempotencyStore
    import gateway.app as appmod

    store = client.app.state.gateway.idempotency
    from gateway.idempotency import fingerprint_payload

    fp = fingerprint_payload({"name": "hung", "quantity": 1, "amount": 0})
    store.acquire("hung-key", fp, namespace="premium")  # 永不 complete

    original_wait = IdempotencyStore.wait_for_completion

    def fast_wait(self, key, timeout=None):
        return original_wait(self, key, timeout=0.01)

    appmod.IDEMPOTENCY_WAIT_SECONDS = 0.01
    try:
        r = client.post(
            "/orders", json={"name": "hung"},
            headers={**H, "Idempotency-Key": "hung-key"},
        )
    finally:
        appmod.IDEMPOTENCY_WAIT_SECONDS = 10.0
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["code"] == "idempotency_inflight"
    assert body["error"]["details"]["idempotency_key"] == "hung-key"
    # 未产生副作用
    assert client.app.state.gateway.orders.side_effects == 0


def test_malformed_json_rejected_as_invalid_request(client):
    r = client.post(
        "/orders",
        content=b"{not-json",
        headers={**H, "Idempotency-Key": "bad-json", "Content-Type": "application/json"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_rate_limit_reset_header_present_on_allowed_responses(client):
    r = client.get("/orders/x", headers=H)
    assert r.headers["X-RateLimit-Limit"] == "1000"
    assert r.headers["X-RateLimit-Remaining"] == "999"
    assert 0 < int(r.headers["X-RateLimit-Reset"]) <= 60
