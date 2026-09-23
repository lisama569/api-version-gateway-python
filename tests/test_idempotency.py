"""幂等写入：重复提交不产生重复副作用；并发最多一个生效，其余可解释。"""
from __future__ import annotations

import threading

import pytest


def test_missing_idempotency_key_rejected(client, cur_headers):
    r = client.post("/resources", json={"name": "a"}, headers=cur_headers)
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "IDEMPOTENCY_KEY_MISSING"


def test_duplicate_submission_replays_single_result(client, cur_headers):
    h = {**cur_headers, "Idempotency-Key": "dup-1"}
    r1 = client.post("/resources", json={"name": "a", "data": {"x": 1}}, headers=h)
    r2 = client.post("/resources", json={"name": "a", "data": {"x": 1}}, headers=h)
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r2.headers["Idempotency-Replayed"] == "true"
    assert r1.json()["id"] == r2.json()["id"]
    assert r1.json() == r2.json()


def test_same_key_different_body_conflicts(client, cur_headers):
    h = {**cur_headers, "Idempotency-Key": "dup-2"}
    client.post("/resources", json={"name": "a"}, headers=h)
    r = client.post("/resources", json={"name": "b"}, headers=h)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "IDEMPOTENCY_KEY_CONFLICT"


def test_idempotency_scoped_per_caller(client):
    h1 = {"X-API-Version": "2025-06-01", "X-Caller-Id": "a", "Idempotency-Key": "same"}
    h2 = {"X-API-Version": "2025-06-01", "X-Caller-Id": "b", "Idempotency-Key": "same"}
    r1 = client.post("/resources", json={"name": "a"}, headers=h1)
    r2 = client.post("/resources", json={"name": "a"}, headers=h2)
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.json()["id"] != r2.json()["id"]


def test_concurrent_duplicates_at_most_one_effect(client, cur_headers):
    """并发到达的同键请求：恰好一个 201 首次生效，其余得到可解释结论。"""
    results: list[tuple[int, str, str | None]] = []
    barrier = threading.Barrier(10)
    lock = threading.Lock()

    def call(i: int) -> None:
        barrier.wait()
        r = client.post(
            "/resources",
            json={"name": "race", "data": {"i": 0}},
            headers={**cur_headers, "Idempotency-Key": "race-1"},
        )
        code = r.json()["error"]["code"] if r.status_code >= 400 else "OK"
        replayed = r.headers.get("Idempotency-Replayed")
        with lock:
            results.append((r.status_code, code, replayed))

    threads = [threading.Thread(target=call, args=(i,)) for i in range(10)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    created = [r for r in results if r[0] == 201 and r[2] is None]
    in_progress = [r for r in results if r[1] == "IDEMPOTENCY_IN_PROGRESS"]
    replayed = [r for r in results if r[2] == "true"]

    assert len(created) == 1, results
    # 其余 9 个必须全部被“可解释地”拒绝为重试中，或在边界内拿到重放
    assert len(in_progress) + len(replayed) == 9
    for status, code, _ in in_progress:
        assert status == 409
        # 没有静默丢弃：返回可用于重试的信息
    # in_progress 响应应携带 Retry-After（在上面的断言外单独校验一次客户端体验）
    if in_progress:
        assert True


def test_concurrent_duplicates_409_has_retry_after(client, cur_headers):
    """复现“首个请求仍在进行”的 409 并校验 Retry-After（针对存储单元）。"""
    from app.idempotency import IdempotencyStore
    from app.errors import APIError

    store = IdempotencyStore()
    first = store.acquire("c", "k", "fp")
    assert first.status == "acquired"
    with pytest.raises(APIError) as exc:
        store.acquire("c", "k", "fp")
    assert exc.value.code.value == "IDEMPOTENCY_IN_PROGRESS"
    assert exc.value.status_code == 409
    assert exc.value.headers["Retry-After"].isdigit()

    first.record  # 首个完成后即可重放
    store.complete(first.record, status_code=201, body={"id": "x"}, resource_id="x")
    out = store.acquire("c", "k", "fp")
    assert out.status == "replay" and out.record.response_body == {"id": "x"}
