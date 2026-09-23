"""版本协商：未知 / 已下线 / 未声明原因可区分；已弃用给出停止支持时间。"""
from __future__ import annotations


def test_version_undeclared_is_distinct(client, cur_headers):
    h = dict(cur_headers)
    h.pop("X-API-Version")
    r = client.post("/resources", json={"name": "a"}, headers=h)
    assert r.status_code == 400
    body = r.json()["error"]
    assert body["code"] == "VERSION_UNDECLARED"
    assert "supported_versions" in body["details"]


def test_version_unknown_is_distinct(client, cur_headers):
    r = client.post(
        "/resources",
        json={"name": "a"},
        headers={**cur_headers, "X-API-Version": "1999-09-09", "Idempotency-Key": "k"},
    )
    assert r.status_code == 404
    body = r.json()["error"]
    assert body["code"] == "VERSION_UNKNOWN"
    assert body["details"]["requested_version"] == "1999-09-09"


def test_version_retired_is_distinct(client, cur_headers):
    r = client.post(
        "/resources",
        json={"name": "a"},
        headers={**cur_headers, "X-API-Version": "2023-01-01", "Idempotency-Key": "k"},
    )
    assert r.status_code == 410
    body = r.json()["error"]
    assert body["code"] == "VERSION_RETIRED"
    assert body["details"]["sunset"] == "2024-06-01"
    assert body["details"]["successor"] == "2024-01-01"


def test_three_rejection_reasons_are_distinguishable(client, cur_headers):
    codes = {}
    cases = [
        (None, "k0"),
        ("1999-09-09", "k1"),
        ("2023-01-01", "k2"),
    ]
    for version, key in cases:
        headers = dict(cur_headers)
        headers["Idempotency-Key"] = key
        if version is not None:
            headers["X-API-Version"] = version
        else:
            headers.pop("X-API-Version")
        codes[version] = client.post("/resources", json={"name": "a"}, headers=headers)
    assert codes[None].json()["error"]["code"] == "VERSION_UNDECLARED"
    assert codes["1999-09-09"].json()["error"]["code"] == "VERSION_UNKNOWN"
    assert codes["2023-01-01"].json()["error"]["code"] == "VERSION_RETIRED"
    assert len({resp.json()["error"]["code"] for resp in codes.values()}) == 3


def test_deprecated_version_accepted_with_explicit_sunset(client, cur_headers):
    headers = {
        **cur_headers,
        "X-API-Version": "2024-01-01",
        "Idempotency-Key": "dep-1",
    }
    r = client.post("/resources", json={"name": "a"}, headers=headers)
    assert r.status_code == 201
    assert r.headers["Deprecation"] == "true"
    assert r.headers["X-Api-Sunset"] == "2026-12-31"
    assert "Sunset" in r.headers
    assert r.headers["X-Api-Successor-Version"] == "2025-06-01"
    # 资源响应体仍标注实际协商到的版本
    assert r.json()["api_version"] == "2024-01-01"


def test_same_resource_shape_across_versions(client):
    """同一资源在各可用版本下字段与语义一致。"""
    h2024 = {"X-API-Version": "2024-01-01", "X-Caller-Id": "u", "Idempotency-Key": "k"}
    h2025 = {"X-API-Version": "2025-06-01", "X-Caller-Id": "u", "Idempotency-Key": "k"}
    r1 = client.post("/resources", json={"name": "x", "data": {"v": 1}}, headers=h2024)
    r2 = client.post("/resources", json={"name": "x", "data": {"v": 1}}, headers=h2025)
    assert r1.status_code == r2.status_code == 201
    assert set(r1.json().keys()) == set(r2.json().keys()) == {"id", "name", "data", "api_version"}
    assert r1.json()["id"] == r2.json()["id"]  # 同键同体 -> 同一资源（幂等重放）
    assert r1.json()["data"] == r2.json()["data"] == {"v": 1}
