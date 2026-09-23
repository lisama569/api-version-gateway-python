"""版本协商：未声明 / 未知 / 已下线 / 已弃用 / 受支持 必须可区分。"""

from __future__ import annotations

import pytest

from gateway.config import VersionStatus, default_settings
from gateway.errors import (
    VersionSunsetError,
    VersionUndeclaredError,
    VersionUnknownError,
)
from gateway.versioning import negotiate_version

V = "2024-01-01"
HEADERS = {"X-API-Version": V}


def test_supported_and_deprecated_resolve(settings):
    d1 = negotiate_version("2024-01-01", settings)
    assert d1.version.status is VersionStatus.SUPPORTED
    d2 = negotiate_version(" 2023-01-01 ", settings)
    assert d2.requested == "2023-01-01"
    assert d2.version.status is VersionStatus.DEPRECATED
    assert d2.version.sunset_at == "2027-06-30T00:00:00Z"


def test_undeclared_rejected_with_distinct_code(settings):
    for raw in (None, "", "   "):
        with pytest.raises(VersionUndeclaredError) as ei:
            negotiate_version(raw, settings)
        assert ei.value.error_code == "version_undeclared"
        assert ei.value.status_code == 400
        assert ei.value.details["header"] == "X-API-Version"
        assert set(ei.value.details["available"]) == {
            "2024-01-01", "2023-01-01", "2022-01-01",
        }


def test_unknown_version_rejected(settings):
    with pytest.raises(VersionUnknownError) as ei:
        negotiate_version("2099-01-01", settings)
    assert ei.value.error_code == "version_unknown"
    assert ei.value.status_code == 404
    assert ei.value.details["requested_version"] == "2099-01-01"


def test_sunset_version_rejected_with_retirement_date(settings):
    with pytest.raises(VersionSunsetError) as ei:
        negotiate_version("2022-01-01", settings)
    assert ei.value.error_code == "version_sunset"
    assert ei.value.status_code == 410
    assert ei.value.details["sunset_at"] == "2025-12-31T00:00:00Z"
    assert ei.value.details["successor"] == "2024-01-01"


# ---- HTTP 层：错误码与状态码同样可区分 ----

def test_http_undeclared(client):
    r = client.post("/orders", json={"name": "x"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "version_undeclared"


def test_http_unknown(client):
    r = client.post("/orders", json={"name": "x"}, headers={"X-API-Version": "1999"})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "version_unknown"


def test_http_sunset(client):
    r = client.post("/orders", json={"name": "x"}, headers={"X-API-Version": "2022-01-01"})
    assert r.status_code == 410
    body = r.json()
    assert body["error"]["code"] == "version_sunset"
    assert body["error"]["details"]["sunset_at"] == "2025-12-31T00:00:00Z"


def test_http_deprecated_carries_warning_headers(client):
    h = {"X-API-Version": "2023-01-01", "Idempotency-Key": "k1", "X-Caller-Id": "premium"}
    r = client.post("/orders", json={"name": "x"}, headers=h)
    assert r.status_code == 201
    assert r.headers["Deprecation"] == "true"
    assert r.headers["X-API-Deprecated"] == "true"
    assert r.headers["Sunset"] == "2027-06-30T00:00:00Z"
    assert r.headers["X-API-Successor"] == "2024-01-01"
    assert r.headers["Warning"].startswith("299")
    assert "2027-06-30" in r.headers["Warning"]
    # 协商后的版本会回显
    assert r.headers["X-API-Version"] == "2023-01-01"


def test_resource_shape_identical_across_versions(client):
    """同一资源在支持版与弃用版下字段与语义完全一致。"""
    h1 = {"X-API-Version": "2024-01-01", "Idempotency-Key": "k1", "X-Caller-Id": "premium"}
    r1 = client.post("/orders", json={"name": "book", "quantity": 2, "amount": 9}, headers=h1)
    oid = r1.json()["order"]["id"]

    h2 = {"X-API-Version": "2023-01-01", "Idempotency-Key": "k2", "X-Caller-Id": "premium"}
    client.post("/orders", json={"name": "seed"}, headers=h2)
    g1 = client.get(f"/orders/{oid}", headers={"X-API-Version": "2024-01-01", "X-Caller-Id": "premium"})
    g2 = client.get(f"/orders/{oid}", headers={"X-API-Version": "2023-01-01", "X-Caller-Id": "premium"})
    assert g1.json()["order"] == g2.json()["order"]
    assert set(g1.json()["order"]) == {"id", "name", "quantity", "amount", "created_by"}


def test_versions_directory_is_public(client):
    r = client.get("/versions")
    assert r.status_code == 200
    statuses = {v["version"]: v["status"] for v in r.json()["versions"]}
    assert statuses == {
        "2024-01-01": "supported",
        "2023-01-01": "deprecated",
        "2022-01-01": "sunset",
    }
    assert r.json()["recommended"] == "2024-01-01"
