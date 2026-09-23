"""pytest 公共夹具：构造带可控时钟与小配额的应用，便于边界与窗口测试。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def client(clock: FakeClock) -> TestClient:
    app = create_app(
        default_limit=3,
        window_seconds=10.0,
        caller_overrides={"vip": 100},
        clock=clock,
    )
    return TestClient(app, raise_server_exceptions=True)


@pytest.fixture()
def cur_headers() -> dict[str, str]:
    return {"X-API-Version": "2025-06-01", "X-Caller-Id": "alice"}
