"""内存资源存储（不依赖外部服务或真实凭据）。"""
from __future__ import annotations

import itertools
import threading
from typing import Any


class ResourceStore:
    def __init__(self) -> None:
        self._items: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._counter = itertools.count(1)

    def create(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            resource_id = f"res-{next(self._counter):06d}"
            item = {"id": resource_id, "name": name, "data": dict(payload)}
            self._items[resource_id] = item
            return dict(item)

    def get(self, resource_id: str) -> dict[str, Any] | None:
        with self._lock:
            item = self._items.get(resource_id)
            return dict(item) if item is not None else None
