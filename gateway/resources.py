"""资源模型与写操作服务（纯内存；副作用以计数器体现，便于测试断言）。"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, asdict
from typing import Any

from gateway.errors import ResourceNotFoundError


@dataclass
class Order:
    """订单资源：所有在线版本返回完全一致的字段与语义。"""

    id: str
    name: str
    quantity: int
    amount: int
    created_by: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class OrderService:
    """订单写入服务：每次真实写入递增 side_effects，重放请求不递增。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._orders: dict[str, Order] = {}
        self._seq = 0
        self.side_effects = 0

    def create_order(self, caller: str, payload: dict[str, Any]) -> Order:
        """执行一次真实写入（调用方需已通过幂等占位）。"""
        with self._lock:
            self._seq += 1
            order = Order(
                id=uuid.uuid4().hex[:12],
                name=str(payload["name"]),
                quantity=int(payload.get("quantity", 1)),
                amount=int(payload.get("amount", 0)),
                created_by=caller,
            )
            self._orders[order.id] = order
            # 真实副作用：只有幂等占位赢家会走到这里
            self.side_effects += 1
            return order

    def get_order(self, order_id: str) -> Order:
        with self._lock:
            order = self._orders.get(order_id)
        if order is None:
            raise ResourceNotFoundError(
                f"订单不存在：{order_id}", details={"resource": "order", "id": order_id}
            )
        return order
