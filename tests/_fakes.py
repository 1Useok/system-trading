"""테스트용 가짜 OrderService / 시계."""
import datetime as _dt
from typing import Any, Dict, List

from trading_system.types.events import FuturesSimpleFilled

ACCEPTED = {"rt_cd": "0", "output": {"ODNO": "0000001"}}
REJECTED = {"rt_cd": "1", "msg1": "rejected"}


class FakeOrderService:
    """`OrderService.place_future_order()`와 같은 키워드 시그니처만 받는다(다르면 TypeError)."""

    def __init__(self, responses: List[Dict[str, Any]] = None):
        self.orders: List[Dict[str, Any]] = []
        self._responses = list(responses or [])

    async def place_future_order(self, *, symbol, quantity, price, side, price_type,
                                 account=None, client_order_id=None, time_in_force=None, **kwargs):
        self.orders.append({"symbol": symbol, "quantity": quantity, "price": price, "side": side})
        return self._responses.pop(0) if self._responses else ACCEPTED


class FakeClock:
    """전략 모듈의 `datetime.datetime.now()`를 고정 시각으로 바꾸기 위한 대체 모듈."""

    def __init__(self, hhmmss: str):
        self.set(hhmmss)

    def set(self, hhmmss: str) -> None:
        h, m, s = int(hhmmss[:2]), int(hhmmss[2:4]), int(hhmmss[4:6])
        self._now = _dt.datetime(2026, 11, 2, h, m, s)

    @property
    def datetime(self):
        clock = self

        class _DT(_dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return clock._now

        return _DT

    timedelta = _dt.timedelta


def tick(price: float, symbol: str = "A01612", high: float = -1, low: float = -1) -> FuturesSimpleFilled:
    return FuturesSimpleFilled(inst_code=symbol, price=price, volume=0, high=high, low=low)
