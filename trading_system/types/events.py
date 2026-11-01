"""공통 이벤트(메시지) 타입 정의.

설계 원칙:
- Market Data(`FuturesSimpleQuote`/`FuturesSimpleFilled`)와
  ExecutionReport(`FuturesSimpleOrderAck`)를 코드 레벨에서 구분한다.
- ACK(`FuturesSimpleOrderAck`)는 `strategy_id`/`client_order_id`를 통해
  `ExecutionReportRouter`가 특정 전략으로 타겟 디스패치할 수 있어야 한다.
"""

from __future__ import annotations

from typing import Any, Optional

from .enums import AckType, BuySell


class FuturesSimpleQuote:
    def __init__(
        self,
        inst_code: str = "",
        best_ask_price: float = -1,
        best_bid_price: float = -1,
        best_ask_volume: int = -1,
        best_bid_volume: int = -1,
    ):
        self.inst_code = inst_code
        self.best_ask_price = best_ask_price
        self.best_bid_price = best_bid_price
        self.best_ask_volume = best_ask_volume
        self.best_bid_volume = best_bid_volume


class FuturesSimpleFilled:
    def __init__(
        self,
        inst_code: str = "",
        price: float = -1,
        volume: int = -1,
        price_chg: float = -1,
        open: float = -1,
        high: float = -1,
        low: float = -1,
    ):
        self.inst_code = inst_code
        self.price = price
        self.volume = volume
        self.price_chg = price_chg
        self.open = open
        self.high = high
        self.low = low


class FuturesSimpleOrderAck:
    def __init__(
        self,
        ack_type: AckType = AckType.Nothing,
        account: str = "",
        inst_code: str = "",
        order_num: str = "",
        original_order_num: str = "",
        cancel_or_replace_volume: int = 0,
        filled_price: float = -1,
        filled_volume: int = 0,
        buysell: BuySell = BuySell.NA,
        broker: str = "",
        strategy_id: str = "",
        client_order_id: str = "",
        timestamp: Any = None,
        event_id: str = "",
        seq: Optional[int] = None,
    ):
        self.ack_type = ack_type
        self.account = account
        self.inst_code = inst_code
        self.order_num = order_num
        self.original_order_num = original_order_num
        self.cancel_or_replace_volume = cancel_or_replace_volume
        self.filled_price = filled_price
        self.filled_volume = filled_volume
        self.buysell = buysell

        # 확장 필드(라우팅/추적/정렬)
        self.broker = broker
        self.strategy_id = strategy_id
        self.client_order_id = client_order_id
        self.timestamp = timestamp
        self.event_id = event_id
        self.seq = seq
