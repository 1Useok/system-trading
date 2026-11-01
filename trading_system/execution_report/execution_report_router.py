"""Execution report 라우터(`ExecutionReportRouter`).

역할:
- `FuturesSimpleOrderAck`(ExecutionReport)만 처리합니다.
- Market data(`FuturesSimpleQuote`/`FuturesSimpleFilled`)는 절대 처리하지 않습니다.

라우팅:
- 원칙적으로 특정 전략(주문을 낸 주체)에게만 전달하는 것이 정합적입니다.
- `strategy_id`가 있으면 우선 사용
- 없으면 `client_order_id -> strategy_id` 매핑(`OrderRegistry`)으로 역참조
- 그래도 못 찾으면 **drop + warning** (혼용/오배달 방지)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, TYPE_CHECKING

from trading_system.execution_report.order_registry import OrderRegistry
from trading_system.types.events import FuturesSimpleFilled, FuturesSimpleOrderAck

if TYPE_CHECKING:  # pragma: no cover
    from trading_system.strategy.execution.strategy_executor import StrategyExecutor

logger = logging.getLogger(__name__)


class ExecutionReportRouter:
    def __init__(
        self,
        order_registry: Optional[OrderRegistry] = None,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        executor: Optional["StrategyExecutor"] = None,
    ):
        self._strategies_by_id: Dict[str, object] = {}
        self.features: List[object] = []
        self.connectors: List[object] = []
        self.order_registry = order_registry
        self._loop = loop
        self._executor = executor

    def register_strategy(self, strategy) -> None:
        strategy_id = getattr(strategy, "strategy_name", None)
        if strategy_id:
            self._strategies_by_id[strategy_id] = strategy

    def register_feature(self, feature) -> None:
        self.features.append(feature)

    def add_connector(self, connector) -> None:
        self.connectors.append(connector)
        connector.set_manager(self)

    def on_ack(self, ack: FuturesSimpleOrderAck) -> None:
        # 방어: 시장 틱이 ACK 경로로 유입되면 장부/포지션 정합성이 깨질 수 있음
        if isinstance(ack, FuturesSimpleFilled):
            raise TypeError("Market tick (FuturesSimpleFilled) must not be passed to on_ack()")
        if not isinstance(ack, FuturesSimpleOrderAck):
            raise TypeError(f"Invalid ack type: {type(ack)}")

        # 1) ack에 strategy_id가 직접 있으면 사용
        target_strategy_id = getattr(ack, "strategy_id", None)

        # 2) 없으면 client_order_id로 registry 역참조
        if not target_strategy_id and self.order_registry is not None:
            client_order_id = getattr(ack, "client_order_id", "") or ""
            target_strategy_id = self.order_registry.resolve_strategy_id(client_order_id)

        if target_strategy_id and target_strategy_id in self._strategies_by_id:
            if self._loop is None or self._executor is None:
                raise RuntimeError("ExecutionReportRouter requires injected (loop, executor) in ver2")

            strategy = self._strategies_by_id[target_strategy_id]
            if not self._executor.is_registered(strategy_id=target_strategy_id):
                self._executor.register(strategy_id=target_strategy_id, strategy=strategy)
            self._executor.publish_ack_threadsafe(strategy_id=target_strategy_id, ack=ack)
        else:
            # 혼용 방지: 라우팅 정보가 없으면 broadcast 대신 drop
            logger.warning(
                "ExecutionReportRouter: dropped ACK due to missing routing (strategy_id=%s, client_order_id=%s)",
                getattr(ack, "strategy_id", ""),
                getattr(ack, "client_order_id", ""),
            )

        for feature in self.features:
            feature.on_ack(ack)
