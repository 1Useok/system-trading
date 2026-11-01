"""시장 데이터 허브(`MarketDataHub`).

역할:
- `FuturesSimpleQuote`(호가), `FuturesSimpleFilled`(시장 틱)만 처리합니다.
- 전략/피처에 기본적으로 broadcast 합니다.

의도:
- ExecutionReport(ACK)를 물리적으로 다른 파이프라인으로 분리하기 위해
  이 Hub에는 `on_ack()` 자체를 두지 않습니다.
"""

from __future__ import annotations

import asyncio
from typing import List, Optional, TYPE_CHECKING

from trading_system.types.events import FuturesSimpleFilled, FuturesSimpleQuote

if TYPE_CHECKING:  # pragma: no cover
    from trading_system.strategy.execution.strategy_executor import StrategyExecutor


class MarketDataHub:
    def __init__(
        self,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        executor: Optional["StrategyExecutor"] = None,
    ):
        self._loop = loop
        self._executor = executor
        self.strategies: List[object] = []
        self.features: List[object] = []
        self.connectors: List[object] = []

    def register_strategy(self, strategy) -> None:
        self.strategies.append(strategy)

    def register_feature(self, feature) -> None:
        self.features.append(feature)

    def add_connector(self, connector) -> None:
        self.connectors.append(connector)
        connector.set_manager(self)

    def on_quote(self, quote: FuturesSimpleQuote) -> None:
        if self._loop is None or self._executor is None:
            raise RuntimeError("MarketDataHub requires injected (loop, executor)")

        for strategy in self.strategies:
            strategy_id = getattr(strategy, "strategy_id", None) or getattr(strategy, "strategy_name", None)
            if not strategy_id:
                continue
            if not self._executor.is_registered(strategy_id=strategy_id):
                self._executor.register(strategy_id=strategy_id, strategy=strategy)
            self._executor.publish_quote_threadsafe(strategy_id=strategy_id, quote=quote)
        for feature in self.features:
            feature.on_quote(quote)

    def on_filled(self, filled: FuturesSimpleFilled) -> None:
        if self._loop is None or self._executor is None:
            raise RuntimeError("MarketDataHub requires injected (loop, executor) in ver2")

        for strategy in self.strategies:
            strategy_id = getattr(strategy, "strategy_id", None) or getattr(strategy, "strategy_name", None)
            if not strategy_id:
                continue
            if not self._executor.is_registered(strategy_id=strategy_id):
                self._executor.register(strategy_id=strategy_id, strategy=strategy)
            self._executor.publish_filled_threadsafe(strategy_id=strategy_id, filled=filled)
        for feature in self.features:
            feature.on_filled(filled)
