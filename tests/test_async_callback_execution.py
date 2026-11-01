import asyncio

from trading_system.execution_report.execution_report_router import ExecutionReportRouter
from trading_system.market_data.market_data_hub import MarketDataHub
from trading_system.strategy.execution.strategy_executor import StrategyExecutor
from trading_system.types.enums import AckType, BuySell
from trading_system.types.events import FuturesSimpleOrderAck, FuturesSimpleQuote


def test_market_data_hub_should_execute_async_strategy_callbacks() -> None:
    """Reproduces the current silent-failure: sync hub calling `async def` does not execute it."""

    async def _main() -> None:
        called = asyncio.Event()
        loop = asyncio.get_running_loop()
        executor = StrategyExecutor(loop=loop)

        class _AsyncStrategy:
            strategy_name = "S"

            async def on_quote(self, quote: FuturesSimpleQuote) -> None:
                called.set()

            async def on_filled(self, filled) -> None:  # pragma: no cover
                return None

            async def on_ack(self, ack) -> None:  # pragma: no cover
                return None

        hub = MarketDataHub(loop=loop, executor=executor)
        hub.register_strategy(_AsyncStrategy())

        hub.on_quote(
            FuturesSimpleQuote(
                inst_code="A01612",
                best_ask_price=101.0,
                best_bid_price=100.0,
                best_ask_volume=1,
                best_bid_volume=1,
            )
        )

        await asyncio.wait_for(called.wait(), timeout=0.2)

        await executor.shutdown()

    asyncio.run(_main())


def test_execution_report_router_should_execute_async_strategy_callbacks() -> None:
    """Reproduces the current silent-failure: sync router calling `async def` does not execute it."""

    async def _main() -> None:
        called = asyncio.Event()
        loop = asyncio.get_running_loop()
        executor = StrategyExecutor(loop=loop)

        class _AsyncStrategy:
            strategy_name = "S"

            async def on_quote(self, quote) -> None:  # pragma: no cover
                return None

            async def on_filled(self, filled) -> None:  # pragma: no cover
                return None

            async def on_ack(self, ack: FuturesSimpleOrderAck) -> None:
                called.set()

        router = ExecutionReportRouter(order_registry=None, loop=loop, executor=executor)
        router.register_strategy(_AsyncStrategy())

        router.on_ack(
            FuturesSimpleOrderAck(
                ack_type=AckType.Received,
                account="ACC",
                inst_code="A01612",
                order_num="OID",
                original_order_num="",
                cancel_or_replace_volume=0,
                filled_price=0,
                filled_volume=0,
                buysell=BuySell.NA,
                broker="new_broker",
                strategy_id="S",
                client_order_id="CID",
                timestamp=None,
                event_id="evt-1",
                seq=1,
            )
        )

        await asyncio.wait_for(called.wait(), timeout=0.2)

        await executor.shutdown()

    asyncio.run(_main())
