import asyncio

from trading_system.strategy.execution.strategy_executor import StrategyExecutor
from trading_system.types.enums import AckType
from trading_system.types.events import FuturesSimpleOrderAck, FuturesSimpleQuote


def test_strategy_executor_prioritizes_ack_over_quote_when_both_queued() -> None:
    async def _main() -> None:
        loop = asyncio.get_running_loop()
        executor = StrategyExecutor(loop=loop)

        events = []
        done = asyncio.Event()

        class _S:
            strategy_name = "S"

            async def on_quote(self, quote) -> None:
                events.append("quote")
                if len(events) >= 2:
                    done.set()

            async def on_filled(self, filled) -> None:  # pragma: no cover
                return None

            async def on_ack(self, ack) -> None:
                events.append("ack")
                if len(events) >= 2:
                    done.set()

        s = _S()
        executor.register(strategy_id="S", strategy=s)

        # consumer task가 실행되기 전에(=yield 전) 두 이벤트를 모두 큐에 넣는다.
        await executor.publish_quote(
            strategy_id="S",
            quote=FuturesSimpleQuote(
                inst_code="A01612",
                best_ask_price=101.0,
                best_bid_price=100.0,
                best_ask_volume=1,
                best_bid_volume=1,
            ),
        )
        await executor.publish_ack(
            strategy_id="S",
            ack=FuturesSimpleOrderAck(
                ack_type=AckType.Received,
                account="ACC",
                inst_code="A01612",
                order_num="OID",
                strategy_id="S",
                client_order_id="CID",
            ),
        )

        await asyncio.wait_for(done.wait(), timeout=0.2)
        assert events[:2] == ["ack", "quote"]

        await executor.shutdown()

    asyncio.run(_main())
