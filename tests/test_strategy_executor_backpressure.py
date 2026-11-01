import asyncio

from trading_system.strategy.execution.strategy_executor import StrategyExecutor
from trading_system.types.events import FuturesSimpleQuote


def test_strategy_executor_drops_one_md_event_when_queue_is_full() -> None:
    """Backpressure 정책(QUOTE/FILLED): 큐가 full이면 1개 drop 후 최신 이벤트를 유지."""

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        executor = StrategyExecutor(loop=loop, quote_queue_size=1)

        received = []
        done = asyncio.Event()

        class _S:
            strategy_name = "S"

            async def on_quote(self, quote) -> None:
                received.append(quote.best_ask_price)
                # backpressure 정책상 drop이 발생할 수 있으므로
                # "최소 1회는 최신 이벤트가 전달된다"를 검증한다.
                if len(received) >= 1:
                    done.set()

            async def on_filled(self, filled) -> None:  # pragma: no cover
                return None

            async def on_ack(self, ack) -> None:  # pragma: no cover
                return None

        executor.register(strategy_id="S", strategy=_S())

        # consumer가 처리하기 전에(md_queue_size=1) 2개를 연달아 넣으면
        # 첫 이벤트가 drop되고 두 번째(최신)가 남는 것이 목표.
        executor.publish_quote_threadsafe(
            strategy_id="S",
            quote=FuturesSimpleQuote(
                inst_code="A01612",
                best_ask_price=101.0,
                best_bid_price=100.0,
                best_ask_volume=1,
                best_bid_volume=1,
            ),
        )
        executor.publish_quote_threadsafe(
            strategy_id="S",
            quote=FuturesSimpleQuote(
                inst_code="A01612",
                best_ask_price=102.0,
                best_bid_price=101.0,
                best_ask_volume=1,
                best_bid_volume=1,
            ),
        )

        await asyncio.wait_for(done.wait(), timeout=0.5)

        # 추가 이벤트가 처리될 수 있는 여지를 조금 준다.
        await asyncio.sleep(0.05)

        # 큐 사이즈(1)에서 연속 publish 시, 최신 이벤트(102.0)가 반드시 반영되어야 한다.
        assert received, "Expected at least one quote callback"
        assert received[-1] == 102.0

        await executor.shutdown()

    asyncio.run(_main())
