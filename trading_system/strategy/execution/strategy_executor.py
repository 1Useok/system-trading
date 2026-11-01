from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Literal, Optional, Protocol, Union

from trading_system.types.events import FuturesSimpleFilled, FuturesSimpleOrderAck, FuturesSimpleQuote

logger = logging.getLogger(__name__)


# --- event wrappers (executor 내부 표준 이벤트) ---
@dataclass(frozen=True)
class QuoteEvent:
    type: Literal["quote"] = "quote"
    data: FuturesSimpleQuote = None  # type: ignore[assignment]


@dataclass(frozen=True)
class FilledEvent:
    type: Literal["filled"] = "filled"
    data: FuturesSimpleFilled = None  # type: ignore[assignment]


@dataclass(frozen=True)
class AckEvent:
    type: Literal["ack"] = "ack"
    data: FuturesSimpleOrderAck = None  # type: ignore[assignment]


StrategyEvent = Union[QuoteEvent, FilledEvent, AckEvent]


class AsyncStrategy(Protocol):
    strategy_id: str  # or strategy_name

    async def on_quote(self, quote: FuturesSimpleQuote) -> None: ...

    async def on_filled(self, filled: FuturesSimpleFilled) -> None: ...

    async def on_ack(self, ack: FuturesSimpleOrderAck) -> None: ...


@dataclass
class _StrategyRuntime:
    strategy: object
    md_queue: asyncio.Queue[StrategyEvent]
    ack_queue: asyncio.Queue[StrategyEvent]
    task: asyncio.Task
    enabled: bool = True
    consecutive_failures: int = 0


class StrategyExecutor:
    """전략 콜백 실행을 단일 asyncio 루프에서 보장하는 실행기.

    핵심 보장:
    - 전략별 단일 consumer task → 동일 전략 내부 순차 실행
    - ACK는 drop 금지(별도 큐)
    - Quote/Filled는 backpressure 시 drop/coalesce 가능(기본: 1개 drop 후 최신 유지)
    - Circuit breaker: 한 전략의 콜백이 연속 `max_consecutive_failures`회 예외를 내면 그 전략에는
      시세(Quote/Filled) 전달을 멈춰 새 신호를 차단한다. ACK는 계속 전달해 주문 상태는 맞춰 둔다.
    """

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        quote_queue_size: int = 1000,
        ack_queue_size: int = 0,
        max_consecutive_failures: int = 5,
    ):
        self._loop = loop
        self._max_consecutive_failures = max_consecutive_failures
        self._quote_queue_size = quote_queue_size
        self._ack_queue_size = ack_queue_size
        self._runtimes: Dict[str, _StrategyRuntime] = {}

    def register(self, *, strategy_id: str, strategy: object) -> None:
        if strategy_id in self._runtimes:
            raise ValueError(f"Duplicate strategy_id: {strategy_id}")

        md_queue: asyncio.Queue[StrategyEvent] = asyncio.Queue(maxsize=self._quote_queue_size)
        ack_queue: asyncio.Queue[StrategyEvent]
        if self._ack_queue_size and self._ack_queue_size > 0:
            ack_queue = asyncio.Queue(maxsize=self._ack_queue_size)
        else:
            ack_queue = asyncio.Queue()

        task = self._loop.create_task(self._consume(strategy_id=strategy_id))
        self._runtimes[strategy_id] = _StrategyRuntime(
            strategy=strategy,
            md_queue=md_queue,
            ack_queue=ack_queue,
            task=task,
        )

    def is_enabled(self, *, strategy_id: str) -> bool:
        """circuit breaker로 시세 전달이 차단되지 않았는지."""
        return self._get_runtime(strategy_id).enabled

    def is_registered(self, *, strategy_id: str) -> bool:
        return strategy_id in self._runtimes

    def get_registered_strategies(self) -> Dict[str, object]:
        """등록된 {strategy_id: strategy 인스턴스} 목록을 반환합니다.

        `on_clock()`을 실제 시각에 맞춰 호출해주는 스케줄러(main.py의 clock scheduler 등)처럼,
        전략별 콜백 실행 큐(내부 `_runtimes`)를 몰라도 되는 외부 순회 용도로 제공합니다.
        """
        return {sid: rt.strategy for sid, rt in self._runtimes.items()}

    async def shutdown(self) -> None:
        for rt in self._runtimes.values():
            rt.task.cancel()
        await asyncio.gather(*(rt.task for rt in self._runtimes.values()), return_exceptions=True)

    def _get_runtime(self, strategy_id: str) -> _StrategyRuntime:
        try:
            return self._runtimes[strategy_id]
        except KeyError as e:
            raise KeyError(f"Unknown strategy_id: {strategy_id}") from e

    # -------- publish (loop 내부) --------
    async def publish_quote(self, *, strategy_id: str, quote: FuturesSimpleQuote) -> None:
        self._put_with_quote_policy(strategy_id, QuoteEvent(data=quote))

    async def publish_filled(self, *, strategy_id: str, filled: FuturesSimpleFilled) -> None:
        self._put_with_quote_policy(strategy_id, FilledEvent(data=filled))

    async def publish_ack(self, *, strategy_id: str, ack: FuturesSimpleOrderAck) -> None:
        rt = self._get_runtime(strategy_id)
        await rt.ack_queue.put(AckEvent(data=ack))

    def _put_with_quote_policy(self, strategy_id: str, event: StrategyEvent) -> None:
        rt = self._get_runtime(strategy_id)
        try:
            rt.md_queue.put_nowait(event)
        except asyncio.QueueFull:
            # quote/filled 최신화를 위해 1개 drop 후 최신 이벤트를 넣는다.
            try:
                _ = rt.md_queue.get_nowait()
                rt.md_queue.task_done()
            except asyncio.QueueEmpty:
                pass
            rt.md_queue.put_nowait(event)
            logger.warning("StrategyExecutor: md queue full; dropped one event to keep latest (strategy_id=%s)", strategy_id)

    # -------- publish (loop 외부 스레드) --------
    def publish_quote_threadsafe(self, *, strategy_id: str, quote: FuturesSimpleQuote) -> None:
        self._loop.call_soon_threadsafe(self._put_with_quote_policy, strategy_id, QuoteEvent(data=quote))

    def publish_filled_threadsafe(self, *, strategy_id: str, filled: FuturesSimpleFilled) -> None:
        self._loop.call_soon_threadsafe(self._put_with_quote_policy, strategy_id, FilledEvent(data=filled))

    def publish_ack_threadsafe(self, *, strategy_id: str, ack: FuturesSimpleOrderAck) -> None:
        # ACK는 drop 금지이므로 루프 스레드에서 코루틴을 스케줄한다.
        def _schedule() -> None:
            asyncio.create_task(self.publish_ack(strategy_id=strategy_id, ack=ack))

        self._loop.call_soon_threadsafe(_schedule)

    async def _consume(self, *, strategy_id: str) -> None:
        rt = self._get_runtime(strategy_id)
        strategy = rt.strategy

        while True:
            # ACK 우선 처리(정합성)
            if not rt.ack_queue.empty():
                event = await rt.ack_queue.get()
                try:
                    await self._dispatch(strategy_id=strategy_id, strategy=strategy, event=event)
                finally:
                    rt.ack_queue.task_done()
                continue

            # ACK/MD 중 먼저 도착한 이벤트 처리
            ack_task = self._loop.create_task(rt.ack_queue.get())
            md_task = self._loop.create_task(rt.md_queue.get())

            done, pending = await asyncio.wait(
                {ack_task, md_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for p in pending:
                p.cancel()

            # 두 get()이 같은 틱에 모두 완료될 수 있다. 이때 하나만 처리하면 이미 큐에서
            # 꺼낸 다른 이벤트(ACK일 수도 있음)가 조용히 유실되므로, 완료된 것을 모두
            # 처리한다. 정합성을 위해 ACK를 먼저 처리한다.
            events = sorted(
                (t.result() for t in done),
                key=lambda e: 0 if isinstance(e, AckEvent) else 1,
            )
            for event in events:
                try:
                    await self._dispatch(strategy_id=strategy_id, strategy=strategy, event=event)
                finally:
                    if isinstance(event, AckEvent):
                        rt.ack_queue.task_done()
                    else:
                        rt.md_queue.task_done()

    async def _dispatch(self, *, strategy_id: str, strategy: object, event: StrategyEvent) -> None:
        rt = self._runtimes.get(strategy_id)
        if rt is not None and not rt.enabled and not isinstance(event, AckEvent):
            return  # circuit open: 새 신호의 근거가 되는 시세는 전달하지 않는다
        try:
            if isinstance(event, QuoteEvent):
                await strategy.on_quote(event.data)
            elif isinstance(event, FilledEvent):
                await strategy.on_filled(event.data)
            elif isinstance(event, AckEvent):
                await strategy.on_ack(event.data)
            else:  # pragma: no cover
                logger.warning("StrategyExecutor: unknown event=%s (strategy_id=%s)", event, strategy_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("StrategyExecutor: strategy callback failed (strategy_id=%s, event=%s)", strategy_id, getattr(event, "type", "?"))
            if rt is not None:
                rt.consecutive_failures += 1
                if rt.enabled and rt.consecutive_failures >= self._max_consecutive_failures:
                    rt.enabled = False
                    logger.critical(
                        "StrategyExecutor: circuit open for %s after %d consecutive failures - "
                        "market data delivery stopped (ACKs still delivered)",
                        strategy_id, rt.consecutive_failures,
                    )
        else:
            if rt is not None:
                rt.consecutive_failures = 0
