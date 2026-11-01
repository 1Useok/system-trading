"""체결/안전장치 경로 회귀 테스트: Executor 동시 도착, 장마감 청산 교차검증, 시작 시 포지션 점검."""
import asyncio
from types import SimpleNamespace

import pandas as pd

import main
from trading_system.strategy.concepts.krx_exit_all import KrxExitAll
from trading_system.strategy.execution.strategy_executor import AckEvent, FilledEvent, StrategyExecutor
from trading_system.types.enums import AckType, BuySell
from trading_system.types.events import FuturesSimpleOrderAck

from tests._fakes import FakeOrderService, tick

RATE_LIMITED = {"is_orderable": False, "error": "초당 거래건수를 초과하였습니다.",
                "total_possible_qty": 0, "order_possible_qty": 0}


def _availability(liquidation_qty: int) -> dict:
    # 플랫 계좌에서도 total_possible_qty(신규 주문 가능 포함)는 0보다 크다(실측: 2).
    return {"is_orderable": True, "total_possible_qty": 2,
            "liquidation_possible_qty": liquidation_qty, "order_possible_qty": 2}


# -----------------------------------------------------------------------------
# StrategyExecutor
# -----------------------------------------------------------------------------
def test_executor_processes_ack_and_market_data_that_arrive_in_the_same_tick() -> None:
    async def _main() -> None:
        executor = StrategyExecutor(loop=asyncio.get_running_loop())
        events = []

        class _S:
            async def on_quote(self, quote):  # pragma: no cover
                pass

            async def on_filled(self, filled):
                events.append("filled")

            async def on_ack(self, ack):
                events.append("ack")

        executor.register(strategy_id="S", strategy=_S())
        for _ in range(3):  # consumer가 두 큐를 동시에 기다리는 상태로 만든다
            await asyncio.sleep(0)

        rt = executor._get_runtime("S")
        # 같은 틱에 두 큐에 동시에 넣으면 두 get()이 함께 완료된다.
        rt.md_queue.put_nowait(FilledEvent(data=tick(100.0)))
        rt.ack_queue.put_nowait(AckEvent(data=FuturesSimpleOrderAck(ack_type=AckType.Filled, strategy_id="S")))
        for _ in range(10):
            await asyncio.sleep(0)

        assert sorted(events) == ["ack", "filled"]  # 어느 쪽도 유실되지 않아야 함
        assert events[0] == "ack"  # ACK 우선
        await executor.shutdown()

    asyncio.run(_main())


# -----------------------------------------------------------------------------
# KrxExitAll
# -----------------------------------------------------------------------------
class _ExitOrderService(FakeOrderService):
    def __init__(self, balance_df):
        super().__init__()
        self._balance = balance_df

    async def get_future_account_balance(self, account):
        return self._balance, {}


class _RestApi:
    def __init__(self, responses):
        self.responses = list(responses)

    async def check_future_order_availability(self, **kwargs):
        return self.responses.pop(0)


def _exit_all(svc, rest_api):
    return KrxExitAll(
        "ExitAll", True, ["A01612"], ["150000"], {"A01612": "12345678-03"}, 0, {},
        {"exit_time": "150000", "exit_retry_cooldown_sec": 0, "cross_check_spacing_sec": 0},
        "kis", rest_api=rest_api, order_service=svc,
    )


def test_exit_all_flattens_positions_even_with_zero_bullets() -> None:
    async def _main() -> None:
        df = pd.DataFrame([{"shtn_pdno": "A01612", "sll_buy_dvsn_name": "매수", "cblc_qty": "2"}])
        svc = _ExitOrderService(df)
        stg = _exit_all(svc, _RestApi([]))
        await stg.on_clock("150001")
        assert svc.orders == [{"symbol": "A01612", "quantity": 2, "price": 0.0, "side": BuySell.SELL}]
        assert stg.exit_done

    asyncio.run(_main())


def test_exit_all_treats_flat_account_as_done_using_liquidation_qty() -> None:
    async def _main() -> None:
        stg = _exit_all(_ExitOrderService(pd.DataFrame()), _RestApi([_availability(0)]))
        await stg.on_clock("150001")
        assert stg.exit_done  # total_possible_qty=2 를 보유로 오인하면 여기서 재시도 상태가 된다

    asyncio.run(_main())


def test_exit_all_retries_when_cross_check_fails() -> None:
    async def _main() -> None:
        stg = _exit_all(_ExitOrderService(pd.DataFrame()), _RestApi([RATE_LIMITED]))
        await stg.on_clock("150001")
        assert not stg.exit_done  # 확인 실패를 "포지션 없음"으로 넘기지 않는다

    asyncio.run(_main())


# -----------------------------------------------------------------------------
# main._reconcile_positions_on_startup
# -----------------------------------------------------------------------------
class _StartupApi:
    def __init__(self, availability):
        self.availability = list(availability)
        self.calls = 0

    async def get_future_account_balance(self, account):
        return pd.DataFrame(), {"total_deposit": 1.0}

    async def check_future_order_availability(self, **kwargs):
        self.calls += 1
        return self.availability.pop(0)


def _run_startup(availability, monkeypatch):
    monkeypatch.setattr(main, "AVAILABILITY_CHECK_SPACING_SEC", 0)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(main.asyncio, "sleep", lambda s: real_sleep(0))  # 5초 대기 생략
    api = _StartupApi(availability)
    stg = SimpleNamespace(inst_code="A01612", strategy_name="MHB", halted=False,
                          long_flag=False, position_closed=False, entry_price=0.0)
    asyncio.run(main._reconcile_positions_on_startup(SimpleNamespace(rest_api=api), "12345678-03", stg))
    return stg, api


def test_startup_flat_account_is_not_halted(monkeypatch) -> None:
    stg, api = _run_startup([_availability(0)], monkeypatch)
    assert not stg.halted and api.calls == 1


def test_startup_retries_once_after_rate_limit(monkeypatch) -> None:
    stg, api = _run_startup([RATE_LIMITED, _availability(0)], monkeypatch)
    assert not stg.halted and api.calls == 2


def test_startup_halts_when_cross_check_keeps_failing(monkeypatch) -> None:
    stg, _ = _run_startup([RATE_LIMITED, RATE_LIMITED], monkeypatch)
    assert stg.halted


def test_startup_halts_when_hidden_position_is_reported_twice(monkeypatch) -> None:
    stg, _ = _run_startup([_availability(1), _availability(1)], monkeypatch)
    assert stg.halted


def test_startup_ignores_hidden_position_reported_only_once(monkeypatch) -> None:
    stg, _ = _run_startup([_availability(1), _availability(0)], monkeypatch)
    assert not stg.halted


def test_executor_circuit_breaker_stops_market_data_but_keeps_acks() -> None:
    async def _main() -> None:
        executor = StrategyExecutor(loop=asyncio.get_running_loop(), max_consecutive_failures=3)
        seen = []

        class _Broken:
            async def on_quote(self, quote):  # pragma: no cover
                pass

            async def on_filled(self, filled):
                seen.append("filled")
                raise RuntimeError("bug in strategy")

            async def on_ack(self, ack):
                seen.append("ack")

        executor.register(strategy_id="S", strategy=_Broken())
        for _ in range(5):
            await executor.publish_filled(strategy_id="S", filled=tick(100.0))
            for _ in range(5):
                await asyncio.sleep(0)
        await executor.publish_ack(strategy_id="S", ack=FuturesSimpleOrderAck(ack_type=AckType.Filled, strategy_id="S"))
        for _ in range(5):
            await asyncio.sleep(0)

        assert seen == ["filled"] * 3 + ["ack"]  # 3회 연속 실패 후 시세 차단, ACK는 전달
        assert not executor.is_enabled(strategy_id="S")
        await executor.shutdown()

    asyncio.run(_main())
