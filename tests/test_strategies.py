import asyncio

from trading_system.market_data.connectors.kis_master_loader import KISMasterLoader
from trading_system.strategy.concepts import cb_split_long_only_pct as cb_mod
from trading_system.strategy.concepts import minute_high_breakout as mhb_mod
from trading_system.strategy.concepts import vol_breaking_long as vbl_mod
from trading_system.types.enums import BuySell

from tests._fakes import ACCEPTED, REJECTED, FakeClock, FakeOrderService, tick

ACCOUNT = {"A01612": "12345678-03"}


# -----------------------------------------------------------------------------
# MinuteHighBreakoutStrategy
# -----------------------------------------------------------------------------
def test_minute_breakout_enters_on_breakout_and_reenters_after_loss_cut(monkeypatch) -> None:
    now = {"t": 0.0}
    monkeypatch.setattr(mhb_mod.time, "time", lambda: now["t"])

    async def _main() -> None:
        svc = FakeOrderService()
        stg = mhb_mod.MinuteHighBreakoutStrategy(
            "MHB", True, ["A01612"], [], ACCOUNT, 1, {},
            {"window_minutes": 2, "loss_cut_pct": 1.0, "take_profit_pct": 5.0, "exit_retry_cooldown_sec": 0},
            order_service=svc,
        )
        # 분봉 2개(고가 100, 101)를 완결시킨다.
        for minute, price in ((0, 100.0), (1, 101.0), (2, 100.5)):
            now["t"] = minute * 60
            await stg.on_filled(tick(price))
        assert svc.orders == []

        await stg.on_filled(tick(101.5))  # 최근 2분 최고가 101 상향 돌파
        assert [o["side"] for o in svc.orders] == [BuySell.BUY]
        assert stg.long_flag and stg.entry_price == 101.5

        await stg.on_filled(tick(100.4))  # -1.0% 이하 -> 손절
        assert [o["side"] for o in svc.orders] == [BuySell.BUY, BuySell.SELL]
        assert not stg.long_flag  # 청산 후 재진입 가능 상태로 리셋

    asyncio.run(_main())


def test_minute_breakout_rejected_entry_does_not_open_position(monkeypatch) -> None:
    now = {"t": 0.0}
    monkeypatch.setattr(mhb_mod.time, "time", lambda: now["t"])

    async def _main() -> None:
        svc = FakeOrderService(responses=[REJECTED])
        stg = mhb_mod.MinuteHighBreakoutStrategy(
            "MHB", True, ["A01612"], [], ACCOUNT, 1, {}, {"window_minutes": 1}, order_service=svc,
        )
        now["t"] = 0
        await stg.on_filled(tick(100.0))
        now["t"] = 60
        await stg.on_filled(tick(100.0))
        await stg.on_filled(tick(101.0))
        assert len(svc.orders) == 1
        assert not stg.long_flag

    asyncio.run(_main())


# -----------------------------------------------------------------------------
# CBSplitLongOnlyPct
# -----------------------------------------------------------------------------
CB_PARAMS = {
    "long_order_volume": 2,
    "long_order_num_divide": 2,
    "long_time_gap": 0,
    "loss_cut_pct": 1.0,
    "tick_size": 0.05,
    "channel_fix_time": "093000",
}


def _cb(svc, bullet):
    return cb_mod.CBSplitLongOnlyPct(
        "CB", True, ["A01612"], ["093000"], ACCOUNT, bullet, {}, CB_PARAMS, "kis", order_service=svc,
    )


def test_channel_breakout_loss_cut_runs_even_when_bullets_are_used_up(monkeypatch) -> None:
    clock = FakeClock("092900")
    monkeypatch.setattr(cb_mod, "datetime", clock)

    async def _main() -> None:
        svc = FakeOrderService()
        stg = _cb(svc, bullet=2)  # 분할 진입 2회가 bullet 2개를 모두 사용
        await stg.on_filled(tick(100.0, high=100.0))  # 채널 고가 100
        clock.set("093100")
        await stg.on_filled(tick(100.5, high=100.5))  # 채널 돌파 -> 분할 매수 2회
        assert [o["side"] for o in svc.orders] == [BuySell.BUY, BuySell.BUY]
        assert stg.bullet == 0 and stg.position == 2

        await stg.on_filled(tick(98.9, high=100.5))  # 채널 고가 대비 -1% 이하
        sells = [o for o in svc.orders if o["side"] == BuySell.SELL]
        assert sum(o["quantity"] for o in sells) == 2
        assert stg.position == 0

    asyncio.run(_main())


def test_channel_breakout_loss_cut_sells_only_accepted_entries(monkeypatch) -> None:
    clock = FakeClock("092900")
    monkeypatch.setattr(cb_mod, "datetime", clock)

    async def _main() -> None:
        svc = FakeOrderService(responses=[ACCEPTED, REJECTED])  # 두 번째 진입 거부
        stg = _cb(svc, bullet=2)
        await stg.on_filled(tick(100.0, high=100.0))
        clock.set("093100")
        await stg.on_filled(tick(100.5, high=100.5))
        assert stg.position == 1

        await stg.on_filled(tick(98.9, high=100.5))
        sells = [o for o in svc.orders if o["side"] == BuySell.SELL]
        assert sum(o["quantity"] for o in sells) == 1  # 목표 2계약이 아니라 실제 1계약만

    asyncio.run(_main())


# -----------------------------------------------------------------------------
# VolBreakingLong
# -----------------------------------------------------------------------------
class _FakeRestApi:
    async def get_future_price(self, symbol):
        return {"current_price": 900.0, "prev_close": 899.0}

    async def get_future_daily_ohlc(self, symbol, market_code="F"):
        return {"date": "20261030", "high": 910.0, "low": 890.0, "close": 899.0}


VB_PARAMS = {
    "c1_code": "A01612",
    "vol_size_check_time": "084500",
    "base_price_time": 90100,
    "long_threshold": 0.5,
    "order_volume": 1,
    "tick_size": 0.05,
    "long_order_num_divide": 1,
    "long_order_start": 90500,
    "long_order_end": 143000,
    "exit_order_volume": 1,
    "exit_order_num_divide": 1,
    "exit_order_start": 145000,
    "exit_order_end": 145500,
    "loss_cut_pct": 0.5,
}


def test_vol_breakout_long_uses_prev_day_range_and_order_service_api(monkeypatch) -> None:
    clock = FakeClock("090200")
    monkeypatch.setattr(vbl_mod, "datetime", clock)

    async def _main() -> None:
        loader = KISMasterLoader(_FakeRestApi(), request_spacing_sec=0)
        assert await loader.load(["A01612"])
        svc = FakeOrderService()
        # 진입 체크 슬롯 2개: 09:05:00, 11:47:30
        stg = vbl_mod.VolBreakingLong(
            "VB", True, ["A01612"], [], ACCOUNT, 1,
            {"last_days_ohlc_price": loader.as_prev_day_ohlc_feature()},
            {**VB_PARAMS, "long_order_num_divide": 2}, svc,
        )

        # 체크 시각(08:45)이 이미 지난 뒤 시작해도 vol_size를 계산해야 한다.
        await stg.on_clock("090200")
        assert stg.vol_size == 20.0  # 910 - 890

        await stg.on_filled(tick(900.0))
        await stg.on_filled(tick(900.0))  # base_price 900 확정
        assert stg.base_price == 900.0

        clock.set("090600")  # 첫 슬롯: 900 + 0.5 * 20 = 910 미만 -> 진입 안 함
        await stg.on_filled(tick(905.0))
        assert svc.orders == []

        clock.set("115000")  # 둘째 슬롯: 돌파 -> 매수 (첫 슬롯 미진입이 수량을 소모하지 않아야 함)
        await stg.on_filled(tick(910.5))
        assert len(svc.orders) == 1
        assert svc.orders[0]["side"] == BuySell.BUY and svc.orders[0]["quantity"] == 1
        assert stg.position == 1

    asyncio.run(_main())


# -----------------------------------------------------------------------------
# 장중 재시작
# -----------------------------------------------------------------------------
BARS = [
    {"time": "090000", "open": 880.0, "high": 884.0, "low": 879.0, "close": 883.0},
    {"time": "090100", "open": 883.5, "high": 886.0, "low": 883.0, "close": 885.0},
    {"time": "092900", "open": 890.0, "high": 897.0, "low": 889.0, "close": 896.0},
    {"time": "093000", "open": 896.0, "high": 899.0, "low": 895.0, "close": 898.0},  # 채널 고정 이후 봉
]


def _vb(svc, loader, bullet=1, **params):
    return vbl_mod.VolBreakingLong(
        "VB", True, ["A01612"], [], ACCOUNT, bullet,
        {"last_days_ohlc_price": loader.as_prev_day_ohlc_feature()}, {**VB_PARAMS, **params}, svc,
    )


def test_vol_breakout_late_start_restores_base_and_evaluates_only_latest_slot(monkeypatch) -> None:
    clock = FakeClock("123000")
    monkeypatch.setattr(vbl_mod, "datetime", clock)

    async def _main() -> None:
        loader = KISMasterLoader(_FakeRestApi(), request_spacing_sec=0)
        assert await loader.load(["A01612"])
        svc = FakeOrderService()
        stg = _vb(svc, loader, bullet=3, order_volume=3, long_order_num_divide=12)

        assert stg.intraday_restore_until(123000) == "090100"
        assert stg.restore_intraday(BARS) and stg.base_price == 883.5  # 09:01 봉 시가
        await stg.on_clock("123000")  # vol_size 20

        await stg.on_filled(tick(894.0))
        await stg.on_filled(tick(894.0))  # 883.5 + 0.5 * 20 = 893.5 돌파
        assert len(svc.orders) == 1  # 지난 슬롯 여러 개가 한 시점에 몰려 진입하지 않음
        assert sum(stg.long_order_sent_list) > 1 and stg.bullet == 2

    asyncio.run(_main())


def test_vol_breakout_late_start_without_restore_does_not_trade(monkeypatch) -> None:
    clock = FakeClock("123000")
    monkeypatch.setattr(vbl_mod, "datetime", clock)

    async def _main() -> None:
        loader = KISMasterLoader(_FakeRestApi(), request_spacing_sec=0)
        assert await loader.load(["A01612"])
        svc = FakeOrderService()
        stg = _vb(svc, loader)
        await stg.on_clock("123000")
        for _ in range(3):
            await stg.on_filled(tick(950.0))
        assert stg.base_price is None and svc.orders == []  # 12:30 가격을 기준가로 쓰지 않는다

    asyncio.run(_main())


def test_channel_breakout_late_start_restores_channel_from_minute_bars(monkeypatch) -> None:
    clock = FakeClock("103000")
    monkeypatch.setattr(cb_mod, "datetime", clock)

    async def _main() -> None:
        svc = FakeOrderService()
        stg = _cb(svc, bullet=2)
        assert stg.intraday_restore_until(103000) == "093000"
        assert stg.restore_intraday(BARS)
        assert stg.high_price == 897.0 and stg.low_price == 879.0  # 09:30 봉은 채널에 넣지 않음

        await stg.on_filled(tick(897.5, high=899.0))
        assert stg.long_flag and svc.orders and svc.orders[0]["side"] == BuySell.BUY

    asyncio.run(_main())
