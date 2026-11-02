"""Kill Switch / RiskManager / 재시작 복구 상태 테스트."""
import asyncio
from datetime import datetime, timedelta, timezone

import pandas as pd

from trading_system.execution_report.connectors.new_broker_connector import NewBrokerConnector
from trading_system.execution_report.order_registry import OrderRegistry
from trading_system.order.brokers.kis_driver import KISDriver
from trading_system.order.order_manager import OrderManager
from trading_system.risk.kill_switch import KillSwitch
from trading_system.risk.risk_manager import RiskManager
from trading_system.types.enums import BuySell, PriceType
from trading_system.utils.clock import now_kst

ACCOUNT = "12345678-03"


class _Driver:
    def __init__(self):
        self.orders, self.cancels = [], []
        self.balance = (pd.DataFrame(), {})

    async def order_future(self, **params):
        self.orders.append(params)
        return {"rt_cd": "0", "output": {"ODNO": f"{len(self.orders):07d}"}}

    async def cancel_future_order(self, **params):
        self.cancels.append(params)
        return {"rt_cd": "0"}

    async def get_future_account_balance(self, **params):
        return self.balance


def _order_service(kill_switch=None):
    manager = OrderManager({}, kill_switch=kill_switch)
    driver = _Driver()
    manager.drivers["fake"] = driver
    return manager.for_broker("fake", strategy_id="S", order_registry=OrderRegistry()), driver, manager


def _order(svc, side=BuySell.BUY, **kw):
    return asyncio.run(svc.place_future_order(symbol="A01612", quantity=1, price=900.0, side=side,
                                              price_type=PriceType.LIMIT, account=ACCOUNT, **kw))


# -----------------------------------------------------------------------------
# Kill Switch
# -----------------------------------------------------------------------------
def test_kill_switch_blocks_new_orders_but_lets_risk_reducing_orders_through() -> None:
    svc, driver, manager = _order_service()
    assert _order(svc)["rt_cd"] == "0"

    manager.kill_switch.activate("test")
    blocked = _order(svc)
    assert blocked["rt_cd"] != "0" and len(driver.orders) == 1  # 브로커로 나가지 않음

    assert _order(svc, side=BuySell.SELL, risk_reducing=True)["rt_cd"] == "0"
    assert len(driver.orders) == 2
    assert "risk_reducing" not in driver.orders[-1]  # 내부 플래그는 브로커로 넘기지 않음


class _OpenOrdersApi:
    def __init__(self, rows=None, fail=False):
        self.rows, self.fail = rows or [], fail

    async def inquire_order_history(self, account, order_status="02", raise_on_error=False):
        if self.fail:
            raise RuntimeError("rate limited")
        return self.rows


def _risk(svc, manager, api, **kw):
    return RiskManager(manager.kill_switch, svc, api, ACCOUNT, **kw)


def test_trigger_blocks_cancels_open_orders_and_flattens_positions() -> None:
    svc, driver, manager = _order_service()
    driver.balance = (pd.DataFrame([{"shtn_pdno": "A01612", "sll_buy_dvsn_name": "매수", "cblc_qty": "2"}]), {})
    api = _OpenOrdersApi(rows=[
        {"odno": "0000010", "pdno": "A01612", "qty": "1"},
        {"odno": "0000011", "pdno": "A01612", "qty": "0"},  # 잔량 없음 -> 취소 대상 아님
    ])
    risk = _risk(svc, manager, api, flatten_on_kill=True)

    asyncio.run(risk.trigger("test"))

    assert manager.kill_switch.active
    assert [c["order_id"] for c in driver.cancels] == ["0000010"]
    assert driver.orders[-1]["side"] == BuySell.SELL and driver.orders[-1]["quantity"] == 2
    assert driver.orders[-1]["price_type"] == PriceType.MARKET


def test_trigger_still_blocks_orders_when_open_order_query_fails() -> None:
    svc, driver, manager = _order_service()
    risk = _risk(svc, manager, _OpenOrdersApi(fail=True), flatten_on_kill=False)
    asyncio.run(risk.trigger("test"))
    assert manager.kill_switch.active and _order(svc)["rt_cd"] != "0"


def test_loss_limit_triggers_kill_switch() -> None:
    svc, _, manager = _order_service()
    risk = _risk(svc, manager, _OpenOrdersApi(), max_loss=1_000_000, flatten_on_kill=False)
    asyncio.run(risk.on_nav({"total_profit_loss": -999_999.0}))
    assert not manager.kill_switch.active
    asyncio.run(risk.on_nav({"total_profit_loss": -1_000_000.0}))
    assert manager.kill_switch.active


def test_kill_file_triggers_kill_switch(tmp_path) -> None:
    svc, _, manager = _order_service()
    kill_file = tmp_path / "KILL"
    risk = _risk(svc, manager, _OpenOrdersApi(), flatten_on_kill=False, kill_file=str(kill_file))

    async def _main():
        task = asyncio.create_task(risk.watch_kill_file(interval_sec=0.01))
        await asyncio.sleep(0.03)
        assert not manager.kill_switch.active
        kill_file.write_text("stop")
        await asyncio.wait_for(task, timeout=1)

    asyncio.run(_main())
    assert manager.kill_switch.active


def test_kis_driver_cancels_by_broker_order_number() -> None:
    calls = []

    class _Client:
        async def cancel_future_order(self, **kw):
            calls.append(kw)
            return {"rt_cd": "0"}

    driver = KISDriver.__new__(KISDriver)
    driver.client = _Client()
    asyncio.run(driver.cancel_future_order(account=ACCOUNT, order_id="0000010", client_order_id="CID", quantity=0))
    assert calls == [{"account": ACCOUNT, "original_order_no": "0000010", "symbol": "", "quantity": 0}]


# -----------------------------------------------------------------------------
# 재시작 복구 상태
# -----------------------------------------------------------------------------
def test_order_registry_restores_mappings_after_restart(tmp_path) -> None:
    first = OrderRegistry.for_trading_day(str(tmp_path))
    first.register(client_order_id="CID-1", strategy_id="MinHighBO")
    first.register_order_no(order_no="0000123", client_order_id="CID-1")

    restarted = OrderRegistry.for_trading_day(str(tmp_path))
    assert restarted.resolve_client_order_id_by_order_no("0000123") == "CID-1"
    assert restarted.resolve_strategy_id("CID-1") == "MinHighBO"


def test_execution_report_checkpoint_survives_restart(tmp_path) -> None:
    path = str(tmp_path / "ckpt.json")
    report = {"event_id": "evt-1", "seq": 7, "status": "FILL", "side": "BUY", "symbol": "A01612",
              "account": ACCOUNT, "broker": "new_broker", "last_fill_qty": 1, "last_fill_price": 900}

    first = NewBrokerConnector(checkpoint_path=path)
    assert first.ingest_execution_report(report) is not None

    restarted = NewBrokerConnector(checkpoint_path=path)
    assert restarted.resume_seq(ACCOUNT) == 7
    assert restarted.ingest_execution_report(report) is None  # 재전송된 같은 이벤트는 무시
    assert restarted.ingest_execution_report({**report, "event_id": "evt-0", "seq": 6}) is None  # 역순
    assert restarted.ingest_execution_report({**report, "event_id": "evt-2", "seq": 8}) is not None


def test_clock_is_fixed_to_kst_regardless_of_os_timezone() -> None:
    expected = datetime.now(timezone.utc) + timedelta(hours=9)
    assert abs((now_kst() - expected.replace(tzinfo=None)).total_seconds()) < 2


def test_kis_future_order_keeps_decimal_price() -> None:
    from trading_system.api.kis.rest_api import RestApi

    sent = {}

    async def _request_data(tr_id, params, method="POST"):
        sent.update(params)
        return {"rt_cd": "0", "output": {"ODNO": "0000001"}}

    api = RestApi.__new__(RestApi)
    api.request_data = _request_data
    asyncio.run(api.order_future(account=ACCOUNT, symbol="A01612", quantity=1, price=905.15,
                                 side=BuySell.BUY, price_type=PriceType.LIMIT))
    assert sent["UNIT_PRICE"] == "905.15"  # 0.05pt 호가단위: 소수점을 버리면 905.00으로 나간다
    assert sent["SLL_BUY_DVSN_CD"] == "02"


def test_kis_request_retries_when_rate_limited(monkeypatch) -> None:
    import trading_system.api.kis.rest_api as rest_api_module
    from trading_system.api.kis.rest_api import RestApi

    replies = [{"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."},
               {"rt_cd": "0", "output": {"ODNO": "0000001"}}]
    calls = []

    async def _request_once(tr_id, params, method="POST"):
        calls.append(tr_id)
        return replies[len(calls) - 1]

    monkeypatch.setattr(rest_api_module, "RATE_LIMIT_BACKOFF_SEC", 0)
    api = RestApi.__new__(RestApi)
    api._request_once = _request_once
    assert asyncio.run(api.request_data("VTTO1103U", {}))["rt_cd"] == "0"
    assert calls == ["VTTO1103U", "VTTO1103U"]


def test_cancel_all_skips_pending_cancel_orders_and_counts_kis_cancel_success() -> None:
    svc, driver, manager = _order_service()

    async def _cancel(**params):
        driver.cancels.append(params)
        return {"success": True, "order_no": "0000000760"}  # KIS 취소 응답 형태

    driver.cancel_future_order = _cancel
    api = _OpenOrdersApi(rows=[
        {"odno": "       729", "pdno": "A01612", "qty": "1", "trad_dvsn_name": "매수"},
        {"odno": "       760", "orgn_odno": "       729", "pdno": "A01612", "qty": "1", "trad_dvsn_name": "매수취소"},
    ])
    assert asyncio.run(_risk(svc, manager, api).cancel_all()) == 1
    assert [c["order_id"] for c in driver.cancels] == ["729"]


def test_order_numbers_match_across_kis_formats() -> None:
    registry = OrderRegistry()
    registry.register(client_order_id="CID-9", strategy_id="S")
    registry.register_order_no(order_no="0000000729", client_order_id="CID-9")  # 주문 응답 / 체결통보 형식
    assert registry.resolve_client_order_id_by_order_no("       729") == "CID-9"  # 주문체결내역 조회 형식


def test_blocked_strategy_cannot_enter_but_can_exit_and_others_trade() -> None:
    svc, driver, manager = _order_service()
    other = manager.for_broker("fake", strategy_id="Other", order_registry=OrderRegistry())
    manager.kill_switch.block_entries("S", "existing position on restart")

    assert _order(svc)["rt_cd"] != "0"
    assert _order(svc, side=BuySell.SELL, risk_reducing=True)["rt_cd"] == "0"
    assert _order(other)["rt_cd"] == "0"
    assert len(driver.orders) == 2 and not manager.kill_switch.active


def test_restart_with_unrestorable_position_blocks_that_strategy() -> None:
    import main
    from types import SimpleNamespace

    def _run(positions, restorable=False):
        ks = KillSwitch()
        mgr = SimpleNamespace(order_manager=SimpleNamespace(kill_switch=ks))
        main._guard_unrestorable_position(mgr, SimpleNamespace(strategy_name="VolBO_Long"), "A01612",
                                          positions, restorable)
        return ks.is_blocked("VolBO_Long")

    assert _run([("A01612", 1)])
    assert _run(None)  # 계좌 상태를 모르면 막는다
    assert not _run([])
    assert not _run([("A05603", -1)])  # 다른 종목
    assert not _run([("A01612", 1)], restorable=True)  # 포지션을 이어받는 전략


def test_streamer_retries_flaky_handshake(monkeypatch) -> None:
    import trading_system.api.kis.rest_streamer as rs

    attempts = []

    async def _connect(url):
        attempts.append(url)
        if len(attempts) == 1:
            raise ConnectionError("did not receive a valid HTTP response")
        return object()

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(rs.websockets, "connect", _connect)
    streamer = rs.RestStreamer(type("TM", (), {"svr": "vps"})())
    streamer.CONNECT_RETRY_SEC = 0
    monkeypatch.setattr(streamer, "_authenticate", _noop)
    monkeypatch.setattr(streamer, "_message_handler", _noop)

    async def _main():
        return await streamer.connect()

    assert asyncio.run(_main()) and len(attempts) == 2
