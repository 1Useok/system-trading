"""KIS 실시간(WebSocket) 메시지 해석과 체결통보 -> ACK 변환 테스트.

필드 순서는 KIS 공식 샘플(open-trading-api)의 컬럼 정의를 그대로 따른다.
"""
import asyncio
import base64

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from trading_system.api.kis.rest_streamer import RestStreamer
from trading_system.execution_report.connectors.kis_ack_connector import KISAckConnector
from trading_system.execution_report.order_registry import OrderRegistry
from trading_system.types.enums import AckType, BuySell, QuoteType

AES_KEY = "k" * 32
AES_IV = "i" * 16

NOTICE_COLUMNS = [
    "cust_id", "acnt_no", "oder_no", "ooder_no", "seln_byov_cls", "rctf_cls", "oder_kind2",
    "stck_shrn_iscd", "cntg_qty", "cntg_unpr", "stck_cntg_hour", "rfus_yn", "cntg_yn", "acpt_yn",
    "brnc_no", "oder_qty", "acnt_name", "cntg_isnm", "oder_cond", "ord_grp", "ord_grpseq", "order_prc",
]


class _TokenManager:
    def __init__(self, svr):
        self.svr = svr


class _Router:
    def __init__(self):
        self.acks = []

    def on_ack(self, ack):
        self.acks.append(ack)


def _notice(**values) -> str:
    base = {c: "" for c in NOTICE_COLUMNS}
    base.update(cust_id="hts01", acnt_no="1234567803", seln_byov_cls="02", stck_shrn_iscd="A01612",
                stck_cntg_hour="093015", rfus_yn="0", oder_qty="2")
    base.update(values)
    return "^".join(base[c] for c in NOTICE_COLUMNS)


def _encrypt(plain: str) -> str:
    cipher = AES.new(AES_KEY.encode(), AES.MODE_CBC, AES_IV.encode())
    return base64.b64encode(cipher.encrypt(pad(plain.encode(), AES.block_size))).decode()


def _streamer_with_callback(callback, svr="vps") -> RestStreamer:
    streamer = RestStreamer(_TokenManager(svr))
    streamer._callbacks["hts01_execution"] = callback
    streamer._maybe_store_aes_key({"header": {"tr_id": "H0IFCNI0"}, "body": {"output": {"key": AES_KEY, "iv": AES_IV}}})
    return streamer


# -----------------------------------------------------------------------------
# RestStreamer
# -----------------------------------------------------------------------------
def test_streamer_selects_websocket_endpoint_by_server() -> None:
    assert RestStreamer(_TokenManager("vps")).ws_url.endswith(":31000")
    assert RestStreamer(_TokenManager("prod")).ws_url.endswith(":21000")


def test_streamer_subscribes_both_execution_tr_ids_on_mock_server() -> None:
    assert RestStreamer(_TokenManager("vps"))._tr_ids_for(QuoteType.EXECUTION) == ("H0IFCNI0", "H0IFCNI9")
    assert RestStreamer(_TokenManager("prod"))._tr_ids_for(QuoteType.EXECUTION) == ("H0IFCNI0",)


def test_streamer_decrypts_and_parses_fill_notice_per_spec() -> None:
    got = []
    streamer = _streamer_with_callback(got.append)
    # 선물 체결단가는 소수점 없이 0.01 단위로 온다(모의투자 실측: 923.85 -> "92385")
    plain = _notice(oder_no="0000123", oder_kind2="0", cntg_qty="1", cntg_unpr="89945", cntg_yn="2", acpt_yn="2")
    streamer._handle_execution_notice("1", "H0IFCNI0", _encrypt(plain))

    assert len(got) == 1
    d = got[0]
    assert d["order_num"] == "0000123" and d["symbol"] == "A01612"
    assert d["order_kind"] == "0" and d["qty"] == 1 and d["price"] == 899.45
    assert d["filled_time"] == "093015" and d["order_qty"] == 2 and d["buysell"] == "02"


def test_streamer_parses_best_quote_volumes_from_remaining_qty_fields() -> None:
    # 종목코드를 뗀 뒤: 0 시각, 1~5 매도호가, 6~10 매수호가, 11~20 호가건수, 21~30 호가잔량
    fields = ["090000"] + [str(900 + i) for i in range(5)] + [str(899 - i) for i in range(5)]
    fields += ["7"] * 10 + ["30", "31", "32", "33", "34", "40", "41", "42", "43", "44"]
    parsed = RestStreamer(_TokenManager("prod"))._parse_realtime_data("H0IFASP0", "A01612", "^".join(fields))
    assert parsed["best_ask_price"] == 900 and parsed["best_bid_price"] == 899
    assert parsed["best_ask_volume"] == 30 and parsed["best_bid_volume"] == 40


# -----------------------------------------------------------------------------
# KISAckConnector
# -----------------------------------------------------------------------------
def _connector(registry=None, rest_api=None):
    registry = registry or OrderRegistry()
    connector = KISAckConnector(streamer=None, hts_id="hts01", order_registry=registry,
                                rest_api=rest_api, account="12345678-03")
    router = _Router()
    connector.set_manager(router)
    return connector, router, registry


def _feed(connector, plain: str, tr_id: str = "H0IFCNI0") -> None:
    streamer = _streamer_with_callback(connector._on_execution)
    streamer._aes_key[tr_id], streamer._aes_iv[tr_id] = AES_KEY, AES_IV
    streamer._handle_execution_notice("1", tr_id, _encrypt(plain))


def test_ack_classification_follows_order_kind_and_correction_flags() -> None:
    connector, router, registry = _connector()
    registry.register(client_order_id="CID-1", strategy_id="MinHighBO")
    registry.register_order_no(order_no="0000123", client_order_id="CID-1")

    _feed(connector, _notice(oder_no="0000123", oder_kind2="L", rctf_cls="0", cntg_qty="2", acpt_yn="1"))
    _feed(connector, _notice(oder_no="0000123", oder_kind2="0", cntg_qty="1", cntg_unpr="89945"))
    _feed(connector, _notice(oder_no="0000124", ooder_no="0000123", oder_kind2="L", rctf_cls="2", cntg_qty="1"))
    _feed(connector, _notice(oder_no="0000125", oder_kind2="L", rfus_yn="1", cntg_qty="1"))

    kinds = [a.ack_type for a in router.acks]
    assert kinds == [AckType.Received, AckType.Filled, AckType.Canceled, AckType.Rejected]
    received, filled, canceled, _ = router.acks
    assert received.filled_volume == 0  # 접수 통보의 수량은 체결수량이 아님
    assert filled.filled_volume == 1 and filled.filled_price == 899.45 and filled.buysell == BuySell.BUY
    assert filled.strategy_id == "MinHighBO" and filled.client_order_id == "CID-1"
    assert canceled.cancel_or_replace_volume == 1
    assert canceled.strategy_id == "MinHighBO"  # 취소는 새 주문번호로 오지만 원주문번호로 전략을 찾는다


def test_same_notice_on_both_channels_is_applied_once() -> None:
    connector, router, _ = _connector()
    plain = _notice(oder_no="0000200", oder_kind2="0", cntg_qty="1", cntg_unpr="900")
    _feed(connector, plain, "H0IFCNI0")
    _feed(connector, plain, "H0IFCNI9")
    assert len(router.acks) == 1


class _OrderHistoryApi:
    def __init__(self, rows):
        self.rows = rows

    async def inquire_order_history(self, account, order_status="02", raise_on_error=False):
        return self.rows


def test_reconcile_after_reconnect_fills_only_the_missing_quantity() -> None:
    rows = [
        {"odno": "0000300", "orgn_odno": "", "pdno": "A01612", "sll_buy_dvsn_cd": "02",
         "tot_ccld_qty": "3", "avg_idx": "900.10"},
        {"odno": "9999999", "pdno": "A01612", "sll_buy_dvsn_cd": "01", "tot_ccld_qty": "5", "avg_idx": "901"},  # 남의 주문
    ]
    connector, router, registry = _connector(rest_api=_OrderHistoryApi(rows))
    registry.register(client_order_id="CID-3", strategy_id="VolBO_Long")
    registry.register_order_no(order_no="0000300", client_order_id="CID-3")

    _feed(connector, _notice(oder_no="0000300", oder_kind2="0", cntg_qty="1", cntg_unpr="900"))
    corrected = asyncio.run(connector.reconcile_via_rest())

    assert corrected == 1
    gap = router.acks[-1]
    assert gap.ack_type == AckType.Filled and gap.filled_volume == 2 and gap.filled_price == 900.10
    assert gap.strategy_id == "VolBO_Long"
    assert asyncio.run(connector.reconcile_via_rest()) == 0  # 이미 보정됨


def test_reconcile_matches_rest_order_numbers_padded_with_spaces() -> None:
    rows = [{"odno": "       729", "orgn_odno": "", "pdno": "A01612", "sll_buy_dvsn_cd": "02",
             "tot_ccld_qty": "1", "avg_idx": "856.80"}]
    connector, router, registry = _connector(rest_api=_OrderHistoryApi(rows))
    registry.register(client_order_id="CID-7", strategy_id="LiveTest")
    registry.register_order_no(order_no="0000000729", client_order_id="CID-7")

    _feed(connector, _notice(oder_no="0000000729", oder_kind2="0", cntg_qty="1", cntg_unpr="856.80"))
    assert asyncio.run(connector.reconcile_via_rest()) == 0  # 통보로 받은 체결을 REST 행과 같은 주문으로 인식


def test_market_data_connector_subscribes_trades_and_order_book() -> None:
    from trading_system.market_data.connectors.kis_connector import KISConnector

    class _Streamer:
        def __init__(self):
            self.subs = []

        async def subscribe(self, symbol, quote_type=QuoteType.TICK, callback=None):
            self.subs.append((symbol, quote_type))
            return True

    connector = KISConnector.__new__(KISConnector)
    connector.streamer, connector.subscribed_symbols, connector.manager = _Streamer(), set(), None
    asyncio.run(connector.start("A01612"))
    assert connector.streamer.subs == [("A01612", QuoteType.TICK), ("A01612", QuoteType.ORDERBOOK)]
