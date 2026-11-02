"""실행 진입점 (KIS 모의투자 / KOSPI200 선물).

파이프라인:
    시세 수신(REST 폴링, 선택적으로 WebSocket) -> MarketDataHub -> StrategyExecutor -> 전략
    전략 -> OrderService -> OrderManager -> KISDriver(REST 주문)
    체결통보(WebSocket H0IFCNI0) -> KISAckConnector -> ExecutionReportRouter -> 전략.on_ack

사용 예:
    python main.py                                   # N분 고가 돌파, dry-run(주문 안 냄)
    python main.py --strategy vol_breakout_long
    python main.py --strategy channel_breakout --send-orders   # 모의투자 계좌로 실제 주문

`--send-orders` 없이 실행하면 신호와 주문 의도만 로그로 남기고 주문은 내지 않습니다.
어떤 전략을 고르든 장마감 전량 청산(KrxExitAll)이 안전장치로 함께 등록됩니다.

운영 메모:
- 기본 시세 경로는 REST 폴링(`get_future_price`, 5초)입니다. KIS 초당 요청 제한(EGW00201)에
  걸리기 쉬우므로 폴링 실패는 예외로 루프를 죽이지 않고 로그만 남긴 뒤 다음 주기에 재시도합니다.
- 실시간 지수선물 시세(H0IFCNT0/H0IFASP0)는 실전계좌 전용 TR_ID라 PROD 키가 있을 때만
  WebSocket 구독을 시도합니다. 체결통보(H0IFCNI0)는 `KIS_HTS_ID`가 있을 때만 구독합니다.
- 종목코드는 만기마다 바뀝니다(config.yaml `market_data.kis.subscriptions`). 롤오버 시 갱신하세요.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from trading_system.book.positions import extract_positions
from trading_system.config.config_loader import ConfigLoader
from trading_system.market_data.connectors.kis_master_loader import KISMasterLoader
from trading_system.risk.risk_manager import RiskManager
from trading_system.strategy.strategy_manager import StrategyManager
from trading_system.strategy.concepts.cb_split_long_only_pct import CBSplitLongOnlyPct
from trading_system.strategy.concepts.krx_exit_all import KrxExitAll
from trading_system.strategy.concepts.minute_high_breakout import MinuteHighBreakoutStrategy
from trading_system.strategy.concepts.vol_breaking_long import VolBreakingLong
from trading_system.strategy.concepts.vol_breaking_short import VolBreakingShort
from trading_system.types.events import FuturesSimpleFilled
from trading_system.types.enums import BuySell
from trading_system.utils.clock import now_kst

logger = logging.getLogger(__name__)

PRICE_POLL_INTERVAL_SEC = 5.0
NAV_POLL_INTERVAL_SEC = 60.0
# 시작 시 교차검증(청산가능수량 조회) 호출 전 간격. 직전 잔고조회와 붙이면
# 모의투자 서버 초당 요청 제한에 걸린다.
AVAILABILITY_CHECK_SPACING_SEC = 1.1
EXIT_ALL_TIME = "150000"
TICK_SIZE = 0.05  # KOSPI200 선물 호가단위

STRATEGIES = ("minute_breakout", "vol_breakout_long", "vol_breakout_short", "channel_breakout")


# -----------------------------------------------------------------------------
# 백그라운드 루프
# -----------------------------------------------------------------------------
async def _price_poll_loop(stg_manager: StrategyManager, inst_code: str) -> None:
    """REST `get_future_price()`를 주기적으로 폴링해 MarketDataHub로 흘려보냅니다."""
    if stg_manager.rest_api is None:
        logger.warning("[PricePoll] rest_api 없음 - REST 시세 폴링을 시작할 수 없습니다.")
        return
    while True:
        try:
            quote = await stg_manager.rest_api.get_future_price(inst_code)
            if quote and quote.get("current_price", 0):
                filled = FuturesSimpleFilled(
                    inst_code=inst_code,
                    price=quote["current_price"],
                    volume=0,  # 이 REST 엔드포인트는 체결량을 주지 않음
                    open=quote.get("open", -1),
                    high=quote.get("high", -1),
                    low=quote.get("low", -1),
                )
                stg_manager.market_data_hub.on_filled(filled)
        except Exception as e:
            logger.error(f"[PricePoll] 폴링 오류(계속 진행): {e}")
        await asyncio.sleep(PRICE_POLL_INTERVAL_SEC)


async def _nav_poll_loop(
    stg_manager: StrategyManager,
    account: str,
    breakout_strategy: Optional[MinuteHighBreakoutStrategy] = None,
    risk: Optional[RiskManager] = None,
) -> None:
    """REST `get_future_account_balance()`를 주기적으로 폴링해 NAV/PnL을 로깅하고 손실 한도를 점검합니다."""
    if stg_manager.rest_api is None:
        return
    # 시작 점검이 방금 같은 API를 호출했으므로 첫 폴링은 한 주기 쉬었다가 시작한다.
    await asyncio.sleep(NAV_POLL_INTERVAL_SEC)
    while True:
        try:
            balance, summary = await stg_manager.rest_api.get_future_account_balance(account=account)
            if not summary:
                # API 에러(rt_cd != '0')면 빈 dict가 온다. "잔고 0원"으로 착각하지 않도록 실패로 로깅.
                logger.warning("[NAV] 조회 실패(빈 응답) - 속도제한이거나 일시적 오류일 수 있음")
            else:
                # 모의투자 잔고조회가 성공(rt_cd='0')이면서 보유 포지션 목록만 비어 오는 경우가 있다.
                # 전략은 보유 중이라고 추적하는데 조회는 0건이면 그 응답을 의심한다.
                if (
                    breakout_strategy is not None
                    and breakout_strategy.long_flag
                    and not breakout_strategy.position_closed
                    and hasattr(balance, "empty")
                    and balance.empty
                ):
                    logger.warning(
                        f"[NAV] 잔고조회는 포지션 0건이지만 {breakout_strategy.strategy_name}은 "
                        f"entry_price={breakout_strategy.entry_price}로 보유 중이라고 추적 중입니다 "
                        "- 일시적 오응답일 가능성이 높으니 아래 NAV 값을 그대로 신뢰하지 마세요."
                    )
                logger.info(f"[NAV] {summary}")
                if risk is not None:
                    await risk.on_nav(summary)
        except Exception as e:
            logger.error(f"[NAV] 폴링 오류(계속 진행): {e}")
        await asyncio.sleep(NAV_POLL_INTERVAL_SEC)


async def _clock_scheduler(stg_manager: StrategyManager, interval_sec: float = 1.0) -> None:
    """등록된 모든 전략의 `on_clock("HHMMSS")`을 실제 벽시계 시각에 맞춰 매초 호출합니다.

    그 시각에 반응할지는 각 전략의 `on_clock()`이 판단합니다(예: KrxExitAll의 장마감 청산).
    """
    last_sent = None
    while True:
        now_str = now_kst().strftime("%H%M%S")
        if now_str != last_sent:
            last_sent = now_str
            for strategy_id, strategy in stg_manager.executor.get_registered_strategies().items():
                try:
                    await strategy.on_clock(now_str)
                except Exception as e:
                    logger.error(f"[ClockScheduler] {strategy_id}.on_clock({now_str}) 오류: {e}")
        await asyncio.sleep(interval_sec)


# -----------------------------------------------------------------------------
# 시작 점검 (재시작 시 기존 포지션 확인)
# -----------------------------------------------------------------------------
async def _restore_intraday_state(stg_manager: StrategyManager, strategy, inst_code: str) -> None:
    """장중에 시작했으면 오늘 분봉으로 전략의 장중 기준값(변동성 돌파 기준가, 채널 고가/저가)을 복원합니다.

    복원하지 못하면 전략은 늦게 본 가격을 기준값으로 쓰지 않고 그날 진입을 하지 않습니다.
    """
    until = getattr(strategy, "intraday_restore_until", None)
    until_hour = until(int(now_kst().strftime("%H%M%S"))) if until else None
    if not until_hour or stg_manager.rest_api is None:
        return
    try:
        bars = await stg_manager.rest_api.get_future_minute_bars(inst_code, until_hour)
    except Exception as e:
        logger.warning(f"[STARTUP] 분봉 조회 실패 - 장중 기준값을 복원하지 못했습니다: {e}")
        return
    finally:
        await asyncio.sleep(AVAILABILITY_CHECK_SPACING_SEC)
    if strategy.restore_intraday(bars):
        logger.info(f"[STARTUP] 장중 기준값 복원 완료 ({len(bars)} bars until {until_hour})")
    else:
        logger.warning(f"[STARTUP] 분봉에 {until_hour} 이전 데이터가 없어 장중 기준값을 복원하지 못했습니다.")


def _guard_unrestorable_position(stg_manager: StrategyManager, strategy, inst_code: str,
                                 positions: Optional[List[Tuple[str, int]]], restorable: bool) -> None:
    """재시작 직후 전략이 이어받을 수 없는 포지션이 있으면 그 전략의 신규 진입을 막습니다.

    진입 횟수/수량 같은 장중 상태는 메모리에만 있어서, 포지션 상태를 복원하지 못하는 전략은 재시작하면
    이미 진입한 사실을 모르고 같은 신호로 다시 진입한다. 기존 포지션은 KrxExitAll 장마감 청산이 정리한다.
    """
    if strategy is None or restorable:
        return
    if positions is None:
        reason = "계좌 포지션을 확인하지 못함"
    elif any(symbol == inst_code and qty != 0 for symbol, qty in positions):
        reason = f"재시작 시 {inst_code} 기존 포지션 보유({dict(positions).get(inst_code)})"
    else:
        return
    stg_manager.order_manager.kill_switch.block_entries(strategy.strategy_name, reason)


async def _reconcile_positions_on_startup(
    stg_manager: StrategyManager,
    account: str,
    breakout_strategy: Optional[MinuteHighBreakoutStrategy] = None,
    *,
    inst_code: Optional[str] = None,
) -> Optional[List[Tuple[str, int]]]:
    """기존 보유 포지션을 조회해 로그로 남기고, 가능하면 전략 상태를 복원합니다.

    반환값은 확인된 보유 포지션 `[(symbol, net_qty)]`이고, 계좌 상태를 확신할 수 없으면 None입니다.

    `breakout_strategy`가 주어지고 그 종목에 매수(롱) 포지션이 있으면 실제 평균체결단가
    (`ccld_avg_unpr1`)로 `long_flag`/`entry_price`를 복원해 재시작 후에도 손절/익절 추적이
    이어지게 합니다. 숏 포지션은 이 전략(롱 온리)이 추적하지 않고 KrxExitAll 청산에 맡깁니다.

    잔고조회가 "포지션 없음"이라고 하면 청산가능수량 조회(VTTO5105R)로 교차검증합니다.
    두 API가 상충하거나 교차검증 자체가 실패하면 추측성 주문 대신 전략을 halted로 멈춥니다.
    """
    if stg_manager.rest_api is None:
        return []
    inst_code = inst_code or getattr(breakout_strategy, "inst_code", None)
    try:
        balance, summary = await stg_manager.rest_api.get_future_account_balance(account=account)

        if (not hasattr(balance, "empty") or balance.empty) and inst_code:

            async def _check_availability() -> Dict[str, Any]:
                result: Dict[str, Any] = {}
                for _ in range(2):
                    await asyncio.sleep(AVAILABILITY_CHECK_SPACING_SEC)
                    try:
                        result = await stg_manager.rest_api.check_future_order_availability(
                            account=account,
                            symbol=inst_code,
                            side=BuySell.SELL,
                            price=0,
                            order_type="02",
                        )
                    except Exception as e:
                        result = {"is_orderable": False, "error": str(e)}
                    if result.get("is_orderable"):
                        break
                return result

            def _halt(reason: str) -> None:
                logger.error(
                    f"[STARTUP] {reason} 실제 상태를 확신할 수 없으므로 추측성 주문 대신 "
                    "전략을 정지시킵니다. 계좌 상태를 수동으로 확인한 뒤 재시작하세요."
                )
                if breakout_strategy is not None:
                    breakout_strategy.halted = True

            # 보유 포지션 판단은 청산가능수량(liquidation_possible_qty, lqd_psbl_qty1)으로 한다.
            # total_possible_qty(tot_psbl_qty)는 신규 주문 가능 수량을 포함해 플랫 계좌에서도
            # 증거금만 있으면 0보다 크다.
            availability = await _check_availability()
            if not availability.get("is_orderable"):
                _halt(f"교차검증(청산가능수량 조회)이 재시도 후에도 실패했습니다({availability.get('error')}).")
                return
            first_qty = availability.get("liquidation_possible_qty", 0)
            if first_qty > 0:
                # 한 번의 응답으로 판단하지 않고 잠시 뒤 다시 물어 "두 번 다 있다고 하면" 믿는다.
                await asyncio.sleep(5)
                availability2 = await _check_availability()
                if not availability2.get("is_orderable"):
                    _halt(f"교차검증 2차 조회가 실패했습니다({availability2.get('error')}).")
                    return
                second_qty = availability2.get("liquidation_possible_qty", 0)
                if second_qty <= 0:
                    logger.warning(
                        f"[STARTUP] 청산가능수량 1차({first_qty}건)와 2차(0건)가 달라 1차를 오응답으로 "
                        "보고 '포지션 없음'으로 진행합니다."
                    )
                else:
                    _halt(
                        f"잔고조회는 포지션 0건이지만 청산가능수량은 두 차례 모두 "
                        f"{first_qty}건/{second_qty}건입니다."
                    )
                    return

        if hasattr(balance, "empty") and not balance.empty:
            logger.warning(f"[STARTUP] 기존 보유 포지션이 있습니다({len(balance)}건).\n{balance}")

            if breakout_strategy is not None and "shtn_pdno" in balance.columns:
                rows = balance[balance["shtn_pdno"] == breakout_strategy.inst_code]
                if not rows.empty:
                    row = rows.iloc[0]
                    side = str(row.get("sll_buy_dvsn_name", ""))
                    avg_price = float(row.get("ccld_avg_unpr1", 0) or 0)
                    if side == "매수" and avg_price > 0:
                        breakout_strategy.long_flag = True
                        breakout_strategy.position_closed = False
                        breakout_strategy.entry_price = avg_price
                        logger.warning(
                            f"[STARTUP] {breakout_strategy.strategy_name}: 기존 매수 포지션을 "
                            f"entry_price={avg_price}로 재추적합니다(손절/익절 라인 복원)."
                        )
                    elif side and side != "매수":
                        logger.warning(
                            f"[STARTUP] {breakout_strategy.strategy_name}: 기존 포지션이 매도(숏)라 "
                            "이 전략(롱 온리)은 추적하지 않습니다 - KrxExitAll 청산에 맡깁니다."
                        )
        else:
            logger.info(f"[STARTUP] 기존 보유 포지션 없음 {summary}")
        return extract_positions((balance, summary))
    except Exception as e:
        logger.error(f"[STARTUP] 포지션 조회 실패: {e}")
        return None


# -----------------------------------------------------------------------------
# 전략 구성
# -----------------------------------------------------------------------------
def _vol_breakout_params(direction: str, inst_code: str) -> Dict[str, Any]:
    """변동성 돌파 샘플 파라미터. 시각은 HHMMSS 정수/문자열."""
    return {
        "c1_code": inst_code,
        "vol_size_check_time": "084500",  # 이 시각 이후 전일 고저폭 계산
        "base_price_time": 90100,         # 이 시각 이후 첫 체결가를 기준가로
        f"{direction}_threshold": 0.5,    # 기준가 ± 0.5 × 전일 고저폭 돌파 시 진입
        "order_volume": 1,
        "tick_size": TICK_SIZE,
        f"{direction}_order_num_divide": 12,  # 진입 구간 내 돌파 체크 시각 12개(약 27분 간격)
        f"{direction}_order_start": 90500,
        f"{direction}_order_end": 143000,
        "exit_order_volume": 1,
        "exit_order_num_divide": 1,
        "exit_order_start": 145000,
        "exit_order_end": 145500,
        "loss_cut_pct": 0.5,
    }


async def _build_strategy(name: str, stg_manager: StrategyManager, inst_code: str, account: str, send_orders: bool):
    accounts = {inst_code: account}
    if name == "minute_breakout":
        return stg_manager.build_strategy(
            MinuteHighBreakoutStrategy, "MinHighBO", send_orders, [inst_code], [], accounts, 1, {},
            {"window_minutes": 5, "loss_cut_pct": 0.5, "take_profit_pct": 1.0, "order_volume": 1},
            strategy_name="MinHighBO", symbol=inst_code,
        )

    if name in ("vol_breakout_long", "vol_breakout_short"):
        # 전일 고가/저가를 REST로 미리 받아 두고, 전략은 feature 어댑터로 읽는다.
        loader = KISMasterLoader(stg_manager.rest_api)
        if not await loader.load([inst_code]):
            logger.warning("[STARTUP] 전일 OHLC 로드 실패 - 변동성 돌파 전략이 진입 조건을 계산하지 못합니다.")
        feature_dict = {"last_days_ohlc_price": loader.as_prev_day_ohlc_feature()}
        direction, cls, sid = (
            ("long", VolBreakingLong, "VolBO_Long") if name == "vol_breakout_long"
            else ("short", VolBreakingShort, "VolBO_Short")
        )
        return stg_manager.build_strategy(
            cls, sid, send_orders, [inst_code], [], accounts, 1, feature_dict,
            _vol_breakout_params(direction, inst_code),
            strategy_name=sid, symbol=inst_code,
        )

    if name == "channel_breakout":
        return stg_manager.build_strategy(
            CBSplitLongOnlyPct, "ChannelBO", send_orders, [inst_code], ["093000"], accounts, 1, {},
            {
                "long_order_volume": 1,
                "long_order_num_divide": 1,
                "long_time_gap": 1000,        # 분할 주문 간격(ms)
                "loss_cut_pct": 0.5,
                "tick_size": TICK_SIZE,
                "channel_fix_time": "093000",  # 이 시각까지의 장중 고가가 채널
            },
            "kis",
            strategy_name="ChannelBO", symbol=inst_code,
        )

    raise ValueError(f"unknown strategy: {name}")


# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
async def main(strategy_name: str, send_orders: bool, inst_code: Optional[str]) -> None:
    loop = asyncio.get_running_loop()
    cfg = ConfigLoader(env_path=".env").get_config()

    inst_code = inst_code or (cfg.get("market_data", {}).get("kis", {}).get("subscriptions") or [None])[0]
    if not inst_code:
        raise SystemExit("종목코드가 없습니다. --symbol 또는 config.yaml market_data.kis.subscriptions를 설정하세요.")

    account = cfg.get("vps_api", {}).get("account_no", "")
    if not account:
        raise SystemExit(".env에 VPS_KIS_ACCOUNT_NO(모의투자 계좌번호, 예: 12345678-03)를 설정하세요.")

    # 1) 상태 복원: 오늘 낸 주문의 order_no <-> 전략 매핑, 실행 리포트 체크포인트
    state_dir = (cfg.get("runtime") or {}).get("state_dir") or "state"
    stg_manager = StrategyManager(api_config=cfg, loop=loop, state_dir=state_dir)

    logger.info(
        f"[STARTUP] strategy={strategy_name} symbol={inst_code} "
        f"mode={'SEND ORDERS (모의투자)' if send_orders else 'DRY-RUN'}"
    )

    strategy = await _build_strategy(strategy_name, stg_manager, inst_code, account, send_orders)
    await _restore_intraday_state(stg_manager, strategy, inst_code)
    exit_all = stg_manager.build_strategy(
        KrxExitAll, "KRX_ExitAll", send_orders, [inst_code], [EXIT_ALL_TIME], {inst_code: account},
        1, {}, {"exit_time": EXIT_ALL_TIME}, "kis",
        strategy_name="KRX_ExitAll", symbol=inst_code,
    )
    breakout = strategy if isinstance(strategy, MinuteHighBreakoutStrategy) else None

    risk_cfg = cfg.get("risk") or {}
    risk = RiskManager(
        stg_manager.order_manager.kill_switch,
        stg_manager.get_order_service(strategy_name="RiskManager", symbol=inst_code),
        stg_manager.rest_api,
        account,
        max_loss=risk_cfg.get("max_loss"),
        flatten_on_kill=bool(risk_cfg.get("flatten_on_kill", True)),
        kill_file=risk_cfg.get("kill_file"),
    )

    # 2) 계좌 스냅샷: 기존 포지션 확인과 전략 상태 복원 (REST 폴링 시작 전에 끝내야 요청 제한에 안 걸림)
    positions = await _reconcile_positions_on_startup(stg_manager, account, breakout, inst_code=inst_code)
    await asyncio.sleep(AVAILABILITY_CHECK_SPACING_SEC)
    _guard_unrestorable_position(stg_manager, strategy, inst_code, positions, restorable=breakout is not None)

    # 3) 체결통보 경로를 시세보다 먼저 연다: 전략이 움직이기 전에 내 주문 결과를 받을 수 있어야 한다.
    #    통보 없이 주문을 내면 체결/취소를 모른 채 같은 신호로 다시 주문할 수 있으므로, 못 열면 신규 주문을 막는다.
    if not await stg_manager.start_kis_ack() and send_orders:
        stg_manager.order_manager.kill_switch.activate("execution notice stream unavailable at startup")

    # 4) 시세 경로
    prod_cfg = cfg.get("prod_api", {})
    if prod_cfg.get("app_key") and prod_cfg.get("app_secret"):
        await stg_manager.start_kis([inst_code])
    price_poll_task = asyncio.create_task(_price_poll_loop(stg_manager, inst_code))

    # 5) 전략 활성화: 등록 전에는 시세/ACK가 전략에 전달되지 않는다.
    for stg in (strategy, exit_all):
        stg_manager.register_strategy(stg)

    # 6) 운영 감시: NAV/손실 한도, 수동 Kill Switch, 시각 이벤트
    nav_poll_task = asyncio.create_task(_nav_poll_loop(stg_manager, account, breakout_strategy=breakout, risk=risk))
    kill_watch_task = asyncio.create_task(risk.watch_kill_file())
    clock_task = asyncio.create_task(_clock_scheduler(stg_manager))

    try:
        await asyncio.Event().wait()
    finally:
        for t in (price_poll_task, nav_poll_task, kill_watch_task, clock_task):
            t.cancel()
        await stg_manager.stop_kis_ack()
        await stg_manager.stop_kis()
        await stg_manager.executor.shutdown()


# -----------------------------------------------------------------------------
# logging
# -----------------------------------------------------------------------------
_LOGGER_SHORT_NAMES = {
    "trading_system.strategy.concepts.minute_high_breakout": "MinHighBO",
    "trading_system.strategy.concepts.krx_exit_all": "ExitAll",
    "trading_system.api.kis.rest_api": "KisRest",
    "trading_system.api.kis.rest_streamer": "KisWS",
    "trading_system.api.kis.token_manager": "Token",
    "trading_system.market_data.connectors.kis_connector": "KisMD",
    "__main__": "main",
}


class _ShortNameFormatter(logging.Formatter):
    """`[YYYY-MM-DD HH:MM:SS.mmm] [태그] 메시지` 형태로 출력합니다."""

    def format(self, record: logging.LogRecord) -> str:
        record.shortname = _LOGGER_SHORT_NAMES.get(record.name, record.name.rsplit(".", 1)[-1])
        return super().format(record)


_INFRA_LOGGERS = (
    "trading_system.api", "trading_system.market_data", "trading_system.order",
    "trading_system.execution_report", "trading_system.config", "trading_system.book",
    "trading_system.strategy.strategy_manager", "trading_system.strategy.execution",
    "asyncio", "websockets",
)


def _configure_logging(verbose: bool) -> None:
    """전략 신호/주문/NAV/시작 점검은 INFO로, 인프라 모듈은 WARNING 이상만 출력합니다(-v면 전부 INFO)."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        _ShortNameFormatter(fmt="[%(asctime)s.%(msecs)03d] [%(shortname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    if not verbose:
        for name in _INFRA_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="KOSPI200 선물 샘플 전략 실행 (KIS 모의투자)")
    p.add_argument("--strategy", choices=STRATEGIES, default="minute_breakout")
    p.add_argument("--send-orders", action="store_true", help="모의투자 계좌로 실제 주문을 냅니다(기본: dry-run).")
    p.add_argument("--symbol", default=None, help="종목 단축코드(기본: config.yaml 첫 구독 종목)")
    p.add_argument("-v", "--verbose", action="store_true", help="모든 모듈의 INFO 로그 출력")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    _configure_logging(args.verbose)
    try:
        asyncio.run(main(args.strategy, args.send_orders, args.symbol))
    except KeyboardInterrupt:
        pass
