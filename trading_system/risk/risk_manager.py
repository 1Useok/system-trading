"""계좌 단위 리스크 감시와 Kill Switch 실행.

발동 경로
- 수동: 실행 디렉터리에 `kill_file`(기본 `KILL`)을 만들면 다음 감시 주기(1초)에 발동
- 자동: NAV 조회 결과의 평가손익합계가 `-max_loss` 이하로 떨어지면 발동

발동 시 수행(`trigger`)
1) 신규 주문 차단 (KillSwitch 활성화 -> OrderService가 리스크를 늘리는 주문을 거부)
2) 미체결 주문 일괄 취소 (주문체결내역 미체결 조회 -> 주문별 전량 취소)
3) (옵션) 보유 포지션 전량 시장가 청산
"""
import asyncio
import logging
import os
from typing import Any, Dict, Optional

from trading_system.book.positions import extract_positions
from trading_system.risk.kill_switch import KillSwitch
from trading_system.types.enums import BuySell, PriceType

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(
        self,
        kill_switch: KillSwitch,
        order_service,
        rest_api,
        account: str,
        *,
        max_loss: Optional[float] = None,
        flatten_on_kill: bool = True,
        kill_file: Optional[str] = "KILL",
    ):
        """
        Args:
            kill_switch: 주문 경로(OrderManager)와 공유하는 KillSwitch
            order_service: 취소/청산 주문에 쓸 브로커 바인딩 OrderService
            rest_api: 미체결 주문 조회용 REST 클라이언트
            account: 대상 계좌("12345678-03")
            max_loss: 평가손익합계 손실 한도(원). None이면 자동 발동 안 함
            flatten_on_kill: 발동 시 보유 포지션까지 청산할지 여부
            kill_file: 수동 발동용 파일 경로. None이면 감시 안 함
        """
        self.kill_switch = kill_switch
        self.order_service = order_service
        self.rest_api = rest_api
        self.account = account
        self.max_loss = max_loss
        self.flatten_on_kill = flatten_on_kill
        self.kill_file = kill_file
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # 발동 조건
    # ------------------------------------------------------------------
    async def on_nav(self, summary: Dict[str, Any]) -> None:
        """NAV 조회 결과로 손실 한도를 점검합니다(`_nav_poll_loop`에서 호출)."""
        if not self.max_loss or not summary:
            return
        pnl = float(summary.get("total_profit_loss", 0.0) or 0.0)
        if pnl <= -abs(self.max_loss):
            await self.trigger(f"loss limit breached: pnl={pnl:,.0f} <= -{abs(self.max_loss):,.0f}")

    async def watch_kill_file(self, interval_sec: float = 1.0) -> None:
        """`kill_file`이 생기면 발동합니다."""
        if not self.kill_file:
            return
        while not self.kill_switch.active:
            if os.path.exists(self.kill_file):
                await self.trigger(f"manual kill file detected ({self.kill_file})")
                return
            await asyncio.sleep(interval_sec)

    # ------------------------------------------------------------------
    # 발동
    # ------------------------------------------------------------------
    async def trigger(self, reason: str) -> None:
        async with self._lock:
            if not self.kill_switch.activate(reason):
                return
            try:
                canceled = await self.cancel_all()
                logger.critical(f"[RISK] 미체결 {canceled}건 취소 요청 완료")
            except Exception as e:
                logger.critical(f"[RISK] 미체결 일괄 취소 실패 - 수동 확인 필요: {e}")
            if self.flatten_on_kill:
                try:
                    flattened = await self.flatten_all()
                    logger.critical(f"[RISK] 포지션 {flattened}건 청산 주문 완료")
                except Exception as e:
                    logger.critical(f"[RISK] 포지션 청산 실패 - 수동 확인 필요: {e}")

    async def cancel_all(self) -> int:
        """계좌의 미체결 주문을 모두 취소 요청합니다. 반환값은 접수된 취소 건수.

        조회 실패를 "미체결 없음"으로 오인하지 않도록 조회 오류는 예외로 올린다.
        """
        open_orders = await self.rest_api.inquire_order_history(self.account, order_status="02", raise_on_error=True)
        accepted = 0
        for row in open_orders or []:
            order_no = str(row.get("odno") or "").strip()
            try:
                remaining = int(float(row.get("qty") or 0))  # 잔량
            except (TypeError, ValueError):
                remaining = 0
            if not order_no or remaining <= 0:
                continue
            rsp = await self.order_service.cancel_future_order(
                order_id=order_no, account=self.account, symbol=str(row.get("pdno") or ""), quantity=0,
            )
            if isinstance(rsp, dict) and rsp.get("rt_cd") == "0":
                accepted += 1
            else:
                logger.error(f"[RISK] 취소 실패 order_no={order_no}: {rsp}")
        return accepted

    async def flatten_all(self) -> int:
        """보유 포지션을 전량 시장가 반대매매로 청산합니다. 반환값은 접수된 청산 주문 수."""
        balance = await self.order_service.get_future_account_balance(account=self.account)
        accepted = 0
        for symbol, net_qty in extract_positions(balance):
            rsp = await self.order_service.place_future_order(
                symbol=symbol,
                quantity=abs(net_qty),
                price=0.0,
                side=BuySell.SELL if net_qty > 0 else BuySell.BUY,
                price_type=PriceType.MARKET,
                account=self.account,
                time_in_force="DAY",
                risk_reducing=True,
            )
            if isinstance(rsp, dict) and rsp.get("rt_cd") == "0":
                accepted += 1
            else:
                logger.error(f"[RISK] 청산 실패 {symbol} {net_qty}: {rsp}")
        return accepted
