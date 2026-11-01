# -*- coding: utf-8 -*-
"""N분 최고가 돌파 전략 (Minute High Breakout).

로직:
1) 매 체결(on_filled)마다 현재 분(minute bucket)의 최고가를 갱신한다.
2) 분이 바뀌면 직전 분을 "완결된 분봉"으로 확정해 rolling window(deque)에 넣고,
   window_minutes개보다 많아지면 가장 오래된 것부터 버린다.
3) window가 다 찼고 현재가가 "최근 N개 완결 분봉의 최고가"보다 높으면 상향 돌파로 보고 매수.
4) 진입가 대비 loss_cut_pct% 하락 시 손절, take_profit_pct% 상승 시 익절.
   청산이 성공하면 `_reset_after_exit()`로 상태를 초기화해 다시 3)부터 재진입할 수 있다.

상태(long_flag/position_closed)는 주문이 실제로 "접수 성공"(rt_cd=='0')했을 때만 바뀐다.
거부된 주문으로 포지션이 바뀐 것으로 착각하지 않기 위함이다. 시장가에 가까운 지정가로 내므로
체결(ACK/Filled)을 기다리지 않고 접수 시점에 상태를 반영하며, 실제 보유 수량은 체결통보와
잔고 조회(시작 점검, NAV 폴링)로 맞춰 본다.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any, Deque, Dict, Optional

from trading_system.types.enums import BuySell, PriceType

logger = logging.getLogger(__name__)


class MinuteHighBreakoutStrategy:
    def __init__(
        self,
        strategy_name: str,
        real: bool,
        quote_list: list,
        clock_list: list,
        account_dict: dict,
        bullet: int,
        feature_dict: dict,
        param: Dict[str, Any],
        order_service=None,
    ):
        """
        Args:
            param: window_minutes(기본 5), loss_cut_pct(기본 0.5), take_profit_pct(기본 1.0),
                   order_volume(기본 1), exit_retry_cooldown_sec(기본 30)
        """
        self.strategy_name = strategy_name
        self.real = real
        self.quote_list = quote_list
        self.clock_list = clock_list
        self.account_dict = account_dict
        self.bullet = bullet
        self.feature_dict = feature_dict
        self.param = param
        self.order_service = order_service
        self.health_check_value = ""

        self.inst_code = quote_list[0]
        self.window_minutes = int(param.get("window_minutes", 5))
        self.loss_cut_pct = float(param.get("loss_cut_pct", 0.5))
        self.take_profit_pct = float(param.get("take_profit_pct", 1.0))
        self.order_volume = int(param.get("order_volume", 1))
        self.account = account_dict.get(self.inst_code, "")

        # 청산 주문이 브로커 쪽 사유(예: 당일 매수분 청산 제한, 속도제한)로 계속
        # 거부되는 경우, 매 틱(수 초 간격)마다 재시도하면 API를 두드려대다 속도제한을
        # 유발하고 다른 폴링(NAV/시세)까지 도미노로 실패시킨다.
        # 그래서 청산 재시도는 이 쿨다운 간격으로 제한한다 - 실패해도 다음 틱에
        # 바로 또 시도하지 않는다.
        self.exit_retry_cooldown_sec = float(param.get("exit_retry_cooldown_sec", 30))
        self._last_exit_attempt_ts: float = 0.0

        self.current_minute_key: Optional[int] = None
        self.current_minute_high: float = 0.0
        self.minute_highs: Deque[float] = deque()

        self.long_flag = False
        self.position_closed = False
        self.entry_price = 0.0

        # 재시작 시 계좌 상태를 확신할 수 없으면(main.py의 시작 점검 참고) 추측성 주문을
        # 내지 않고 진입/청산을 모두 멈추는 halted 상태로 둔다. "확실치 않을 때 일단
        # 팔아본다"는 대응은 실제로 플랫인 계좌에 의도치 않은 숏 포지션을 만들 수 있다.
        self.halted = False

    async def on_quote(self, quote):
        # no-op: REST 폴링 기반이라 체결/현재가만 on_filled로 들어온다.
        return None

    async def on_filled(self, filled):
        if getattr(filled, "inst_code", None) != self.inst_code:
            return None

        price = float(filled.price)
        now_minute_key = int(time.time() // 60)

        if self.current_minute_key is None:
            # 첫 틱: 이번 분을 시작만 하고, 아직 완결된 분이 없으니 신호는 안 본다.
            self.current_minute_key = now_minute_key
            self.current_minute_high = price
            return None

        if now_minute_key != self.current_minute_key:
            # 분이 바뀌었다 -> 직전 분을 "완결된 분봉"으로 확정하고 window에 넣는다.
            self.minute_highs.append(self.current_minute_high)
            while len(self.minute_highs) > self.window_minutes:
                self.minute_highs.popleft()
            logger.info(
                f"분봉 확정 high={self.current_minute_high} "
                f"(window={len(self.minute_highs)}/{self.window_minutes})"
            )
            self.current_minute_key = now_minute_key
            self.current_minute_high = price
        else:
            self.current_minute_high = max(self.current_minute_high, price)

        self.health_check_value = str(price)

        if self.halted:
            # 재시작 시 계좌 상태를 확신할 수 없어 멈춰둔 상태 - 사람이 실제
            # 잔고를 확인한 뒤 재시작해야 풀린다. 이 상태에서는 진입도 청산도 하지 않는다.
            return None

        await self._check_breakout(price)
        await self._check_loss_cut(price)
        await self._check_take_profit(price)
        return None

    async def _check_breakout(self, price: float) -> None:
        if self.long_flag:
            return
        if len(self.minute_highs) < self.window_minutes:
            return  # 아직 N분치가 안 참
        rolling_high = max(self.minute_highs)
        if price > rolling_high:
            logger.info(
                f"최근 {self.window_minutes}분 최고가({rolling_high}) "
                f"상향 돌파 @ {price} -> 매수 진입 시도"
            )
            if await self._place_order(BuySell.BUY, price, "BREAKOUT_ENTRY"):
                self.long_flag = True
                self.entry_price = price

    def _exit_retry_allowed(self) -> bool:
        """청산 재시도 쿨다운 체크. 허용되면 타임스탬프를 갱신하고 True를 반환한다."""
        now = time.time()
        if now - self._last_exit_attempt_ts < self.exit_retry_cooldown_sec:
            return False
        self._last_exit_attempt_ts = now
        return True

    def _reset_after_exit(self) -> None:
        """청산 성공 후 다음 진입을 다시 받을 수 있도록 상태를 초기화한다.

        `long_flag`를 되돌리지 않으면 프로세스가 사는 동안 재진입을 못 하고,
        `position_closed`를 되돌리지 않으면 다음 진입 후 손절/익절 가드
        (`if ... or self.position_closed: return`)가 막혀 새 포지션이 무방비가 된다.
        """
        self.long_flag = False
        self.position_closed = False
        self.entry_price = 0.0

    async def _check_loss_cut(self, price: float) -> None:
        if not self.long_flag or self.position_closed:
            return
        trigger = self.entry_price * (1.0 - self.loss_cut_pct / 100.0)
        if price <= trigger:
            if not self._exit_retry_allowed():
                return
            logger.info(f"손절 트리거({trigger}) @ {price} -> 매도 청산 시도")
            if await self._place_order(BuySell.SELL, price, "LOSS_CUT_EXIT"):
                self._reset_after_exit()

    async def _check_take_profit(self, price: float) -> None:
        if not self.long_flag or self.position_closed:
            return
        trigger = self.entry_price * (1.0 + self.take_profit_pct / 100.0)
        if price >= trigger:
            if not self._exit_retry_allowed():
                return
            logger.info(f"익절 트리거({trigger}) @ {price} -> 매도 청산 시도")
            if await self._place_order(BuySell.SELL, price, "TAKE_PROFIT_EXIT"):
                self._reset_after_exit()

    async def _place_order(self, side: BuySell, price: float, reason: str) -> bool:
        """주문을 낸다. 반환값은 "실제 상태 변화로 취급해도 되는지"
        (dry-run은 항상 True, 실주문은 접수 성공(rt_cd=='0')일 때만 True)."""
        if not self.real or self.order_service is None:
            logger.info(f"(dry-run) order skipped ({reason})")
            return True

        try:
            rsp = await self.order_service.place_future_order(
                symbol=self.inst_code,
                quantity=self.order_volume,
                price=price,
                side=side,
                price_type=PriceType.LIMIT,
                account=self.account,
                time_in_force="DAY",
                risk_reducing=(side == BuySell.SELL),  # 롱 온리: 매도는 청산
            )
        except Exception as e:
            logger.error(f"주문 실패({reason}): {e}")
            return False

        success = isinstance(rsp, dict) and rsp.get("rt_cd") == "0"
        order_no = None
        if isinstance(rsp, dict):
            output = rsp.get("output") or {}
            order_no = output.get("ODNO") if isinstance(output, dict) else None
        logger.info(
            f"order_service.place_future_order -> "
            f"success={success} order_id={order_no} reason={reason} price={price}"
        )
        return success

    async def on_ack(self, ack):
        # ACK는 로깅만 하고 포지션 상태를 바꾸지 않는다(주문 접수 성공 시점에
        # 이미 낙관적으로 상태를 바꿔놨음 - 다른 샘플 전략들과 동일한 설계).
        ack_type = getattr(ack, "ack_type", None)
        order_num = getattr(ack, "order_num", None)
        logger.info(f"ACK 수신 type={ack_type} order_num={order_num}")
        return None

    async def on_clock(self, clock):
        return None
