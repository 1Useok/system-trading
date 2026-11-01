"""KRX 전체 포지션 청산 전략 (안전장치).

`exit_time`(HHMMSS) 이후 계좌의 모든 선물 포지션을 반대매매로 청산합니다.
잔고 조회와 주문은 주입된 `OrderService`를 통해서만 수행하고, 청산은 진입 제한
카운터(`bullet`)와 무관하게 항상 실행합니다.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Iterable, Optional, Tuple

from trading_system.types.enums import AckType, BuySell, NewOrCancelOrReplace, PriceType
from trading_system.book.order_book import Book
from trading_system.book.positions import extract_positions
from trading_system.order.order_service import OrderService
from trading_system.strategy.concepts.abstract_strategy import Strategy

logger = logging.getLogger(__name__)


class KrxExitAll(Strategy):
    def __init__(
        self,
        _strategy_name: str,
        _real: bool,
        _quote_list: Iterable[str],
        _clock_list: Iterable[str],
        _account_dict: Dict[str, str],
        _bullet: int,
        _feature_dict: Dict[str, Any],
        _param: Dict[str, Any],
        _broker: str,
        rest_api=None,
        market_data_hub=None,
        order_service: Optional[OrderService] = None,
    ):
        self.strategy_name = _strategy_name
        self.real = _real
        self.quote_list = list(_quote_list)
        self.clock_list = list(_clock_list)
        self.account_dict = dict(_account_dict)
        self.feature_dict = _feature_dict
        self.param = _param
        self.broker = _broker
        self.health_check_value = ""

        self.rest_api = rest_api
        self.market_data_hub = market_data_hub
        self.order_service = order_service

        self.bullet = _bullet
        self.book = Book(self.strategy_name, list(self.account_dict.keys()))

        self.exit_done = False
        self.exit_time = str(self.param.get("exit_time") or (self.clock_list[0] if self.clock_list else ""))

        # NOTE(운영 중 확인된 문제): 모의투자 잔고조회가 rt_cd='0'(성공)이면서도 실제 보유
        # 포지션을 "0건"으로 응답한 채 수십 분 지속된 적이 있다. exit_time과 정확히 같은
        # 1초에만 시도하면, 그 1초에 오응답이 오는 날은 장마감 청산(마지막 안전장치)이
        # 통째로 스킵된다. 그래서 exit_time '이후' 재시도 창을 두고, 잔고가 비어 보여도
        # 별도 API(청산가능수량조회)로 교차검증해서 의심스러우면 재시도한다.
        self.exit_retry_window_sec = int(self.param.get("exit_retry_window_sec", 300))
        self.exit_retry_cooldown_sec = float(self.param.get("exit_retry_cooldown_sec", 10))
        self.cross_check_spacing_sec = float(self.param.get("cross_check_spacing_sec", 1.1))
        self._exit_deadline = self._clock_to_seconds(self.exit_time) + self.exit_retry_window_sec
        self._last_exit_attempt_ts: float = 0.0

    @staticmethod
    def _clock_to_seconds(clock: str) -> int:
        """`HHMMSS` 문자열을 자정 기준 초로 변환(비교/연산용)."""
        s = str(clock).strip().zfill(6)
        try:
            return int(s[0:2]) * 3600 + int(s[2:4]) * 60 + int(s[4:6])
        except ValueError:
            return -1

    async def on_quote(self, quote):
        return None

    async def on_filled(self, filled):
        return None

    async def on_ack(self, ack):
        # 라우팅은 QuoteManager가 strategy_id 기준으로 처리하므로 전략에서 broker 필터링 금지
        self.book.process_ack(ack)
        return None

    def _extract_positions(self, balance_result) -> Iterable[Tuple[str, int]]:
        """브로커 잔고 응답에서 (symbol, net_qty) 목록을 추출(매도 포지션은 음수)."""
        return extract_positions(balance_result)

    async def _flatten_position(self, account: str, symbol: str, net_qty: int) -> None:
        if self.order_service is None:
            raise RuntimeError("KrxExitAll requires injected order_service")
        if net_qty == 0:
            return
        # NOTE: 이전에는 `self.bullet <= 0`이면 청산 자체를 건너뛰었다. `bullet`은
        # "동시 진입 가능 횟수" 제한용 카운터인데, 청산(안전장치)에까지 적용되면
        # 보유 종목 수가 bullet보다 많을 때 일부 포지션이 장 마감 후에도 청산되지
        # 않고 남는다. 청산은 리스크를 줄이는 동작이므로 bullet과 무관하게 항상
        # 수행해야 한다.

        side = BuySell.SELL if net_qty > 0 else BuySell.BUY
        qty = abs(int(net_qty))

        if self.real:
            rsp = await self.order_service.place_future_order(
                symbol=symbol,
                quantity=qty,
                price=0.0,
                side=side,
                price_type=PriceType.MARKET,
                account=account,
                time_in_force="DAY",
                risk_reducing=True,
            )
        else:
            rsp = None

        order_num = rsp
        if isinstance(rsp, dict):
            output = rsp.get("output") or {}
            if isinstance(output, dict):
                order_num = output.get("ODNO") or output.get("order_id") or output.get("order_num") or rsp
            else:
                order_num = rsp.get("order_num") or rsp

        self.book.register_order_info(
            symbol,
            order_num,
            None,
            side,
            str(0),
            qty,
            NewOrCancelOrReplace.NEW,
        )
        self.book.register_runtest_order_info(
            symbol,
            order_num if self.real else None,
            None,
            side,
            str(0),
            qty,
            NewOrCancelOrReplace.NEW,
        )
        # NOTE: 청산은 bullet(진입 제한 카운터)을 소모하지 않는다(위 참고).

    async def _has_uncleared_position(self, account: str, symbol: str) -> bool:
        """잔고조회가 "포지션 없음"이라고 할 때, 별도 API로 한 번 더 의심해본다.

        `rest_api`가 주입돼 있어야 동작한다(`StrategyManager.build_strategy()`가
        `rest_api` 파라미터를 보고 자동 주입함). 주입 안 됐으면 교차검증을
        건너뛰고(기존과 동일하게) 잔고조회 결과를 그대로 믿는다.
        """
        if self.rest_api is None:
            return False
        # 직전 잔고조회와 붙여 보내면 초당 요청 제한에 걸리므로 간격을 둔다.
        await asyncio.sleep(self.cross_check_spacing_sec)
        try:
            availability = await self.rest_api.check_future_order_availability(
                account=account, symbol=symbol, side=BuySell.SELL, price=0, order_type="02"
            )
        except Exception as e:
            logger.warning("KrxExitAll: 청산가능수량 교차검증 실패(%s/%s): %s", account, symbol, e)
            return True  # 확인 불가 -> 불확실로 보고 재시도
        if not availability.get("is_orderable"):
            logger.warning("KrxExitAll: 청산가능수량 교차검증 실패(%s/%s): %s",
                           account, symbol, availability.get("error"))
            return True  # 확인 불가 -> 불확실로 보고 재시도
        # 보유 포지션 판단은 청산가능수량(lqd_psbl_qty1)으로 한다. total_possible_qty는
        # 신규 주문 가능 수량을 포함해 플랫 계좌에서도 0보다 크다.
        return availability.get("liquidation_possible_qty", 0) > 0

    async def on_clock(self, clock):
        if self.exit_done:
            return None

        # clock은 외부에서 문자열로 들어오는 경우가 많음
        now_sec = self._clock_to_seconds(clock)
        if now_sec < self._clock_to_seconds(self.exit_time):
            return None
        if now_sec >= self._exit_deadline:
            # 재시도 기한을 넘기면(무한 재시도 방지) 여기서 포기하고 종료 처리한다.
            logger.error(
                "KrxExitAll: 재시도 기한(%ds) 초과 - 청산 확인 없이 종료합니다. 수동 확인 필요.",
                self.exit_retry_window_sec,
            )
            self.exit_done = True
            return None

        if time.time() - self._last_exit_attempt_ts < self.exit_retry_cooldown_sec:
            return None
        self._last_exit_attempt_ts = time.time()

        if self.order_service is None:
            raise RuntimeError("KrxExitAll requires injected order_service")

        # account_dict는 종목별 계좌일 수 있으므로 unique account list 구성
        account_list = sorted(set(self.account_dict.values()))

        any_uncertain = False
        for account in account_list:
            try:
                balance = await self.order_service.get_future_account_balance(account=account)
            except Exception as e:
                logger.error("KrxExitAll: balance fetch failed (account=%s): %s", account, e)
                any_uncertain = True
                continue

            positions = self._extract_positions(balance)

            if not positions:
                for symbol in self.quote_list:
                    if await self._has_uncleared_position(account, symbol):
                        logger.error(
                            "KrxExitAll: %s/%s 잔고조회는 포지션 0건이지만 청산가능수량조회는 "
                            "실제 보유수량이 있다고 보고합니다 - 오응답으로 판단해 이번 틱은 "
                            "종료 처리하지 않고 재시도합니다.",
                            account, symbol,
                        )
                        any_uncertain = True

            for symbol, net_qty in positions:
                await self._flatten_position(account=account, symbol=symbol, net_qty=net_qty)

        if not any_uncertain:
            self.exit_done = True
        return None
