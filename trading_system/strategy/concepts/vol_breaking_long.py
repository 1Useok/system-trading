# -*- coding: utf-8 -*-
"""
변동성 돌파 Long 전략 (Volatility Breakout)

- 기준가(base_price): `base_price_time` 이후 첫 체결가
- 변동성(vol_size): 전일 고가 - 전일 저가 (`feature_dict["last_days_ohlc_price"]`)
- 진입: 현재가가 `base_price + threshold × 전일 고저폭`를 상향 돌파하면, 진입 구간 동안 분할 주문
- 청산: 청산 구간에 분할 청산, 진입 평단 대비 `loss_cut_pct`% 역행 시 전량 손절
- 진입 횟수는 `bullet`으로 제한하며, 청산/손절은 bullet을 소모하지 않음
"""
import datetime
import math
import logging
from typing import Optional, List

from trading_system.types.enums import BuySell as OrderSide, PriceType

from .utils.book import Book, BuySell
from .utils.time_utils import calculate_time_list, is_after_market_close


class VolBreakingLong:
    """
    Volatility Breaking Long Strategy Concept.

    Enters long positions when price breaks above base price + threshold * volatility.
    Uses previous day's high-low range as volatility measure.
    """

    def __init__(self, strategy_name: str, real: bool, quote_list: list,
                 clock_list: list, account_dict: dict, bullet: int,
                 feature_dict: dict, param: dict, order_service):
        self.strategy_name = strategy_name
        self.real = real
        self.quote_list = quote_list
        self.clock_list = clock_list
        self.account_dict = account_dict
        self.feature_dict = feature_dict
        self.param = param
        self.order_service = order_service
        self.health_check_value = ""

        self.bullet = bullet
        self.book = Book(strategy_name, list(account_dict.keys()))

        self.futures_code = quote_list[0]

        # Vol breaking specific parameters
        self.futures_c1_code = str(param['c1_code'])
        self.vol_size_check_time = str(param['vol_size_check_time'])
        self.base_price_time = int(param['base_price_time'])
        self.vol_size: Optional[float] = None
        self.base_price: Optional[float] = None
        self.base_price_recorded = False

        # Threshold and order parameters
        self.long_threshold = float(param['long_threshold'])
        self.order_volume = int(param['order_volume'])
        self.tick_size = float(param['tick_size'])

        self.long_order_num_divide = int(param['long_order_num_divide'])
        self.long_order_start = int(param['long_order_start'])
        self.long_order_end = int(param['long_order_end'])

        self.exit_order_volume = int(param['exit_order_volume'])
        self.exit_order_num_divide = int(param['exit_order_num_divide'])
        self.exit_order_start = int(param['exit_order_start'])
        self.exit_order_end = int(param['exit_order_end'])

        self.loss_cut_pct = float(param['loss_cut_pct'])
        self.loss_cut = False

        # Price tracking
        self.price: Optional[float] = None
        self.price_chg: Optional[float] = None
        self.min_check_time: Optional[int] = None
        self.min_close_price: Optional[float] = None

        # 호가 데이터 (1호가)
        self.best_ask: Optional[float] = None
        self.best_bid: Optional[float] = None

        # Position tracking
        self.position = 0
        self.average_price = 0.0

        # Backtest variable
        self.backtest_volume = 0

        # Initialize time lists
        self._initiate_time_lists()

        # Check if exit is after market close (15:45+)
        self.exit_after_close = is_after_market_close(self.exit_order_start)

    def _initiate_time_lists(self):
        """Initialize order time lists for split entry/exit."""
        self.long_order_sent_list = [False] * self.long_order_num_divide
        self.exit_order_sent_list = [False] * self.exit_order_num_divide

        self.long_order_time_list = calculate_time_list(
            self.long_order_start, self.long_order_end, self.long_order_num_divide
        )
        self.exit_order_time_list = calculate_time_list(
            self.exit_order_start, self.exit_order_end, self.exit_order_num_divide
        )

    async def _send_order(self, side: OrderSide, volume: int, price: float) -> Optional[str]:
        """주문을 내고, 브로커가 접수(rt_cd == '0')하면 주문번호를, 아니면 None을 반환합니다."""
        try:
            rsp = await self.order_service.place_future_order(
                symbol=self.futures_code,
                quantity=int(volume),
                price=float(price),
                side=side,
                price_type=PriceType.LIMIT,
                account=self.account_dict.get(self.futures_code),
                time_in_force="DAY",
            )
        except Exception as e:
            logging.error(f"{self.strategy_name} order failed: {e}")
            return None
        if not (isinstance(rsp, dict) and rsp.get("rt_cd") == "0"):
            logging.warning(f"{self.strategy_name} order rejected: {rsp}")
            return None
        output = rsp.get("output") or {}
        order_no = output.get("ODNO") if isinstance(output, dict) else None
        return str(order_no or "UNKNOWN")  # 접수 성공이면 항상 truthy

    async def _check_and_order(self):
        """Check conditions and place orders if necessary."""
        if self.price is None or self.base_price is None:
            return
        if self.vol_size is None or self.price_chg is None:
            return
        if self.min_close_price is None:
            return

        now = datetime.datetime.now()
        hms = float(now.strftime("%H%M%S.%f"))

        # Long Entry
        if hms > self.long_order_end:
            for i in range(self.long_order_num_divide):
                if not self.long_order_sent_list[i]:
                    self.long_order_sent_list[i] = True
        else:
            for i in range(self.long_order_num_divide):
                if (hms >= self.long_order_time_list[i]
                        and not self.long_order_sent_list[i]
                        and not self.loss_cut):

                    logging.info(f"{self.strategy_name} Long entry {i}")
                    self.long_order_sent_list[i] = True

                    entry_order_volume = 0
                    if self.order_volume > 0:
                        entry_order_volume = math.floor(
                            self.order_volume / (self.long_order_num_divide - i)
                        )
                        if entry_order_volume == 0:
                            entry_order_volume = 1

                    # Vol breaking long condition
                    if (self.price >= (self.base_price + self.long_threshold * self.vol_size)
                            and entry_order_volume > 0
                            and self.bullet > 0):

                        logging.info(f"Buy! Bullet: {self.bullet}, volume: {entry_order_volume}")

                        self.bullet -= 1
                        # 남은 진입 수량은 실제로 진입할 때만 줄인다. 조건 미충족 슬롯에서 줄이면
                        # 첫 슬롯에서 돌파가 없을 때 이후 슬롯이 모두 수량 0으로 막힌다.
                        self.order_volume -= entry_order_volume
                        # 손절은 분봉 종가(min_close_price) 기준으로 판단한다. 진입 직전 분의 가격은
                        # 돌파 전이라 진입가보다 불리한 쪽에 있으므로, 그대로 쓰면 큰 돌파에서
                        # 진입 즉시 손절이 나간다. 진입 시점 가격으로 기준을 다시 잡는다.
                        self.min_close_price = self.price

                        if self.real and self.order_service:
                            # 매수 → 매도1호가(Ask1) + 3틱 (반대편 3호가, 체결 보장)
                            if self.best_ask and self.best_ask > 0:
                                order_price = round(self.best_ask + self.tick_size * 3, 2)
                            else:
                                order_price = round(self.price + self.tick_size * 6, 2)
                            order_result = await self._send_order(OrderSide.BUY, entry_order_volume, order_price)

                            if order_result:
                                logging.info(f"Order registered: {order_result}")
                                self.book.register_order_info(
                                    self.futures_code,
                                    order_result,
                                    BuySell.BUY,
                                    str(order_price),
                                    entry_order_volume
                                )
                                if self.position == 0:
                                    self.average_price = self.price
                                else:
                                    self.average_price = (
                                        (self.average_price * self.position + self.price * entry_order_volume)
                                        / (self.position + entry_order_volume)
                                    )
                                self.position += entry_order_volume
                                logging.info(f"{self.strategy_name} position: {self.position}, avg: {self.average_price:.2f}")

                        self.book.register_runtest_order_info(
                            self.futures_code,
                            BuySell.BUY,
                            str(round(self.price, 2)),
                            entry_order_volume
                        )
                        self.backtest_volume += entry_order_volume

        # Exit
        if hms > self.exit_order_end:
            for i in range(self.exit_order_num_divide):
                if not self.exit_order_sent_list[i]:
                    self.exit_order_sent_list[i] = True
        else:
            for i in range(self.exit_order_num_divide):
                if (hms >= self.exit_order_time_list[i]
                        and not self.exit_order_sent_list[i]
                        and not self.loss_cut):

                    self.exit_order_sent_list[i] = True

                    if self.real:
                        if self.position > 0:
                            exit_order_volume_ = self.exit_order_volume

                            logging.info(f"{self.strategy_name} Exit {i} order {exit_order_volume_}")

                            if self.exit_after_close:
                                self.book.register_closing_exit(
                                    self.futures_code,
                                    BuySell.SELL,
                                    self.price,
                                    exit_order_volume_
                                )
                                self.position -= exit_order_volume_
                            else:
                                if self.order_service:
                                    if self.best_bid and self.best_bid > 0:
                                        order_price = self.best_bid
                                    else:
                                        order_price = round(self.price - self.tick_size * 6, 2)
                                    order_result = await self._send_order(OrderSide.SELL, exit_order_volume_, order_price)

                                    if order_result:
                                        self.book.register_order_info(
                                            self.futures_code,
                                            order_result,
                                            BuySell.SELL,
                                            str(order_price),
                                            exit_order_volume_
                                        )
                                        self.position -= exit_order_volume_

                    if self.backtest_volume > 0:
                        exit_order_volume_ = self.exit_order_volume

                        self.book.register_runtest_order_info(
                            self.futures_code,
                            BuySell.SELL,
                            str(round(self.price, 2)),
                            exit_order_volume_
                        )
                        self.backtest_volume -= exit_order_volume_

        await self._check_loss_cut()

    async def _check_loss_cut(self):
        """Check and execute loss cut if necessary."""
        if self.loss_cut or self.position == 0 or self.average_price == 0:
            return

        order_volume = abs(self.position)
        loss_pct = (self.average_price - self.min_close_price) / self.average_price * 100

        if self.real and self.position > 0 and loss_pct >= self.loss_cut_pct:
            logging.info(f"{self.strategy_name} Loss cut exit {order_volume}")

            if self.order_service:
                if self.best_bid and self.best_bid > 0:
                    order_price = self.best_bid
                else:
                    order_price = round(self.price - self.tick_size * 6, 2)
                order_result = await self._send_order(OrderSide.SELL, order_volume, order_price)

                if order_result:
                    self.book.register_order_info(
                        self.futures_code,
                        order_result,
                        BuySell.SELL,
                        str(order_price),
                        order_volume
                    )
                    self.position = 0

            self.loss_cut = True

        if self.backtest_volume > 0 and loss_pct >= self.loss_cut_pct:
            logging.info(f"{self.strategy_name} Backtest loss cut {order_volume}")
            self.book.register_runtest_order_info(
                self.futures_code,
                BuySell.SELL,
                str(round(self.price, 2)),
                order_volume
            )
            self.loss_cut = True

    def _check_min_price(self):
        """Update minimum close price for loss cut calculation."""
        if self.price is None:
            return

        now = datetime.datetime.now()
        int_hms = int(now.strftime("%H%M%S"))

        if self.min_check_time is None:
            self.min_check_time = int_hms
        if self.min_close_price is None:
            self.min_close_price = self.price
        if int_hms // 100 > self.min_check_time // 100:
            self.min_close_price = self.price
            self.min_check_time = int_hms

    async def on_quote(self, quote):
        """Handle quote data."""
        self._check_min_price()
        await self._check_and_order()

    async def on_filled(self, filled):
        """Handle filled trade data."""
        now = datetime.datetime.now()
        hms = int(now.strftime("%H%M%S"))

        if (not self.base_price_recorded
                and hms >= self.base_price_time
                and self.price is not None):
            self.base_price = self.price
            self.base_price_recorded = True
            logging.info(f"{self.strategy_name} Base price set: {self.base_price}")

        self._check_min_price()
        self.price = float(filled.price)
        self.price_chg = float(filled.price_chg)
        self.health_check_value = str(filled.price)

        if hasattr(filled, 'best_ask') and filled.best_ask > 0:
            self.best_ask = filled.best_ask
        if hasattr(filled, 'best_bid') and filled.best_bid > 0:
            self.best_bid = filled.best_bid

        await self._check_and_order()

    async def on_ack(self, ack):
        """Handle order acknowledgment."""
        self.book.process_ack(ack)

    def _update_vol_size(self) -> Optional[float]:
        """Calculate volatility size from previous day's high-low range."""
        now = datetime.datetime.now()
        last_day = now - datetime.timedelta(1)

        lastday_high_price_list: List[float] = []
        lastday_low_price_list: List[float] = []

        feature = self.feature_dict.get("last_days_ohlc_price")
        if feature is None:
            return None

        feature_data = feature.call_feature()
        if self.futures_code not in feature_data:
            return None

        for i in range(20):
            temp_last_day = last_day - datetime.timedelta(i)
            temp_last_day_str = temp_last_day.strftime("%Y%m%d")

            if temp_last_day_str in feature_data[self.futures_code]:
                high_price = float(
                    feature_data[self.futures_code][temp_last_day_str]['futs_hgpr']
                )
                low_price = float(
                    feature_data[self.futures_code][temp_last_day_str]['futs_lwpr']
                )
                lastday_high_price_list.append(high_price)
                lastday_low_price_list.append(low_price)

        if not lastday_high_price_list or not lastday_low_price_list:
            return None

        logging.info(f"{self.strategy_name} high prices: {lastday_high_price_list[:10]}")
        logging.info(f"{self.strategy_name} low prices: {lastday_low_price_list[:10]}")

        vol_size = lastday_high_price_list[0] - lastday_low_price_list[0]
        logging.info(f"vol_size: {vol_size}")

        return vol_size

    async def on_clock(self, clock):
        """Handle clock event (HHMMSS, 매초 호출).

        `vol_size_check_time` 이후 vol_size가 아직 없으면 계산합니다. 정확히 그 초에만
        계산하면 그 시각 이후에 프로세스를 시작했을 때 전략이 영영 동작하지 않으므로,
        '시각 이후 + 미계산' 조건으로 판단합니다.
        """
        if self.vol_size is None and int(clock) >= int(self.vol_size_check_time):
            vol_size = self._update_vol_size()
            if vol_size is not None:
                self.vol_size = vol_size
