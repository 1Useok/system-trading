
# -*- coding: utf-8 -*-
"""
채널 브레이크아웃 롱 포지션 전략 모듈 (rest_api 기반)

이 모듈은 채널 브레이크아웃 기반의 롱 포지션 전략을 구현합니다.
특정 시간까지 형성된 고가 채널을 기준으로, 가격이 채널을 상향 돌파할 때 매수 포지션을 취하고,
손실 발생 시 손절매를 실행하는 전략입니다.
주문은 여러 번에 나누어 실행되며, 손절매도 동일한 방식으로 분할 실행됩니다.

- 진입 횟수는 `bullet`으로 제한합니다. 손절은 리스크를 줄이는 동작이므로 bullet과
  무관하게 항상 실행됩니다(진입이 bullet을 다 써도 손절이 막히지 않음).
- 손절 수량은 목표 진입 수량이 아니라 실제로 접수된 진입 수량(`position`)입니다.
  일부 진입이 거부됐는데 목표 수량만큼 팔면 의도치 않은 숏 포지션이 생깁니다.
"""

from trading_system.types.enums import AckType, BuySell, NewOrCancelOrReplace, PriceType
from trading_system.strategy.concepts.abstract_strategy import Strategy
import datetime
import logging
from trading_system.book.order_book import Book
import math
from typing import List, Optional
from trading_system.utils.clock import KST


class CBSplitLongOnlyPct(Strategy):
    """
    채널 브레이크아웃 롱 포지션 전략 클래스

    특정 시간까지 형성된 고가 채널을 기준으로, 가격이 채널을 상향 돌파할 때 매수 포지션을 취하고,
    손실 발생 시 손절매를 실행하는 전략입니다.

    Attributes:
        strategy_name (str): 전략 이름
        real (bool): 실제 거래 모드 여부
        quote_list (list): 시세 데이터 목록
        clock_list (list): 시간 이벤트 목록
        account_dict (dict): 계좌 정보 사전
        order_module_dict (dict): 주문 모듈 사전
        feature_dict (dict): 기능 사전
        param (dict): 전략 파라미터
        broker (str): 브로커 정보
        bullet (int): 주문 가능 횟수
        index_futures_order_module: 선물 주문 모듈
        book (Book): 주문 관리 객체
        futures_code (str): 선물 종목 코드
        long_order_volume (int): 매수 주문 수량
        long_order_num_divide (int): 매수 주문 분할 횟수
        long_time_gap (int): 매수 주문 간 시간 간격
        long_order_start: 매수 주문 시작 시간
        long_base_price: 매수 기준 가격
        long_flag (bool): 매수 모드 활성화 여부
        high_price: 고가
        low_price: 저가
        report_channel (bool): 채널 보고 여부
        loss_cut_pct (float): 손절매 비율
        loss_cut_order_start: 손절매 주문 시작 시간
        loss_cut_flag (bool): 손절매 모드 활성화 여부
        loss_cut_volume (int): 손절매 주문 수량
        tick_size (float): 틱 크기
        volume_check_time: 거래량 확인 시간
        channel_fix_time: 채널 고정 시간
        position (int): 현재 포지션
        backtest_volume (int): 백테스트용 거래량
        backtest_loss_cut_volume (int): 백테스트용 손절매 거래량
    """

    # Consider generate book
    # Book and bullet must be inside the lock!!!

    def __init__(
        self,
        _strategy_name,
        _real,
        _quote_list,
        _clock_list,
        _account_dict,
        _bullet,
        _feature_dict,
        _param,
        _broker,
        rest_api=None,
        market_data_hub=None,
        order_service=None,
    ):
        """
        CBSplitLongOnlyPct 클래스 초기화
        """
        self.strategy_name = _strategy_name
        self.real = _real
        self.quote_list = _quote_list
        self.clock_list = _clock_list
        self.account_dict = _account_dict
        self.feature_dict = _feature_dict
        self.param = _param
        self.broker = _broker
        self.health_check_value = ""

        # 인프라 의존성 주입
        self.rest_api = rest_api
        self.market_data_hub = market_data_hub
        # 주문은 RestApi/브로커 모듈을 직접 호출하지 않고,
        # 주입된 `OrderService`(브로커 바인딩)만 사용합니다.
        # - 브로커 선택은 StrategyManager(구성 레이어)가 config 기반으로 수행
        # - 주문 식별(client_order_id) 및 strategy_id 라우팅을 위한 registry 등록은 OrderService가 담당
        self.order_service = order_service

        self.bullet = _bullet
        self.book = Book(self.strategy_name, list(self.account_dict.keys()))
        self.futures_code = self.quote_list[0]  # 선물 종목 코드

        # 파라미터 초기화
        self.long_order_volume = int(self.param['long_order_volume'])  # 매수 주문 수량
        self.long_order_num_divide = int(self.param['long_order_num_divide'])  # 매수 주문 분할 횟수
        self.long_time_gap = int(self.param['long_time_gap'])  # 매수 주문 간 시간 간격
        self.long_order_start = None  # 매수 주문 시작 시간
        self.long_base_price = None  # 매수 기준 가격
        self.long_flag = False  # 매수 모드 활성화 여부
        self.high_price = None  # 고가
        self.low_price = None  # 저가

        self.report_channel = False  # 채널 보고 여부

        self.loss_cut_pct = float(self.param['loss_cut_pct'])  # 손절매 비율
        self.loss_cut_order_start = None  # 손절매 주문 시작 시간
        self.loss_cut_flag = False  # 손절매 모드 활성화 여부
        self.loss_cut_volume = 0  # 손절매 주문 수량 (손절 발동 시점의 position으로 확정)

        self.tick_size = float(self.param['tick_size'])  # 틱 크기

        # 시간 이벤트 초기화
        self.volume_check_time = self.clock_list[0]  # 거래량 확인 시간
        self.channel_fix_time = self.param['channel_fix_time']  # 채널 고정 시간

        # 포지션 관리
        self.position = 0  # 현재 포지션 (접수된 진입 수량 기준)

        # 백테스트용 변수
        self.backtest_volume = int(self.param['long_order_volume'])  # 백테스트용 거래량
        self.backtest_loss_cut_volume = int(self.param['long_order_volume'])  # 백테스트용 손절매 거래량
   
    async def _check_and_order(self):
        """
        매수 주문 실행 메서드

        현재 시간이 매수 주문 시간에 도달했는지 확인하고, 조건이 맞으면 매수 주문을 실행합니다.
        주문은 분할 실행되며, 각 주문은 지정된 시간에 실행됩니다.

        Returns:
            None
        """
        # 가격 정보가 없으면 실행하지 않음
        if self.price == None:
            return None

        # 현재 시간 확인
        now = datetime.datetime.now(KST)
        hms = now.strftime("%H%M%S.%f")

        # 각 분할 주문에 대해 실행 시간 확인
        for i in range(self.long_order_num_divide):
            if float(hms) >= self.long_order_time_list[i] \
               and (not self.long_order_sent_list[i]) \
               and (not self.loss_cut_flag):
                logging.info(self.strategy_name)
                logging.info("Long entry " + str(i))

                self.long_order_sent_list[i] = True

                if self.bullet > 0 and self.long_order_volume > 0:
                    long_order_volume = math.floor(self.long_order_volume/(self.long_order_num_divide - i))
                    if long_order_volume == 0:
                        long_order_volume = 1

                    logging.info("Buy!")
                    logging.info("Bullet: " + str(self.bullet))
                    logging.info("")

                    if self.real and self.order_service is not None:
                        try:
                            # `OrderService`가 이미 브로커에 바인딩되어 있으므로 전략은 broker를 모릅니다.
                            ord_rsp = await self.order_service.place_future_order(
                                symbol=self.futures_code,
                                quantity=long_order_volume,
                                price=float(round(self.price + self.tick_size * 4, 2)),
                                side=BuySell.BUY,
                                price_type=PriceType.LIMIT,
                                account=self.account_dict[self.futures_code],
                                time_in_force="DAY",
                            )
                        except Exception as e:
                            ord_rsp = None
                            logging.error(f"Order error: {e}")

                        if self._accepted(ord_rsp):
                            logging.info("Check Register Order")
                            logging.info(ord_rsp)
                            logging.info("")

                            order_num = ord_rsp
                            if isinstance(ord_rsp, dict):
                                output = ord_rsp.get("output") or {}
                                if isinstance(output, dict):
                                    order_num = output.get("ODNO") or output.get("order_id") or output.get("order_num") or ord_rsp
                                else:
                                    order_num = ord_rsp.get("order_num") or ord_rsp

                            self.book.register_order_info(
                                self.futures_code,
                                order_num, None,
                                BuySell.BUY,
                                str(round(self.price+self.tick_size*4,2)),
                                long_order_volume,
                                NewOrCancelOrReplace.NEW
                            )

                            self.position += long_order_volume
                    elif not self.real:
                        # dry-run: 주문을 내지 않고 체결된 것으로 간주
                        self.position += long_order_volume
                    self.long_order_volume = self.long_order_volume - long_order_volume
                    self.bullet = self.bullet - 1

                    self.book.register_runtest_order_info(
                        self.futures_code,
                        None, None,
                        BuySell.BUY,
                        str(round(self.price,2)),
                        long_order_volume,
                        NewOrCancelOrReplace.NEW
                    )

                    self.backtest_volume = self.backtest_volume - long_order_volume


    async def _loss_cut(self):
        """
        손절매 주문 실행 메서드

        현재 시간이 손절매 주문 시간에 도달했는지 확인하고, 조건이 맞으면 손절매 주문을 실행합니다.
        손절매 주문도 분할 실행되며, 각 주문은 지정된 시간에 실행됩니다.

        Returns:
            None
        """
        # 가격 정보가 없으면 실행하지 않음
        if self.price == None:
            return None

        # 현재 시간 확인
        now = datetime.datetime.now(KST)
        hms = now.strftime("%H%M%S.%f")

        # 각 분할 주문에 대해 실행 시간 확인
        for i in range(self.long_order_num_divide):
            if float(hms) >= self.loss_cut_order_time_list[i] \
               and (not self.loss_cut_order_sent_list[i]):
                logging.info(self.strategy_name)
                logging.info("Loss Cut entry " + str(i))

                self.loss_cut_order_sent_list[i] = True

                if self.loss_cut_volume > 0:
                    loss_cut_order_volume = math.floor(self.loss_cut_volume/(self.long_order_num_divide - i))
                    if loss_cut_order_volume == 0:
                        loss_cut_order_volume = 1

                    logging.info("Sell!")
                    logging.info("Bullet: " + str(self.bullet))
                    logging.info("")

                    if self.real and self.order_service is not None:
                        try:
                            ord_rsp = await self.order_service.place_future_order(
                                symbol=self.futures_code,
                                quantity=loss_cut_order_volume,
                                price=float(round(self.price - self.tick_size * 4, 2)),
                                side=BuySell.SELL,
                                price_type=PriceType.LIMIT,
                                account=self.account_dict[self.futures_code],
                                time_in_force="DAY",
                                risk_reducing=True,
                            )
                        except Exception as e:
                            ord_rsp = None
                            logging.error(f"Order error: {e}")

                        if self._accepted(ord_rsp):

                            logging.info("Check Register Order")
                            logging.info(ord_rsp)
                            logging.info("")

                            order_num = ord_rsp
                            if isinstance(ord_rsp, dict):
                                output = ord_rsp.get("output") or {}
                                if isinstance(output, dict):
                                    order_num = output.get("ODNO") or output.get("order_id") or output.get("order_num") or ord_rsp
                                else:
                                    order_num = ord_rsp.get("order_num") or ord_rsp

                            self.book.register_order_info(
                                self.futures_code,
                                order_num, None,
                                BuySell.SELL,
                                str(round(self.price-self.tick_size*4,2)),
                                loss_cut_order_volume,
                                NewOrCancelOrReplace.NEW
                            )

                            self.position -= loss_cut_order_volume
                    elif not self.real:
                        self.position -= loss_cut_order_volume
                    self.loss_cut_volume = self.loss_cut_volume - loss_cut_order_volume

                    self.book.register_runtest_order_info(
                        self.futures_code,
                        None, None,
                        BuySell.SELL,
                        str(round(self.price,2)),
                        loss_cut_order_volume,
                        NewOrCancelOrReplace.NEW
                    )

                    self.backtest_loss_cut_volume = self.backtest_loss_cut_volume - loss_cut_order_volume

        return


    @staticmethod
    def _accepted(ord_rsp) -> bool:
        """브로커가 주문을 접수했는지(rt_cd == '0') 판정합니다."""
        return isinstance(ord_rsp, dict) and ord_rsp.get("rt_cd") == "0"

    async def on_quote(self, quote):
        """
        시세 데이터 처리 메서드

        시세 데이터가 수신될 때마다 호출되며, 매수 주문 또는 손절매 주문 실행 여부를 확인합니다.

        Args:
            quote: 수신된 시세 데이터 객체

        Returns:
            None
        """
        # 매수 모드가 활성화되어 있고 손절매 모드가 아닌 경우 매수 주문 확인
        if self.long_order_start != None and self.long_flag and (not self.loss_cut_flag):
            await self._check_and_order()

        if self.loss_cut_order_start != None and self.loss_cut_flag:
            await self._loss_cut()

        return None

    def _initiate_time_lists(self):
        """
        매수 주문 시간 목록 초기화 메서드

        매수 주문을 분할 실행하기 위한 시간 목록을 초기화합니다.
        각 분할 주문의 실행 시간을 계산하여 목록에 저장합니다.

        Returns:
            None
        """
        # 주문 실행 여부 및 시간 목록 초기화
        self.long_order_sent_list = []
        self.long_order_time_list = []

        # 분할 횟수가 0 이하면 실행하지 않음
        if self.long_order_num_divide <= 0:
            return

        # 각 분할 주문에 대한 시간 계산
        for i in range(self.long_order_num_divide):
            # 주문 실행 여부 초기화 (아직 실행되지 않음)
            self.long_order_sent_list.append(False)

            # 시작 시간을 밀리초로 변환
            long_order_start_in_milisec = (math.floor(self.long_order_start / 10000)*3600\
                                      + math.floor((self.long_order_start % 10000)/100) * 60\
                                      + self.long_order_start % 100) * 1000

            # i번째 주문 시간 계산 (시작 시간 + i * 시간 간격)
            long_order_time_in_milisec = long_order_start_in_milisec + i * self.long_time_gap

            # 밀리초를 HHMMSS 형식으로 변환
            long_order_time_in_format = math.floor((long_order_time_in_milisec/1000)/3600) * 10000\
                                        + math.floor(((long_order_time_in_milisec/1000)%3600)/60) * 100\
                                        + (long_order_time_in_milisec/1000) % 60

            # 주문 시간 목록에 추가
            self.long_order_time_list.append(long_order_time_in_format)
            logging.info("long_order_time_list " + str(long_order_time_in_format))

        return


    def _initiate_loss_cut_time_lists(self):
        """
        손절매 주문 시간 목록 초기화 메서드

        손절매 주문을 분할 실행하기 위한 시간 목록을 초기화합니다.
        각 분할 주문의 실행 시간을 계산하여 목록에 저장합니다.

        Returns:
            None
        """
        # 주문 실행 여부 및 시간 목록 초기화
        self.loss_cut_order_sent_list = []
        self.loss_cut_order_time_list = []

        # 분할 횟수가 0 이하면 실행하지 않음
        if self.long_order_num_divide <= 0:
            return

        # 각 분할 주문에 대한 시간 계산
        for i in range(self.long_order_num_divide):
            # 주문 실행 여부 초기화 (아직 실행되지 않음)
            self.loss_cut_order_sent_list.append(False)

            # 시작 시간을 밀리초로 변환
            loss_cut_order_start_in_milisec = (math.floor(self.loss_cut_order_start / 10000)*3600\
                                      + math.floor((self.loss_cut_order_start % 10000)/100) * 60\
                                      + self.loss_cut_order_start % 100) * 1000

            # i번째 주문 시간 계산 (시작 시간 + i * 시간 간격)
            loss_cut_order_time_in_milisec = loss_cut_order_start_in_milisec + i * self.long_time_gap

            # 밀리초를 HHMMSS 형식으로 변환
            loss_cut_order_time_in_format = math.floor((loss_cut_order_time_in_milisec/1000)/3600) * 10000\
                                        + math.floor(((loss_cut_order_time_in_milisec/1000)%3600)/60) * 100\
                                        + (loss_cut_order_time_in_milisec/1000) % 60

            # 주문 시간 목록에 추가
            self.loss_cut_order_time_list.append(loss_cut_order_time_in_format)
            logging.info("loss_cut_order_time_list " + str(loss_cut_order_time_in_format))

        return


    async def on_filled(self, filled):
        """
        체결 정보 처리 메서드

        체결 정보가 수신될 때마다 호출되며, 채널 업데이트, 채널 브레이크아웃 확인, 
        매수 주문 실행, 손절매 조건 확인 등을 수행합니다.

        Args:
            filled: 수신된 체결 정보 객체

        Returns:
            None
        """
        # 현재 시간 및 가격 정보 업데이트
        now = datetime.datetime.now(KST)
        hms = now.strftime("%H%M%S")
        self.price = float(filled.price)
        self.health_check_value = str(filled.price)

        # 채널 고정 시간 이전에는 채널 업데이트
        if int(hms) <= int(self.channel_fix_time):
            # 고가와 저가 업데이트
            self.high_price = float(filled.high)
            self.low_price = float(filled.low)

        # 채널 고정 시간 이후에 채널 정보 보고 (최초 1회)
        if int(hms) > int(self.channel_fix_time) and (not self.report_channel):
            # 채널 정보 로깅
            logging.info(self.strategy_name)
            logging.info("high price: " + str(self.high_price))

            # 채널 보고 완료 표시
            self.report_channel = True

        # 채널 브레이크아웃 확인 (채널 고정 시간 이후, 아직 매수 모드가 아닌 경우)
        if int(hms) > int(self.channel_fix_time) and (not self.long_flag):
            if self.price != None and self.high_price != None:
                if self.price > self.high_price and self.bullet > 0:

                    logging.info(self.strategy_name)
                    logging.info("Start Buy!")

                    self.long_base_price = self.high_price
                    self.long_flag = True
                    self.long_order_start = int(hms)

                    self._initiate_time_lists()

        # 매수 모드가 활성화되어 있고 손절매 모드가 아닌 경우
        if self.long_order_start != None and self.long_flag and (not self.loss_cut_flag):
            await self._check_and_order()

            if self.price <= (self.long_base_price * (1 - self.loss_cut_pct / 100))\
               and self.position > 0:

                logging.info(self.strategy_name)
                logging.info("Start Sell Loss Cut!")

                self.loss_cut_flag = True
                self.loss_cut_volume = self.position
                self.loss_cut_order_start = int(hms)

                self._initiate_loss_cut_time_lists()

        # 손절매 모드가 활성화된 경우 손절매 주문 실행
        if self.loss_cut_order_start != None and self.loss_cut_flag:
            await self._loss_cut()

        return None


    def intraday_restore_until(self, now_hms: int) -> Optional[str]:
        """장중 재시작 시 복원에 필요한 분봉의 마지막 시각(HHMMSS). 채널이 이미 잡혔거나 고정 전이면 None."""
        if self.high_price is not None or now_hms <= int(self.channel_fix_time):
            return None
        return str(self.channel_fix_time).zfill(6)

    def restore_intraday(self, bars: List[dict]) -> bool:
        """채널 고정 시각 전에 시작하지 못했으면 오늘 분봉으로 그때까지의 고가/저가(채널)를 복원합니다."""
        window = [b for b in bars if int(b["time"]) < int(self.channel_fix_time) and b.get("high")]
        if not window:
            return False
        self.high_price = max(float(b["high"]) for b in window)
        self.low_price = min(float(b["low"]) for b in window)
        logging.info(f"{self.strategy_name} channel restored from {len(window)} bars: high={self.high_price} low={self.low_price}")
        return True

    async def on_ack(self, ack):
        """
        주문 응답 처리 메서드

        주문 응답(ACK)을 처리하고 주문 관리 객체에 전달합니다.

        Args:
            ack: 수신된 주문 응답 객체

        Returns:
            None
        """
        # NOTE: ACK 라우팅은 `ExecutionReportRouter`가 수행하고,
        # 실제 콜백 실행은 `StrategyExecutor`가 담당합니다.
        # 따라서 전략 내부에서 `ack.broker` 등의 필터링을 하면 오히려 누락/혼선이 생길 수 있어 금지합니다.
        # 주문 관리 객체에 응답 처리
        self.book.process_ack(ack)
        return None

    async def on_clock(self, clock):
        """
        시간 이벤트 처리 메서드

        시간 이벤트가 발생할 때마다 호출됩니다.
        현재 이 전략에서는 시간 이벤트를 특별히 처리하지 않습니다.

        Args:
            clock: 시간 이벤트 정보

        Returns:
            None
        """
        # 시간 이벤트 로깅

        # 거래량 확인 시간인 경우 (현재는 특별한 처리 없음)
        if clock == self.volume_check_time:
            return None

        return None
    
    def print_summary(self):
        """전략 실행 요약"""
        print(f"\n=== {self.strategy_name} 전략 요약 ===")
        print(f"실행 모드: {'실전' if self.real else '테스트'}")
        print(f"최종 포지션: {self.position}")
        print(f"남은 bullet: {self.bullet}")
        print(f"백테스트 남은 거래량: {self.backtest_volume}")
        print(f"백테스트 남은 손절량: {self.backtest_loss_cut_volume}")
        # 필요하면 더 많은 요약 정보 추가