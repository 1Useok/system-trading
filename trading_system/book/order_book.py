"""
주문 관리 및 주문 상태 추적을 위한 모듈

이 모듈은 주문 정보를 관리하고, 주문 상태를 추적하며, ACK(응답) 처리 및 기록 기능을 제공합니다.
주문 생성, 취소, 정정 및 체결 정보 관리 등의 기능을 포함합니다.
"""

from trading_system.types.enums import AckType, BuySell, NewOrCancelOrReplace
from trading_system.types.events import FuturesSimpleFilled, FuturesSimpleOrderAck

from enum import Enum
import os
import datetime
import logging

"""
주문 상태를 나타내는 Enum 클래스
주문의 현재 상태를 추적하기 위해 사용됩니다.
"""
class OrderStatus(Enum):
    ACKRECEIVED = 1  # 주문 접수됨
    LIVE = 0         # 주문 활성화 상태
    REJECTED = 2     # 주문 거절됨

"""
주문 정보를 담는 클래스
개별 주문의 모든 관련 정보와 상태를 저장하고 관리합니다.
"""
class OrderInfo():
    def __init__(self):
        # NOTE: 이전에는 이 속성들이 __init__ 없이 클래스 속성으로 선언되어 있었다.
        # 특히 filled_price_list/filled_volume_list는 가변(list) 객체라서,
        # 모든 OrderInfo 인스턴스가 "같은" 리스트 객체를 공유하게 되어
        # 한 주문의 체결 내역에 다른 주문(다른 종목 포함)의 체결이 함께 누적되는
        # 버그가 있었다(클래스 속성에 .append()하면 인스턴스가 아니라 클래스 자체가
        # 바뀐다). 반드시 인스턴스별로 새로 만들어야 한다.
        self.inst_code = ''                    # 종목 코드
        self.order_num = -1                    # 주문 번호
        self.original_order_num = -1           # 원주문 번호
        self.buysell = BuySell.NA              # 매수/매도 구분
        self.order_price = -1                  # 주문 가격
        self.order_volume = -1                 # 주문 수량
        self.filled_price_list = []            # 체결 가격 리스트
        self.filled_volume_list = []           # 체결 수량 리스트
        self.cancel_or_replace_volume = 0      # 취소/정정 수량
        self.cancel_or_replace_sent_volume = 0 # 취소/정정 요청된 수량
        self.status = OrderStatus.LIVE         # 주문 상태

"""
주문관리를 위한 Book 클래스
주문 정보를 관리하고, 주문 상태를 추적하며, ACK(응답) 처리 및 기록 기능을 제공합니다.
주문 생성, 취소, 정정 및 체결 정보 관리 등의 기능을 포함합니다.
"""
class Book():

    def __init__(self, _strategy_name, _inst_list):
        """
        Book 클래스 초기화
        Args:
            _strategy_name: 전략 이름
            _inst_list: 종목 리스트
        """
        self.strategy_name = _strategy_name
        self.inst_list = _inst_list
        self.order_book_dict = {}            # 주문 관리 딕셔너리
        self.not_registered_ack = {}         # 미등록 ACK 관리
        # 현재 날짜 가져오기
        now = datetime.datetime.now()
        ymd = now.strftime("%Y%m%d")

        # ACK 기록 파일 경로 설정 및 생성
        if not os.path.exists(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/acks/" + ymd):
            os.makedirs(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/acks/" + ymd)

        self.ack_file_name = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/acks/" + ymd + "/"\
                                + self.strategy_name + "_acks.csv"

        # 체결 기록 파일 경로 설정 및 생성
        if not os.path.exists(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/filled/" + ymd):
            os.makedirs(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/filled/" + ymd)

        self.filled_file_name = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/filled/" + ymd + "/"\
                                + self.strategy_name + "_filled.csv"

        # 실행 테스트 기록 파일 경로 설정 및 생성
        if not os.path.exists(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/runtest/" + ymd):
            os.makedirs(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/runtest/" + ymd)

        self.runtest_file_name = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))\
                              + "/record/runtest/" + ymd + "/"\
                                + self.strategy_name + "_runtest.csv"

        # 종목별 주문 관리 딕셔너리 초기화
        for inst_code in self.inst_list:
            self.order_book_dict[inst_code] = []

        return

    def _register_new_order_info(self,\
                            _inst_code,\
                            _order_num, _original_order_num,\
                            _buysell,\
                            _order_price, _order_volume,\
                            ):
        """
        신규 주문 정보 등록
        Args:
            _inst_code: 종목 코드
            _order_num: 주문 번호
            _original_order_num: 원주문 번호
            _buysell: 매수/매도 구분
            _order_price: 주문 가격
            _order_volume: 주문 수량
        """
        order_info = OrderInfo()
        order_info.inst_code = _inst_code
        order_info.order_num = _order_num
        order_info.original_order_num = _original_order_num
        order_info.buysell = _buysell
        order_info.order_price = float(_order_price)
        order_info.order_volume = int(_order_volume)

        self.order_book_dict[_inst_code].append(order_info)

        return

    def _register_cancel_order_info(self,\
                            _inst_code,\
                            _order_num, _original_order_num,\
                            _buysell,\
                            _order_price, _order_volume
                            ):
        """
        취소 주문 정보 등록
        Args:
            _inst_code: 종목 코드
            _order_num: 주문 번호
            _original_order_num: 원주문 번호
            _buysell: 매수/매도 구분
            _order_price: 주문 가격
            _order_volume: 주문 수량
        """
        if _inst_code in self.order_book_dict:
            for order_info in self.order_book_dict[_inst_code]:
                if order_info.order_num == _original_order_num:
                    order_info.cancel_or_replace_sent_volume\
                        = order_info.cancel_or_replace_sent_volume + int(_order_volume)

        return

    def register_order_info(self,\
                            _inst_code,\
                            _order_num, _original_order_num,\
                            _buysell,\
                            _order_price, _order_volume,\
                            _new_or_cancel_or_replace
                            ):
        """
        주문 정보 등록 (신규/취소/정정)
        Args:
            _inst_code: 종목 코드
            _order_num: 주문 번호
            _original_order_num: 원주문 번호
            _buysell: 매수/매도 구분
            _order_price: 주문 가격
            _order_volume: 주문 수량
            _new_or_cancel_or_replace: 주문 유형(신규/취소/정정)
        """
        if _new_or_cancel_or_replace == NewOrCancelOrReplace.NEW:
            self._register_new_order_info(_inst_code,\
                                          _order_num, _original_order_num,\
                                          _buysell,\
                                          _order_price, _order_volume,\
                                         )
            return
        elif _new_or_cancel_or_replace == NewOrCancelOrReplace.CANCEL:
            self._register_cancel_order_info(_inst_code,\
                                          _order_num, _original_order_num,\
                                          _buysell,\
                                          _order_price, _order_volume,\
                                         )
            return
        elif _new_or_cancel_or_replace == NewOrCancelOrReplace.REPLACE:
            self._register_new_order_info(_inst_code,\
                                          _order_num, _original_order_num,\
                                          _buysell,\
                                          _order_price, _order_volume,\
                                         )
            self._register_cancel_order_info(_inst_code,\
                                          _order_num, _original_order_num,\
                                          _buysell,\
                                          _order_price, _order_volume,\
                                         )
            return

        return

    def _register_runtest_new_order_info(self,\
                            _inst_code,\
                            _order_num, _original_order_num,\
                            _buysell,\
                            _order_price, _order_volume,\
                            ):
        """
        실행 테스트용 신규 주문 정보 등록
        Args:
            _inst_code: 종목 코드
            _order_num: 주문 번호
            _original_order_num: 원주문 번호
            _buysell: 매수/매도 구분
            _order_price: 주문 가격
            _order_volume: 주문 수량
        """
        now = datetime.datetime.now()
        hms = now.strftime("%H%M%S.%f")

        row = hms + ","\
              + str(AckType.Nothing) + ","\
              + str(None) + ","\
              + str(_inst_code) + ","\
              + str(_order_num) + ","\
              + str(_original_order_num) + ","\
              + str(NewOrCancelOrReplace.NEW) + ","\
              + str(_order_price) + ","\
              + str(_order_volume) + ","\
              + str(_buysell.value) + "\n"

        logging.info("_record_runtest_order")
        logging.info(self.runtest_file_name)
        logging.info(row)

        with open(self.runtest_file_name, "a") as file:
            file.write(row)
        file.close()

        return

    def register_runtest_order_info(self,\
                            _inst_code,\
                            _order_num, _original_order_num,\
                            _buysell,\
                            _order_price, _order_volume,\
                            _new_or_cancel_or_replace
                            ):
        """
        실행 테스트용 주문 정보 등록
        Args:
            _inst_code: 종목 코드
            _order_num: 주문 번호
            _original_order_num: 원주문 번호
            _buysell: 매수/매도 구분
            _order_price: 주문 가격
            _order_volume: 주문 수량
            _new_or_cancel_or_replace: 주문 유형(신규/취소/정정)
        """
        #우선 New만 구현
        if _new_or_cancel_or_replace == NewOrCancelOrReplace.NEW:
            self._register_runtest_new_order_info(_inst_code,\
                                          _order_num, _original_order_num,\
                                          _buysell,\
                                          _order_price, _order_volume,\
                                         )
            return
        elif _new_or_cancel_or_replace == NewOrCancelOrReplace.CANCEL:
            return
        elif _new_or_cancel_or_replace == NewOrCancelOrReplace.REPLACE:
            return
        return

    def _check_and_remove_order_from_dict(self, ack):
        """
        주문 완료 또는 거절된 주문을 딕셔너리에서 제거
        Args:
            ack: ACK 정보
        """
        if ack.inst_code in self.order_book_dict:

            index_to_remove = []

            for i in range(len(self.order_book_dict[ack.inst_code])):
                order_info = (self.order_book_dict[ack.inst_code])[i]

                order_volume = order_info.order_volume
                filled_volume = sum(order_info.filled_volume_list)
                canceled_or_replaced_volume = order_info.cancel_or_replace_volume

                if order_volume == (filled_volume + canceled_or_replaced_volume):
                    index_to_remove.append(i)

                if order_info.status == OrderStatus.REJECTED:
                    index_to_remove.append(i)

            for i in range(len(index_to_remove)):
                self.order_book_dict[ack.inst_code].pop(index_to_remove[len(index_to_remove)-1-i])

        return

    def _update_filled_ack(self, ack):
        """
        체결 ACK 정보 업데이트
        Args:
            ack: ACK 정보
        """
        if ack.inst_code in self.order_book_dict:
            for order_info in self.order_book_dict[ack.inst_code]:
                if order_info.order_num == ack.order_num:
                    order_info.filled_price_list.append(float(ack.filled_price))
                    order_info.filled_volume_list.append(int(ack.filled_volume))
                    self._record_ack(ack, self.filled_file_name)

        self._check_and_remove_order_from_dict(ack)

        return

    def _update_canceled_ack(self, ack):
        """
        취소 ACK 정보 업데이트
        Args:
            ack: ACK 정보
        """
        if ack.inst_code in self.order_book_dict:
            for order_info in self.order_book_dict[ack.inst_code]:
                if order_info.order_num == ack.original_order_num:
                    order_info.cancel_or_replace_volume\
                        = order_info.cancel_or_replace_volume + int(ack.cancel_or_replace_volume)
                    self._record_ack(ack, self.ack_file_name)

        self._check_and_remove_order_from_dict(ack)

        return

    def _update_received_ack(self, ack):
        """
        접수 ACK 정보 업데이트
        Args:
            ack: ACK 정보
        """
        if ack.inst_code in self.order_book_dict:
            for order_info in self.order_book_dict[ack.inst_code]:
                if order_info.order_num == ack.order_num:
                    order_info.status = OrderStatus.ACKRECEIVED
                    self._record_ack(ack, self.ack_file_name)
        return

    def _update_reject_ack(self, ack):
        """
        거절 ACK 정보 업데이트
        Args:
            ack: ACK 정보
        """
        if ack.inst_code in self.order_book_dict:
            for order_info in self.order_book_dict[ack.inst_code]:
                if order_info.order_num == ack.order_num:
                    order_info.status = OrderStatus.REJECTED
                    self._record_ack(ack, self.ack_file_name)

        self._check_and_remove_order_from_dict(ack)

        return

    def _record_ack(self, ack, file_name):
        """
        ACK 정보를 파일에 기록
        Args:
            ack: ACK 정보
            file_name: 기록할 파일 이름
        """
        now = datetime.datetime.now()
        hms = now.strftime("%H%M%S.%f")

        row = hms + ","\
              + str(ack.ack_type) + ","\
              + str(ack.account) + ","\
              + str(ack.inst_code) + ","\
              + str(ack.order_num) + ","\
              + str(ack.original_order_num) + ","\
              + str(ack.cancel_or_replace_volume) + ","\
              + str(ack.filled_price) + ","\
              + str(ack.filled_volume) + ","\
              + str(ack.buysell) + "\n"

        logging.info("_record_ack")
        logging.info(file_name)
        logging.info(row)

        with open(file_name, "a") as file:
            file.write(row)
        file.close()

    def process_ack(self, ack):
        """
        ACK 처리 메인 함수
        Args:
            ack: 처리할 ACK 정보
        """
        try:

            # 방어: 시장 틱이 ACK 처리로 유입되면 장부/포지션 정합성이 깨질 수 있음
            if isinstance(ack, FuturesSimpleFilled):
                raise TypeError("Market tick (FuturesSimpleFilled) must not be processed as an ACK")
            if not isinstance(ack, FuturesSimpleOrderAck):
                raise TypeError(f"Invalid ack type for Book.process_ack: {type(ack)}")

            # 타입 정규화: 일부 입력은 AckType이 아닌 값(int/str)일 수 있음
            if not isinstance(ack.ack_type, AckType):
                try:
                    ack.ack_type = AckType(int(ack.ack_type))
                except Exception:
                    ack.ack_type = AckType.Nothing

            if ack.ack_type == AckType.Filled:
                self._update_filled_ack(ack)
                return
            elif ack.ack_type == AckType.Replaced:
                self._update_received_ack(ack)
                self._update_canceled_ack(ack)
                return
            elif ack.ack_type == AckType.Canceled:
                self._update_canceled_ack(ack)
                return
            elif ack.ack_type == AckType.Rejected:
                self._update_reject_ack(ack)
                return
            elif ack.ack_type == AckType.Received:
                self._update_received_ack(ack)
                return

        except:
            print("Book Process Ack Error")

        return
