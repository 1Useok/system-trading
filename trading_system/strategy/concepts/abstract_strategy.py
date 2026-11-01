# -*- coding: utf-8 -*-
"""
전략 추상 클래스 모듈

이 모듈은 모든 트레이딩 전략의 기본이 되는 추상 클래스를 정의합니다.
모든 구체적인 전략 클래스는 이 추상 클래스를 상속받아 구현해야 합니다.
"""
from abc import *

class Strategy(metaclass=ABCMeta):
    """
    전략 추상 클래스

    모든 트레이딩 전략의 기본 구조를 정의하는 추상 클래스입니다.
    시세 데이터, 체결 정보, 주문 응답, 시간 이벤트 등을 처리하는 메서드를 포함합니다.

    Attributes:
        strategy_name (str): 전략 이름
        real (bool): 실제 거래 모드 여부 (True: 실제 거래, False: 테스트 모드)
        quote_list (list): 시세 데이터 목록
        clock_list (list): 시간 이벤트 목록
        account_dict (dict): 계좌 정보 사전
        bullet (int): 주문 가능 횟수
        order_module_dict (dict): 주문 모듈 사전
        feature_dict (dict): 기능 사전
        param (dict): 전략 파라미터
        broker (str): 브로커 정보
        health_check_value (str): 상태 확인 값
    """

    def __init__(self, _strategy_name, _real, _quote_list, _clock_list, _account_dict,
                       _bullet, _order_module_dict, _feature_dict, _param, _broker):
        """
        Strategy 클래스 초기화

        Args:
            _strategy_name (str): 전략 이름
            _real (bool): 실제 거래 모드 여부
            _quote_list (list): 시세 데이터 목록
            _clock_list (list): 시간 이벤트 목록
            _account_dict (dict): 계좌 정보 사전
            _bullet (int): 주문 가능 횟수
            _order_module_dict (dict): 주문 모듈 사전
            _feature_dict (dict): 기능 사전
            _param (dict): 전략 파라미터
            _broker (str): 브로커 정보
        """
        self.strategy_name = _strategy_name
        self.real = _real
        self.quote_list = _quote_list
        self.clock_list = _clock_list
        self.account_dict = _account_dict
        self.bullet = _bullet
        self.order_module_dict = _order_module_dict
        self.feature_dict = _feature_dict
        self.param = _param
        self.broker = _broker
        self.health_check_value = "" # MUST HAVE HEALTH CHECK VALUE

    @abstractmethod
    async def on_quote(self, quote):
        """
        시세 데이터 처리 메서드

        시세 데이터가 수신될 때마다 호출되는 메서드입니다.
        각 전략은 이 메서드를 구현하여 시세 데이터에 따른 로직을 처리해야 합니다.

        Args:
            quote: 수신된 시세 데이터 객체

        Returns:
            None
        """
        pass

    @abstractmethod
    async def on_filled(self, filled):
        """
        체결 정보 처리 메서드

        체결 정보가 수신될 때마다 호출되는 메서드입니다.
        각 전략은 이 메서드를 구현하여 체결 정보에 따른 로직을 처리해야 합니다.

        Args:
            filled: 수신된 체결 정보 객체

        Returns:
            None
        """
        pass

    @abstractmethod
    async def on_ack(self, ack):
        """
        주문 응답 처리 메서드

        주문 응답(ACK)이 수신될 때마다 호출되는 메서드입니다.
        각 전략은 이 메서드를 구현하여 주문 응답에 따른 로직을 처리해야 합니다.

        Args:
            ack: 수신된 주문 응답 객체

        Returns:
            None
        """
        pass

    @abstractmethod
    async def on_clock(self, clock):
        """
        시간 이벤트 처리 메서드

        시간 이벤트가 발생할 때마다 호출되는 메서드입니다.
        각 전략은 이 메서드를 구현하여 시간 이벤트에 따른 로직을 처리해야 합니다.

        Args:
            clock: 시간 이벤트 정보

        Returns:
            None
        """
        pass
