from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple, Callable
from datetime import datetime
import pandas as pd

from trading_system.types.enums import BuySell, MarketType, OrderType, PriceType, QuoteType
from trading_system.types.events import FuturesSimpleOrderAck, FuturesSimpleQuote

class BaseApi(ABC):
    """통합 증권사 API 기본 인터페이스"""

    @abstractmethod
    def __init__(self, config: dict):
        """API 초기화"""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """API 연결 상태 확인"""
        pass

    @abstractmethod
    async def get_all_accounts(self) -> List[str]:
        """모든 계좌번호를 리스트로 반환"""
        pass

    @abstractmethod
    async def get_access_token(self) -> str:
        """토큰 발급/갱신"""
        pass

    @abstractmethod
    async def request_data(self, tr_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 요청 공통 인터페이스"""
        pass

    @abstractmethod
    async def get_account_info(self) -> List[Dict[str, str]]:
        """계좌 목록 조회"""
        pass

    # 주식 관련
    @abstractmethod
    async def get_stock_balance(self, account: str) -> pd.DataFrame:
        pass

    @abstractmethod
    async def get_stock_orders(self, account: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        pass

    @abstractmethod
    async def get_stock_price(self, symbol: str) -> Dict[str, float]:
        pass

    @abstractmethod
    async def order_stock(self, account: str, symbol: str, quantity: int, price: float, side: BuySell, price_type: PriceType = PriceType.LIMIT, order_type: OrderType = OrderType.NORMAL) -> str:
        pass

    @abstractmethod
    async def cancel_stock_order(self, account: str, order_no: str, quantity: int) -> bool:
        pass

    # 선물 관련
    @abstractmethod
    async def get_future_account_balance(self, account: str, base_date: str) -> Tuple[pd.DataFrame, Dict]:
        pass

    @abstractmethod
    async def get_future_orders(self, account: str, start_date: str, end_date: str) -> Tuple[pd.DataFrame, Dict]:
        pass

    @abstractmethod
    async def get_future_price(self, symbol: str) -> Dict[str, float]:
        pass

    @abstractmethod
    async def order_future(self, account: str, symbol: str, quantity: int, price: float, side: BuySell, price_type: PriceType = PriceType.LIMIT, order_type: OrderType = OrderType.NORMAL) -> str:
        pass

    @abstractmethod
    async def cancel_future_order(self, account: str, order_no: str, quantity: int) -> bool:
        pass

    # 실시간 시세/체결 구독 관련
    @abstractmethod
    def subscribe_order(self, callback: Callable[[FuturesSimpleOrderAck], None]) -> None:
        pass

    @abstractmethod
    def subscribe_quote(self, symbol: str, callback: Callable[[FuturesSimpleQuote], None]) -> None:
        pass

    @abstractmethod
    def unsubscribe_quote(self, symbol: str) -> None:
        pass

    @abstractmethod
    def unsubscribe_all(self) -> None:
        pass