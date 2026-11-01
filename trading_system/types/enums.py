"""공통 Enum 정의.

이 파일의 Enum들은 `quote`/`order`/`strategy`/`book`/`api` 전반에서 공통으로 사용됩니다.
"""

from __future__ import annotations

from enum import Enum


class BuySell(Enum):
    BUY = "02"
    SELL = "01"
    NA = "00"


class PriceType(Enum):
    """가격 유형"""

    LIMIT = "01"  # 지정가
    MARKET = "02"  # 시장가


class OrderType(Enum):
    """주문 유형"""

    NORMAL = "01"  # 일반
    IOC = "03"  # IOC
    FOK = "04"  # FOK


class MarketType(Enum):
    """시장 구분"""

    STOCK = "stock"  # 주식
    FUTURE = "future"  # 선물


class QuoteType(Enum):
    """시세 구분"""

    TICK = "tick"  # 체결 시세
    FILLED = "filled"  # 체결
    ORDERBOOK = "orderbook"  # 호가
    EXECUTION = "execution"  # 체결통보(내 주문 체결/거절/접수 - H0IFCNI0)


class StreamerStatus(Enum):
    """스트리머 상태 구분"""

    DISCONNECTED = "disconnected"  # 연결 해제됨
    CONNECTING = "connecting"  # 연결 시도 중
    CONNECTED = "connected"  # 연결됨
    ERROR = "error"


class AckType(Enum):
    Filled = 0
    Received = 1
    Canceled = 2
    Replaced = 3
    Rejected = 4
    Nothing = 5


# --- Order Action Enum (for use in strategies, e.g. cb_split_long_only_pct) ---
class NewOrCancelOrReplace(Enum):
    NEW = "new"
    CANCEL = "cancel"
    REPLACE = "replace"
