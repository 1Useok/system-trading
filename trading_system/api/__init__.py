"""
브로커 API 클라이언트 (한국투자증권 KIS REST/WebSocket)
"""

from trading_system.api.base import BaseApi
from trading_system.types import *

# NOTE:
# 공통 타입은 `trading_system.types`에 모여 있으며(`Enum`/이벤트 메시지),
# `trading_system.api`는 외부 API 클라이언트(KIS 등) 구현을 포함합니다.
# 테스트/도구 환경에서 `websockets` 같은 선택 의존성이 강제 import 되어 깨지지 않도록
# KIS 관련 모듈은 optional import로 둡니다.
try:
    from trading_system.api.kis.rest_api import RestApi
except Exception:  # pragma: no cover
    RestApi = None

try:
    from trading_system.api.kis.rest_streamer import RestStreamer
except Exception:  # pragma: no cover
    RestStreamer = None

try:
    from trading_system.api.kis.token_manager import KISTokenManager
except Exception:  # pragma: no cover
    KISTokenManager = None

__version__ = "1.0.0"
__all__ = [
    "BaseApi",
    "RestApi",
    "RestStreamer",
    "KISTokenManager",
]