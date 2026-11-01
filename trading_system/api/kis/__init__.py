"""
KIS (한국투자증권) API 구현체
"""

from trading_system.api.kis.rest_api import RestApi
from trading_system.api.kis.token_manager import KISTokenManager

# KIS 실시간 스트리밍은 `websockets` 등 optional dependency가 필요할 수 있으므로
# import 시점에서 실패하지 않도록 optional import 처리합니다.
try:
    from trading_system.api.kis.rest_streamer import RestStreamer
except Exception:  # pragma: no cover
    RestStreamer = None

__all__ = ["RestApi", "RestStreamer", "KISTokenManager"]