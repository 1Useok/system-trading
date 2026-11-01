"""KIS 실시간 스트리밍 입력 → 내부 Market Data 이벤트로 변환하는 Connector.

설계 원칙:
- 이 커넥터는 **시장 데이터만** 책임집니다.
  - `FuturesSimpleQuote` (호가)
  - `FuturesSimpleFilled` (시장 체결/틱)
- `ExecutionReport(ACK)`는 별도 경로(`KISAckConnector` 등)로만 들어오며,
  KIS 스트리밍의 `ack_type` payload는 **완전히 무시(drop)** 합니다.

추가로, 스트리밍은 `websockets` 등의 optional dependency가 필요하므로
import/테스트 환경에서 깨지지 않게 optional import + 명확한 에러 메시지를 제공합니다.
"""

import logging

# KIS 실시간 스트리밍은 `websockets` 등 추가 의존성이 필요할 수 있으므로 optional import 처리
try:
    from trading_system.api.kis.rest_streamer import RestStreamer
except Exception:  # pragma: no cover
    RestStreamer = None

from trading_system.types.events import FuturesSimpleFilled, FuturesSimpleQuote
from trading_system.api.kis.token_manager import KISTokenManager

logger = logging.getLogger(__name__)

class KISConnector:
    def __init__(self, token_manager: 'KISTokenManager', manager=None):
        self.manager = manager
        if RestStreamer is None:
            raise ModuleNotFoundError(
                "KISConnector requires optional dependency for streaming (e.g. `websockets`)."
            )
        self.streamer = RestStreamer(token_manager)
        self.subscribed_symbols = set()

    async def start(self, symbol: str):
        try:
            # NOTE(실서버에서 실제로 확인된 버그): subscribe()가 WebSocket 연결 실패 시
            # 조용히 반환하기만 해서, 반환값을 확인 안 하면 연결이 안 됐는데도 "Subscribed"
            # 로그가 찍혀 시세가 들어오는 것처럼 착각하게 된다.
            ok = await self.streamer.subscribe(symbol, callback=self._on_data)
            if ok:
                self.subscribed_symbols.add(symbol)
                logger.info(f"KISConnector: Subscribed to {symbol}")
            else:
                logger.error(
                    f"KISConnector: Failed to subscribe {symbol} "
                    f"(WebSocket 연결 실패 - streamer status={self.streamer.status})"
                )
        except Exception as e:
            logger.error(f"KISConnector: Failed to subscribe {symbol}: {e}")

    async def stop(self, symbol: str):
        try:
            await self.streamer.unsubscribe(symbol)
            self.subscribed_symbols.discard(symbol)
            logger.info(f"KISConnector: Unsubscribed from {symbol}")
        except Exception as e:
            logger.error(f"KISConnector: Failed to unsubscribe {symbol}: {e}")

    def set_manager(self, manager):
        self.manager = manager

    def _on_data(self, data):
        try:
            # 자동 분배: 시세/체결/ACK
            if "best_ask_price" in data and "best_bid_price" in data:
                quote = FuturesSimpleQuote(
                    inst_code=data.get("symbol", ""),
                    best_ask_price=data.get("best_ask_price", -1),
                    best_bid_price=data.get("best_bid_price", -1),
                    best_ask_volume=data.get("best_ask_volume", -1),
                    best_bid_volume=data.get("best_bid_volume", -1)
                )
                if self.manager:
                    self.manager.on_quote(quote)
            elif "price" in data and "volume" in data:
                filled = FuturesSimpleFilled(
                    inst_code=data.get("symbol", ""),
                    price=data.get("price", -1),
                    volume=data.get("volume", -1),
                    price_chg=data.get("price_chg", -1),
                    open=data.get("open", -1),
                    high=data.get("high", -1),
                    low=data.get("low", -1)
                )
                if self.manager:
                    self.manager.on_filled(filled)
            elif "ack_type" in data:
                # 이 커넥터는 market data만 다룬다. ack_type payload는 완전히 무시.
                logger.debug("KISConnector: ignoring ack_type payload (market-data-only policy)")
                return
            else:
                logger.warning(f"KISConnector: Unknown data format - {data}")
        except Exception as e:
            logger.error(f"KISConnector: Error in _on_data: {e}")