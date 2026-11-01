import asyncio
import json
import logging
import os
from typing import Callable, Dict, List, Optional, TYPE_CHECKING
import websockets

from trading_system.types.enums import QuoteType, StreamerStatus

# 체결통보(H0IFCNI0)는 KIS가 AES-256-CBC로 암호화해서 보낸다. 이 암/복호화에 필요한
# `pycryptodome`은 optional dependency로 취급한다(시세만 쓰는 환경에서는 불필요).
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    _CRYPTO_AVAILABLE = True
except ImportError:  # pragma: no cover
    _CRYPTO_AVAILABLE = False


logger = logging.getLogger(__name__)

class RestStreamer:
    """KIS 실시간 데이터(WebSocket) 스트리머"""

    # API 문서에 따르면 선물옵션 실시간은 다른 URL 사용
    WS_URL = os.getenv("KIS_WS_URL", "ws://ops.koreainvestment.com:21000")  # 선물옵션용

    def __init__(self, token_manager: 'KISTokenManager'):
        self.token_manager = token_manager
        self._ws = None
        self._status = StreamerStatus.DISCONNECTED
        # sub_key -> (원본 symbol/HTS ID, quote_type). 재연결 시 원본 값으로 재구독하기 위해
        # quote_type뿐 아니라 symbol도 같이 저장한다(과거에는 quote_type만 저장해
        # 재연결 시 sub_key 문자열 자체를 symbol처럼 잘못 재구독하는 버그가 있었음).
        self._subscribed_symbols: Dict[str, tuple] = {}
        self._callbacks: Dict[str, Callable] = {}
        self._approval_key = None
        # 체결통보(H0IFCNI0)는 암호화되어 오므로, 구독 직후 오는 system 메시지에서
        # AES key/iv를 받아 저장해둔다. tr_id별로 다를 수 있어 dict로 관리.
        self._aes_key: Dict[str, str] = {}
        self._aes_iv: Dict[str, str] = {}

    @property
    def status(self) -> StreamerStatus:
        return self._status

    async def connect(self) -> bool:
        if self._status == StreamerStatus.CONNECTED:
            return True
        self._status = StreamerStatus.CONNECTING
        try:
            self._ws = await websockets.connect(self.WS_URL)
            await self._authenticate()
            self._status = StreamerStatus.CONNECTED
            asyncio.create_task(self._message_handler())
            logger.info("WebSocket connection established.")
            return True
        except Exception as e:
            self._status = StreamerStatus.ERROR
            logger.exception(f"WebSocket connection failed: {e}")
            return False

    async def _authenticate(self):
        # 선물옵션은 approval_key를 사용 (실시간 접속키)
        approval_key = await self.token_manager.get_approval_key()
        
        # 선물옵션 WebSocket은 별도 인증 과정이 없고, 
        # 각 메시지에 approval_key를 포함하여 전송
        self._approval_key = approval_key
        logger.info("Approval key acquired for WebSocket connection")

    async def subscribe(
        self, symbol: str, quote_type: QuoteType = QuoteType.TICK, callback: Optional[Callable] = None
    ) -> bool:
        """종목(또는 체결통보의 경우 HTS ID)을 구독합니다.

        `quote_type=QuoteType.EXECUTION`인 경우 `symbol` 인자는 종목코드가 아니라
        **KIS HTS ID**여야 합니다(H0IFCNI0는 종목이 아니라 계좌 단위로 구독하는
        체결통보 채널입니다). 예: `subscribe(hts_id, quote_type=QuoteType.EXECUTION, ...)`.

        Returns:
            실제로 구독 메시지를 전송했으면 True, 연결 실패 등으로 못 보냈으면 False.
            (실서버 테스트에서 확인된 문제: 이 반환값을 확인 안 하면 WebSocket 연결이
            실패했는데도 호출부가 "구독 성공"으로 잘못 로깅하는 일이 생긴다.)
        """
        # 종목+시세타입으로 키 생성 (같은 종목에 대해 체결/호가 동시 구독 가능)
        sub_key = f"{symbol}_{quote_type.value}"
        if sub_key in self._subscribed_symbols:
            logger.warning(f"{symbol} ({quote_type.name}) is already subscribed.")
            return False
        if not self._ws or self._status != StreamerStatus.CONNECTED:
            if not await self.connect():
                return False
        self._subscribed_symbols[sub_key] = (symbol, quote_type)
        if callback:
            self._callbacks[sub_key] = callback
        # API 문서에 따른 선물옵션 실시간 구독 형식
        if quote_type == QuoteType.TICK:
            tr_id = "H0IFCNT0"  # 지수선물 실시간체결가
        elif quote_type == QuoteType.ORDERBOOK:
            tr_id = "H0IFASP0"  # 지수선물 실시간호가
        elif quote_type == QuoteType.EXECUTION:
            tr_id = "H0IFCNI0"  # 선물옵션 실시간체결통보 (내 주문)
        else:
            logger.warning(f"Unknown quote type: {quote_type}")
            return False

        subscribe_data = {
            "header": {
                "approval_key": self._approval_key,
                "custtype": "P",  # 개인
                "tr_type": "1",   # 1: 등록
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": symbol  # TICK/ORDERBOOK: 종목코드, EXECUTION: HTS ID
                }
            }
        }
        await self._ws.send(json.dumps(subscribe_data))
        logger.info(f"Subscribed to {symbol} ({quote_type.name})")
        return True

    async def unsubscribe(self, symbol: str):
        # 모든 quote_type에 대해 구독 해제
        keys_to_remove = [key for key in self._subscribed_symbols.keys() if key.startswith(f"{symbol}_")]
        for key in keys_to_remove:
            del self._subscribed_symbols[key]
            if key in self._callbacks:
                del self._callbacks[key]
        if self._ws and self._status == StreamerStatus.CONNECTED:
            unsubscribe_data = {
                "type": "unsubscribe",
                "symbol": symbol
            }
            await self._ws.send(json.dumps(unsubscribe_data))
            logger.info(f"Unsubscribed from {symbol}")

    async def _message_handler(self):
        try:
            async for message in self._ws:
                # 선물옵션 실시간은 다양한 형태로 올 수 있음
                try:
                    # JSON 형태 메시지 (시스템 메시지) - 구독 성공 응답에 암호화 key/iv가 실려옴
                    if message.startswith('{'):
                        data = json.loads(message)
                        logger.info(f"RestStreamer: System message - {data}")
                        self._maybe_store_aes_key(data)
                        continue

                    # 실시간 데이터는 구분자로 연결된 문자열
                    # 형식: 0|H0IFCNT0|001|A01612^필드1^필드2^...
                    # parts[0]은 암호화 여부(0=평문, 1=암호화). 체결통보(H0IFCNI0)는 항상 암호화되어 온다.
                    parts = message.split('|')
                    if len(parts) >= 4:
                        # parts[1]이 tr_id, parts[3]에 실제 데이터가 있음
                        encrypt_flag = parts[0]
                        tr_id = parts[1] if len(parts) > 1 else ""

                        if tr_id == "H0IFCNI0":
                            self._handle_execution_notice(encrypt_flag, tr_id, parts[3] if len(parts) > 3 else "")
                            continue

                        # parts[3]에서 symbol과 데이터 분리
                        if len(parts) > 3:
                            raw_data = parts[3]
                            # 실제 데이터에서 symbol 추출 (첫 번째 ^ 앞까지)
                            data_parts = raw_data.split('^')
                            if len(data_parts) >= 2:
                                symbol = data_parts[0]  # 예: A01612
                                data_str = '^'.join(data_parts[1:])  # 시간과 데이터
                                
                                # tr_id로 quote_type 판단
                                if tr_id == "H0IFCNT0":
                                    sub_key = f"{symbol}_tick"
                                elif tr_id == "H0IFASP0":
                                    sub_key = f"{symbol}_orderbook"
                                else:
                                    logger.warning(f"RestStreamer: Unknown tr_id - {tr_id}")
                                    # tr_id가 없거나 알 수 없는 경우 모든 콜백 시도
                                    for sub_key in self._callbacks:
                                        if symbol in sub_key:
                                            self._callbacks[sub_key](data_str)
                                            break
                                    continue
                                
                                # 실시간 데이터 파싱 및 콜백 호출
                                if sub_key in self._callbacks:
                                    parsed_data = self._parse_realtime_data(tr_id, symbol, data_str)
                                    if parsed_data:
                                        # Parsing success logging removed for cleaner output
                                        self._callbacks[sub_key](parsed_data)
                                    else:
                                        logger.warning(f"RestStreamer: Failed to parse data for {symbol}")
                                else:
                                    logger.debug(f"No callback for {sub_key}")
                            else:
                                logger.debug(f"No data parts in message: {raw_data}")
                        else:
                            logger.debug(f"No raw data in message: {message}")
                    else:
                        logger.debug(f"Unknown message format: {message[:100]}...")
                        
                except json.JSONDecodeError:
                    # JSON이 아닌 경우 그대로 전달
                    logger.debug(f"Non-JSON message: {message[:100]}...")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except websockets.ConnectionClosed:
            logger.warning("WebSocket connection closed.")
            await self._handle_disconnect()
        except Exception as e:
            logger.exception(f"Unexpected error in message handler: {e}")
            await self._handle_disconnect()

    async def _handle_disconnect(self):
        self._status = StreamerStatus.DISCONNECTED
        logger.warning("WebSocket disconnected. Reconnecting in 5 seconds...")
        await asyncio.sleep(5)
        await self.connect()
        # sub_key가 아니라 원본 symbol/quote_type으로 재구독해야 한다(버그 수정).
        previous = dict(self._subscribed_symbols)
        self._subscribed_symbols.clear()
        for sub_key, (symbol, qt) in previous.items():
            await self.subscribe(symbol, qt, self._callbacks.get(sub_key))

    async def close(self):
        if self._ws:
            await self._ws.close()
            self._status = StreamerStatus.DISCONNECTED
            logger.info("WebSocket connection closed.")

    def _maybe_store_aes_key(self, system_message: Dict) -> None:
        """구독 성공 system 메시지에서 체결통보용 AES key/iv를 추출해 저장한다.

        KIS는 암호화가 필요한 tr_id(H0IFCNI0 등)를 처음 구독할 때, 그 응답의
        `body.output.key`/`body.output.iv`에 이후 데이터 프레임 복호화에 쓸 값을
        실어 보낸다. 정확한 응답 스키마는 실제 KIS 서버 응답으로 검증이 필요하다.
        """
        try:
            body = system_message.get("body") or {}
            output = body.get("output") or {}
            tr_id = (system_message.get("header") or {}).get("tr_id")
            key = output.get("key")
            iv = output.get("iv")
            if tr_id and key and iv:
                self._aes_key[tr_id] = key
                self._aes_iv[tr_id] = iv
                logger.info(f"RestStreamer: AES key/iv stored for tr_id={tr_id}")
        except Exception as e:
            logger.debug(f"RestStreamer: no AES key/iv in system message ({e})")

    def _decrypt_aes256_cbc(self, tr_id: str, cipher_b64: str) -> Optional[str]:
        """AES-256-CBC + PKCS7 복호화. `pycryptodome`이 없으면 None."""
        if not _CRYPTO_AVAILABLE:
            logger.error(
                "RestStreamer: 체결통보 복호화 실패 - pycryptodome이 설치되어 있지 않습니다. "
                "`pip install pycryptodome` 필요."
            )
            return None
        key = self._aes_key.get(tr_id)
        iv = self._aes_iv.get(tr_id)
        if not key or not iv:
            logger.error(f"RestStreamer: {tr_id}용 AES key/iv가 아직 없습니다(구독 직후 system 메시지 대기 필요).")
            return None
        try:
            import base64
            cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
            decrypted = unpad(cipher.decrypt(base64.b64decode(cipher_b64)), AES.block_size)
            return decrypted.decode("utf-8")
        except Exception as e:
            logger.error(f"RestStreamer: 체결통보 복호화 오류: {e}")
            return None

    def _handle_execution_notice(self, encrypt_flag: str, tr_id: str, raw_data: str) -> None:
        """H0IFCNI0(체결통보) 프레임을 처리한다.

        종목 기반 구독(H0IFCNT0/H0IFASP0)과 달리 계좌 단위 구독이라 sub_key가
        `symbol_execution`이 아니라 `<HTS ID>_execution` 형태로 등록되어 있다.
        """
        data_str = raw_data
        if encrypt_flag == "1":
            decrypted = self._decrypt_aes256_cbc(tr_id, raw_data)
            if decrypted is None:
                return
            data_str = decrypted

        parsed = self._parse_realtime_data(tr_id, symbol="", data_str=data_str)
        if not parsed:
            logger.warning("RestStreamer: 체결통보 파싱 실패")
            return

        # execution 구독은 계좌(HTS ID) 단위이므로, 등록된 execution 콜백을 모두 호출한다.
        dispatched = False
        for sub_key, callback in self._callbacks.items():
            if sub_key.endswith("_execution"):
                callback(parsed)
                dispatched = True
        if not dispatched:
            logger.debug("RestStreamer: 체결통보 콜백이 등록되어 있지 않습니다.")

    def _parse_realtime_data(self, tr_id: str, symbol: str, data_str: str) -> Optional[Dict]:
        """실시간 데이터 파싱"""
        try:
            # 데이터는 ^ 구분자로 분리됨
            # data_str은 이미 symbol을 제거한 상태 (시간^데이터^...)
            fields = data_str.split('^')
            # Parsing info logging removed for cleaner output
            
            if tr_id == "H0IFCNT0":  # 체결 데이터
                # fields[0]: 시간, fields[1]: FUTS_PRDY_VRSS, fields[2]: PRDY_VRSS_SIGN ...
                if len(fields) >= 10:  # 최소 필드 개수 확인 (완화)
                    return {
                        "symbol": symbol,
                        "price": float(fields[4]) if fields[4] else 0,  # FUTS_PRPR (선물 현재가) - fields[4]
                        "volume": int(fields[8]) if fields[8] else 0,   # LAST_CNQN (최종 거래량) - fields[8]
                        "open": float(fields[5]) if fields[5] else 0,   # FUTS_OPRC (선물 시가) - fields[5]
                        "high": float(fields[6]) if fields[6] else 0,   # FUTS_HGPR (선물 최고가) - fields[6]
                        "low": float(fields[7]) if fields[7] else 0,    # FUTS_LWPR (선물 최저가) - fields[7]
                        "price_chg": float(fields[1]) if fields[1] else 0  # FUTS_PRDY_VRSS (전일대비) - fields[1]
                    }
            elif tr_id == "H0IFASP0":  # 호가 데이터
                # fields[0]: 시간, fields[1]: FUTS_ASKP1, fields[5]: FUTS_BIDP1
                if len(fields) >= 16:  # 최소 필드 개수 확인
                    return {
                        "symbol": symbol,
                        "best_ask_price": float(fields[1]) if fields[1] else 0,  # FUTS_ASKP1
                        "best_bid_price": float(fields[6]) if fields[6] else 0,  # FUTS_BIDP1 (6번째)
                        "best_ask_volume": int(fields[11]) if fields[11] else 0, # ASKP_RSQN1 (11번째)
                        "best_bid_volume": int(fields[16]) if fields[16] else 0  # BIDP_RSQN1 (16번째)
                    }
            elif tr_id == "H0IFCNI0":  # 체결통보(내 주문)
                # !!! 검증 필요(VERIFY BEFORE LIVE) !!!
                # 아래 필드 순서는 KIS 선물옵션 실시간체결통보의 공개된 필드 목록을
                # 기반으로 한 추정 매핑이며, 실제 KIS 서버 응답으로 검증되지 않았습니다.
                # 실거래 투입 전 반드시 실제 모의투자 체결통보 원문으로 인덱스를 재확인하세요.
                if len(fields) >= 14:
                    return {
                        "ack_type": "ack_type",  # placeholder, 실제 값은 아래 rfus/cntg/acpt로 결정
                        "cust_id": fields[0],
                        "acnt_no": fields[1],
                        "order_num": fields[2],           # oder_no
                        "original_order_num": fields[3],  # ooder_no
                        "buysell": fields[4],              # seln_byov_cls: 01=매도, 02=매수
                        "symbol": fields[8] if len(fields) > 8 else "",  # 단축코드
                        "filled_volume": int(fields[9]) if len(fields) > 9 and fields[9] else 0,   # cntg_qty
                        "filled_price": float(fields[10]) if len(fields) > 10 and fields[10] else 0.0,  # cntg_unpr
                        "rfus_yn": fields[12] if len(fields) > 12 else "",  # 거부여부
                        "cntg_yn": fields[13] if len(fields) > 13 else "",  # 체결여부
                        "acpt_yn": fields[14] if len(fields) > 14 else "",  # 접수여부
                        "order_qty": int(fields[16]) if len(fields) > 16 and fields[16] else 0,
                    }
                logger.warning(f"H0IFCNI0: 필드 개수 부족({len(fields)}) - 원문: {fields}")
                return None

            logger.warning(f"Insufficient fields in data: {len(fields)} for {tr_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing realtime data: {e}")
            logger.error(f"Fields: {fields[:10] if len(fields) > 10 else fields}")  # 처음 10개 필드만 로깅
            return None
    
    def get_subscribed_symbols(self) -> List[str]:
        return list(self._subscribed_symbols.keys())