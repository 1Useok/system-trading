import logging
import asyncio
import aiohttp
import os
import json
from typing import List, Dict, Tuple, Optional, Any, Callable
from datetime import datetime, timedelta
import pandas as pd

from trading_system.api.base import BaseApi
from trading_system.types.enums import BuySell, OrderType, PriceType
from trading_system.types.events import FuturesSimpleOrderAck, FuturesSimpleQuote
from trading_system.api.kis.token_manager import KISTokenManager
from trading_system.utils.clock import now_kst


logger = logging.getLogger(__name__)

class RestApi(BaseApi):
    """한국투자증권 REST API 단일 구현체"""

    ENDPOINTS = {
        "account_info": os.getenv("KIS_ACCOUNT_URL", "https://openapi.koreainvestment.com:9443/uapi/domestic-account/v1/accounts"),
        "request_data": os.getenv("KIS_DATA_URL", "https://openapi.koreainvestment.com:9443/uapi/domestic-futureoption/v1/quotations/inquire-daily"),
        "websocket": os.getenv("KIS_WS_URL", "wss://openapi.koreainvestment.com:9443/ws/quotations")
    }

    def __init__(self, config: dict, context: str = 'data'):
        """
        RestApi 초기화
        
        Args:
            config: 설정 딕셔너리
            context: 사용 컨텍스트 ('order', 'balance', 'data', 'quote', 'strategy')
        """
        self.context = context
        self.config = config
        
        # 컨텍스트에 따라 적절한 TokenManager 선택
        if context in ['order', 'balance']:
            # 주문/잔고 -> VPS (모의투자) 사용
            svr = 'vps'
            api_config = config.get('vps_api', {})
        else:
            # 시세/데이터/전략 -> PROD (실전투자) 사용
            svr = 'prod'
            api_config = config.get('prod_api', {})
        
        # API 설정 추출
        self.app_key = api_config.get('app_key')
        self.app_secret = api_config.get('app_secret')
        self.base_url = api_config.get('base_url')
        
        self._session = None
        
        # KIS API 엔드포인트 매핑
        self.endpoints = config.get('kis_api_endpoints', {})
        
        # TokenManager 초기화 (컨텍스트에 맞는 SVR)
        self.token_manager = KISTokenManager(svr=svr)
        self.token_manager.configure(api_config)
        
        logger.info(f"RestApi instance created with TokenManager (context={context}, svr={svr})")


    async def is_connected(self) -> bool:
        if not self._session:
            self._session = aiohttp.ClientSession()
        try:
            # TokenManager를 통해 토큰 확인
            token = await self.token_manager.get_token()
            if token:
                logger.info("REST API connection established successfully")
                return True
            return False
        except Exception as e:
            logger.exception(f"Failed to establish REST API connection: {e}")
            return False

    async def get_access_token(self) -> str:
        """TokenManager를 통해 토큰 반환"""
        return await self.token_manager.get_token()

    async def request_data(self, tr_id: str, params: Dict[str, Any], method: str = "POST") -> Dict[str, Any]:
        if not self._session:
            await self.is_connected()
        
        # TokenManager를 통해 토큰 획득
        access_token = await self.token_manager.get_token()
        
        headers = {
            "authorization": f"Bearer {access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id
        }
        
        # TR_ID에 해당하는 엔드포인트 찾기
        endpoint = self.endpoints.get(tr_id)
        if not endpoint:
            raise ValueError(f"Unknown TR_ID: {tr_id}. Please add it to kis_api_endpoints in config.yaml")
        
        url = f"{self.base_url}{endpoint}"
        
        logger.debug(f"Sending request: TR_ID={tr_id}, Method={method}, URL={url}, Params={params}")
        try:
            if method.upper() == "GET":
                async with self._session.get(
                        url,
                        headers=headers,
                        params=params  # GET에서는 params로 전달
                ) as response:
                    result = await response.json()
            else:  # POST
                async with self._session.post(
                        url,
                        headers=headers,
                        json=params  # POST에서는 json으로 전달
                ) as response:
                    result = await response.json()
            
            # API 에러 체크
            rt_cd = result.get('rt_cd', '')
            if rt_cd and rt_cd != '0':
                logger.error(f"API Error - TR_ID: {tr_id}, Code: {rt_cd}, Msg: {result.get('msg1')}")
            else:
                # output2가 있는지 확인
                if 'output2' in result:
                    output2_len = len(result.get('output2', []))
                    logger.debug(f"Received response for TR_ID={tr_id}: Success, output2 records: {output2_len}")
                else:
                    logger.debug(f"Received response for TR_ID={tr_id}: Success")
            
            return result
        except aiohttp.ClientError as e:
            logger.error(f"Client error during request_data: {e}")
            raise
        except asyncio.TimeoutError:
            logger.error("Request timed out")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during request_data: {e}")
            raise

    async def get_account_info(self) -> List[Dict[str, str]]:
        # TokenManager를 통해 토큰 확인
        access_token = await self.token_manager.get_token()
        account = self.config.get("stock_account") or os.getenv("KIS_ACCOUNT")
        product_code = self.config.get("product_code") or os.getenv("KIS_PRODUCT", "01")
        params = {
            "CANO": account[:8],
            "ACNT_PRDT_CD": account[-2:]
        }
        headers = {
            "authorization": f"Bearer {access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "VTTC8434R",  # 계좌정보 조회용 TR_ID (공식 문서 참고)
            "content-type": "application/json"
        }
        logger.info("Fetching account information")
        try:
            async with self._session.get(
                self.ENDPOINTS["account_info"],
                headers=headers,
                params=params
            ) as response:
                logger.debug(f"HTTP status: {response.status}")
                logger.debug(f"Response text: {await response.text()}")
                result = await response.json()
                logger.debug(f"Account info response: {result}")
                if result is None:
                    logger.error("API 응답이 None입니다.")
                    return []
                if "output" not in result or not result["output"]:
                    logger.error(f"API 응답에 계좌 정보가 없습니다: {result}")
                    return []
                return [
                    {
                        "account_number": acc.get("account_no", ""),
                        "product_code": acc.get("product_code", "")
                    }
                for acc in result["output"]
            ]
        except Exception as e:
            logger.error(f"Error fetching account info: {e}")
            return []

    async def get_all_accounts(self) -> List[str]:
        accounts = await self.get_account_info()
        return [acc['account_number'] for acc in accounts]

    async def get_stock_balance(self, account: str) -> pd.DataFrame:
        params = {
            "CANO": account[:8],          # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:], # 계좌번호 뒤 2자리
        # 필요에 따라 추가 파라미터 (예: 조회구분 등)
        }
        try:
            result = await self.request_data("VTTC8434R", params)
            return pd.DataFrame(result.get('output1', []))
        except Exception as e:
            logger.error(f"Error fetching stock balance: {e}")
            return pd.DataFrame()

    async def get_stock_orders(self, account: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        today = now_kst()
        start = datetime.strptime(start_date or today.strftime("%Y%m%d"), "%Y%m%d")
        end = datetime.strptime(end_date or today.strftime("%Y%m%d"), "%Y%m%d")
        three_months_ago = today - timedelta(days=90)
        # 3개월 이내면 VTTC0081R, 아니면 VTSC9215R
        tr_id = "VTTC0081R" if start >= three_months_ago else "VTSC9215R"

        params = {
            "CANO": account[:8],
            "ACNT_PRDT_CD": account[-2:],
            "INQR_STRT_DT": start.strftime("%Y%m%d"),
            "INQR_END_DT": end.strftime("%Y%m%d"),
            "SLL_BUY_DVSN_CD": "00",  # 전체
            "INQR_DVSN": "00",        # 전체
            "PDNO": "",
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
        try:
            result = await self.request_data(tr_id, params)
            return pd.DataFrame(result.get('output1', []))
        except Exception as e:
            logger.error(f"Error fetching stock orders: {e}")
            return pd.DataFrame()

    async def get_stock_price(self, symbol: str) -> Dict[str, float]:
        params = {"FID_COND_MRKT_DIV_CODE": symbol}
        try:
            result = await self.request_data("FHKST01010100", params)
            output = result.get('output', {})
            return {
                'current_price': float(output.get('stck_prpr', 0)),
                'high': float(output.get('stck_hgpr', 0)),
                'low': float(output.get('stck_lwpr', 0)),
                'open': float(output.get('stck_oprc', 0)),
                'prev_close': float(output.get('stck_sdpr', 0))
            }
        except Exception as e:
            logger.error(f"Error fetching stock price: {e}")
            return {}

    async def order_stock(self, account: str, symbol: str, quantity: int, price: float, side: BuySell, price_type: PriceType = PriceType.LIMIT, order_type: OrderType = OrderType.NORMAL) -> str:
        params = {
            "CANO": account[:8],              # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:],     # 계좌번호 뒤 2자리
            "PDNO": symbol,                   # 종목코드
            "ORD_QTY": quantity,              # 주문수량
            "ORD_PRC": price,                 # 주문가격
            "ORD_DVSN": side.value,           # 매수/매도 구분
            "ORD_TP": order_type.value,       # 주문유형
            "ORD_PRC_DVSN": price_type.value  # 가격유형
        }
        logger.info(f"Placing stock order: {symbol} x{quantity} @ {price} ({side.name})")
        logger.debug(f"Stock order params: {params}")
        try:
            tr_id = "VTTC0012U" if side == BuySell.BUY else "VTTC0011U"
            result = await self.request_data(tr_id, params)
            order_no = result.get('output', {}).get('order_no', '')
            logger.info(f"Order placed successfully: {order_no}")
            return order_no
        except Exception as e:
            logger.error(f"Failed to place stock order: {e}")
            return ""

    async def cancel_stock_order(self, account: str, order_no: str, quantity: int) -> bool:
        params = {
          "CANO": account[:8],          # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:], # 계좌번호 뒤 2자리
            "ORD_ODNO": order_no,           # 주문번호
            "ORD_QTY": quantity           # 취소수량
        }
        logger.info(f"Canceling stock order: {order_no} ({quantity} shares)")
        try:
            await self.request_data("VTTC0013U", params)
            logger.info("Stock order cancelled successfully")
            return True
        except Exception as e:
            logger.error(f"Error during stock order cancellation: {e}")
            return False

    async def get_future_account_balance(self, account: str, mgna_dvsn: str = "01", excc_stat_cd: str = "1") -> Tuple[pd.DataFrame, Dict]:
        """
        선물옵션 잔고현황 조회
        
        Args:
            account: 계좌번호 (예: "12345678-03")
            mgna_dvsn: 증거금 구분 ("01": 개시, "02": 유지)
            excc_stat_cd: 정산상태코드 ("1": 정산, "2": 본정산)
        """
        params = {
            "CANO": account[:8],                    # 계좌번호 앞 8자리 (종합계좌번호)
            "ACNT_PRDT_CD": account[-2:],           # 계좌번호 뒤 2자리 (계좌상품코드)
            "MGNA_DVSN": mgna_dvsn,                # 증거금 구분 (01: 개시, 02: 유지)
            "EXCC_STAT_CD": excc_stat_cd,          # 정산상태코드 (1: 정산, 2: 본정산)
            "CTX_AREA_FK200": "",                  # 연속조회검색조건200 (최초조회시 공란)
            "CTX_AREA_NK200": ""                   # 연속조회키200 (최초조회시 공란)
        }
        logger.info(f"Future account balance request params: {params}")
        try:
            # 컨텍스트에 따라 TR_ID 선택 (order 컨텍스트는 VPS이므로 V 접두사)
            tr_id = "VTFO6118R" if self.context in ['order', 'balance'] else "CTFO6118R"
            result = await self.request_data(tr_id, params, method="GET")
            # NOTE(실서버에서 실제로 확인된 버그): request_data()는 KIS가 에러를 응답해도
            # (예: 속도제한 EGW00201) rt_cd만 로그로 남기고 그 결과를 그대로 반환한다.
            # 여기서 rt_cd를 확인 안 하면 output1/output2가 텅 빈 에러 응답을 "포지션 0개,
            # 잔고 0원"으로 잘못 해석하게 되고, KrxExitAll처럼 이 값으로 청산 여부를
            # 판단하는 코드가 실제 보유 포지션을 놓칠 수 있다.
            rt_cd = result.get('rt_cd', '')
            if rt_cd and rt_cd != '0':
                raise RuntimeError(f"KIS API error (TR_ID={tr_id}): {result.get('msg1')} (rt_cd={rt_cd})")
            balance_detail = pd.DataFrame(result.get('output1', []))
            output2 = result.get('output2', {})
            account_summary = {
                'total_deposit': float(output2.get('tot_dncl_amt', 0)),      # 총예수금액
                'available_deposit': float(output2.get('ord_psbl_cash', 0)),  # 주문가능현금
                'margin_rate': float(output2.get('mgna_tota', 0)),           # 증거금총액
                'total_profit_loss': float(output2.get('evlu_pfls_amt_smtl', 0))  # 평가손익금액합계
            }
            return balance_detail, account_summary
        except Exception as e:
            logger.error(f"Error fetching future account balance: {e}")
            return pd.DataFrame(), {}

    async def get_future_orders(self, account: str, start_date: str = None, end_date: str = None) -> Tuple[pd.DataFrame, Dict]:
        # 기본값 설정: 당일 조회
        from datetime import datetime
        if not start_date:
            start_date = now_kst().strftime("%Y%m%d")
        if not end_date:
            end_date = now_kst().strftime("%Y%m%d")
            
        # API 문서에 따른 필수 Query Parameter 구성
        params = {
            "CANO": account[:8],          # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:], # 계좌번호 뒤 2자리
            "STRT_ORD_DT": start_date,    # 시작주문일자(YYYYMMDD)
            "END_ORD_DT": end_date,       # 종료주문일자(YYYYMMDD)
            "SLL_BUY_DVSN_CD": "00",      # 매도매수구분코드 (00: 전체)
            "CCLD_NCCS_DVSN": "02",       # 체결미체결구분 (02: 미체결)
            "SORT_SQN": "DS",             # 정렬순서 (DS: 역순)
            "STRT_ODNO": "",              # 시작주문번호 (공란)
            "PDNO": "",                   # 상품번호 (공란: 전체 조회)
            "MKET_ID_CD": "",             # 시장ID코드 (공란)
            "CTX_AREA_FK200": "",         # 연속조회검색조건200 (최초 조회시 공란)
            "CTX_AREA_NK200": ""          # 연속조회키200 (최초 조회시 공란)
        }
        try:
            # GET 방식으로 호출 (API 문서에 따라)
            result = await self.request_data("VTTO5201R", params, method="GET")
            orders = pd.DataFrame(result.get('output1', []))
            summary = {
                'total_orders': len(orders),
                'period': f"{start_date}~{end_date}"
            }
            return orders, summary
        except Exception as e:
            logger.error(f"Error fetching future orders: {e}")
            return pd.DataFrame(), {}

    async def check_future_order_availability(self, account: str, symbol: str, side: BuySell, price: float = 0, order_type: str = "01") -> Dict[str, Any]:
        """
        선물옵션 주문가능 여부 확인
        
        Args:
            account: 계좌번호
            symbol: 종목코드 (예: "A01612")
            side: 매도매수구분 (BUY/SELL)
            price: 주문가격 (0이면 현재가 기준)
            order_type: 주문구분코드 ("01": 지정가, "02": 시장가)
        """
        params = {
            "CANO": account[:8],              # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:],     # 계좌번호 뒤 2자리
            "PDNO": symbol,                   # 종목코드
            "SLL_BUY_DVSN_CD": "01" if side == BuySell.SELL else "02",  # 매도매수구분
            "UNIT_PRICE": str(int(price)),    # 주문가격
            "ORD_DVSN_CD": order_type         # 주문구분코드
        }
        logger.info(f"Checking future order availability for {symbol} - {side.name}")
        logger.info(f"Order availability check params: {params}")
        try:
            result = await self.request_data("VTTO5105R", params, method="GET")
            if result.get('rt_cd') == '0':
                output = result.get('output', {})
                availability = {
                    'is_orderable': True,
                    'total_possible_qty': int(output.get('tot_psbl_qty', 0)),
                    'liquidation_possible_qty': int(output.get('lqd_psbl_qty1', 0)),
                    'order_possible_qty': int(output.get('ord_psbl_qty', 0)),
                    'base_index': float(output.get('bass_idx', 0))
                }
                logger.info(f"Order availability: {availability}")
                return availability
            else:
                error_msg = result.get('msg1', '알 수 없는 오류')
                logger.error(f"Order availability check failed: {error_msg}")
                return {
                    'is_orderable': False,
                    'error': error_msg,
                    'total_possible_qty': 0,
                    'order_possible_qty': 0
                }
        except Exception as e:
            logger.error(f"Error checking order availability: {e}")
            return {
                'is_orderable': False,
                'error': str(e),
                'total_possible_qty': 0,
                'order_possible_qty': 0
            }

    async def get_future_price(self, symbol: str) -> Dict[str, float]:
        # 선물옵션 시세 API 파라미터 (API 문서 기준)
        params = {
            "FID_COND_MRKT_DIV_CODE": "F",  # F: 지수선물, O: 지수옵션 등
            "FID_INPUT_ISCD": symbol        # 종목코드
        }
        try:
            result = await self.request_data("FHMIF10000000", params, method="GET")
            # API 문서에 따르면 output1에 선물 시세 정보가 있음
            output = result.get('output1', {})
            return {
                'current_price': float(output.get('futs_prpr', 0)),      # 선물 현재가
                'high': float(output.get('futs_hgpr', 0)),               # 선물 최고가
                'low': float(output.get('futs_lwpr', 0)),                # 선물 최저가
                'open': float(output.get('futs_oprc', 0)),               # 선물 시가
                'basis': float(output.get('basis', 0)),                  # 베이시스
                'prev_close': float(output.get('futs_prdy_clpr', 0))     # 선물 전일종가
            }
        except Exception as e:
            logger.error(f"Error fetching future price: {e}")
            return {}

    async def get_future_daily_ohlc(self, symbol: str, market_code: str = "F") -> Dict[str, float]:
        """선물 전일(가장 최근 완결된 거래일) OHLC를 조회합니다.

        `get_future_price()`가 주는 `prev_close`(전일종가) 외에, 변동성 돌파 전략류가
        필요로 하는 전일 고가/저가는 별도의 일봉 조회(`FHKIF03020100`)를 통해서만
        얻을 수 있습니다. 일봉 조회 TR_ID/파라미터 규약을 사용합니다.
        """
        today = now_kst()
        start = (today - timedelta(days=10)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        params = {
            "FID_COND_MRKT_DIV_CODE": market_code,
            "FID_INPUT_ISCD": symbol,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": end,
        }
        try:
            result = await self.request_data("FHKIF03020100", params, method="GET")
            rows = result.get("output2") or []
            if not rows:
                return {}
            # output2는 최신순으로 오는 경우가 일반적이나, 날짜 문자열로 직접 정렬해 확정한다.
            rows = sorted(rows, key=lambda r: r.get("stck_bsop_date", ""), reverse=True)
            today_str = today.strftime("%Y%m%d")
            # 오늘 날짜 행이 섞여 있으면 제외하고, 그 다음(=전일)을 사용한다.
            prev_rows = [r for r in rows if r.get("stck_bsop_date") != today_str]
            target = prev_rows[0] if prev_rows else rows[0]
            return {
                "date": target.get("stck_bsop_date", ""),
                "open": float(target.get("futs_oprc", 0) or 0),
                "high": float(target.get("futs_hgpr", 0) or 0),
                "low": float(target.get("futs_lwpr", 0) or 0),
                "close": float(target.get("futs_prpr", 0) or 0),
            }
        except Exception as e:
            logger.error(f"Error fetching future daily OHLC: {e}")
            return {}

    async def order_future(self, account: str = None, symbol: str = None, quantity: int = None, price: float = None, side: BuySell = None, price_type: PriceType = PriceType.LIMIT, order_type: OrderType = OrderType.NORMAL, account_no: str = None, **kwargs) -> str:
        # account_no와 account 둘 다 지원
        if account is None and account_no is not None:
            account = account_no
            
        params = {
            "ORD_PRCS_DVSN_CD": "02",         # 주문처리구분코드 (02: 주문전송)
            "CANO": account[:8],              # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:],     # 계좌번호 뒤 2자리
            "SLL_BUY_DVSN_CD": "02" if side == BuySell.BUY else "01",  # 매도매수구분코드
            "SHTN_PDNO": symbol,              # 단축상품번호 (종목번호)
            "ORD_QTY": str(quantity),         # 주문수량
            # 지수선물 호가단위는 0.05pt라 소수점을 잘라내면 다른 가격으로 주문이 나간다.
            "UNIT_PRICE": f"{float(price):.2f}" if price_type == PriceType.LIMIT else "0",  # 주문가격
            "NMPR_TYPE_CD": "",               # 호가유형코드 (공란)
            "KRX_NMPR_CNDT_CD": "",           # 한국거래소호가조건코드 (공란)
            "CTAC_TLNO": "",                  # 연락전화번호 (공란)
            "FUOP_ITEM_DVSN_CD": "",          # 선물옵션종목구분코드 (공란)
            "ORD_DVSN_CD": self._get_order_division_code(price_type, order_type)  # 주문구분코드
        }
        logger.info(f"Placing future order: {symbol} x{quantity} @ {price} ({side.name}, {price_type.name})")
        logger.debug(f"Order params: {params}")
        try:
            result = await self.request_data("VTTO1101U", params)
            
            # API 응답 확인
            if result.get('rt_cd') == '0':
                order_no = result.get('output', {}).get('ODNO', '')
                logger.info(f"Order successful. Order No: {order_no}")
                return result  # 전체 결과 반환
            else:
                error_msg = result.get('msg1', '알 수 없는 오류')
                logger.error(f"Order failed: {error_msg}")
                return result  # 에러 정보도 반환
        except Exception as e:
            logger.error(f"Error placing future order: {e}")
            # 장마감 후 오류 처리
            if "선물옵션 운영시간" in str(e) or "운영시간" in str(e):
                logger.warning("현재 선물옵션 운영시간이 아닙니다. 모의투자 환경에서도 운영시간을 확인해주세요.")
            return {"rt_cd": "1", "msg1": str(e)}

    def _get_order_division_code(self, price_type: PriceType, order_type: OrderType) -> str:
        """주문구분코드 생성"""
        if price_type == PriceType.LIMIT:
            if order_type == OrderType.NORMAL:
                return "01"  # 지정가
            elif order_type == OrderType.IOC:
                return "10"  # 지정가(IOC)
            elif order_type == OrderType.FOK:
                return "11"  # 지정가(FOK)
        elif price_type == PriceType.MARKET:
            if order_type == OrderType.NORMAL:
                return "02"  # 시장가
            elif order_type == OrderType.IOC:
                return "12"  # 시장가(IOC)
            elif order_type == OrderType.FOK:
                return "13"  # 시장가(FOK)
        elif price_type == PriceType.BEST:
            if order_type == OrderType.NORMAL:
                return "04"  # 최유리
            elif order_type == OrderType.IOC:
                return "14"  # 최유리(IOC)
            elif order_type == OrderType.FOK:
                return "15"  # 최유리(FOK)
        return "01"  # 기본값: 지정가

    async def inquire_order_history(self, account: str, start_date: str = None, end_date: str = None,
                                   order_status: str = "02", raise_on_error: bool = False) -> List[Dict]:
        """
        선물옵션 주문체결내역조회
        
        Args:
            account: 계좌번호 (8-2 형태)
            start_date: 시작일자 (YYYYMMDD, 기본값: 오늘)
            end_date: 종료일자 (YYYYMMDD, 기본값: 오늘)
            order_status: "00"=전체, "01"=체결, "02"=미체결
            raise_on_error: True면 조회 실패 시 빈 목록 대신 예외를 던진다
                (빈 목록을 "주문 없음"으로 오인하면 안 되는 일괄 취소/체결 보정에서 사용).
        
        Returns:
            미체결 주문 리스트
        """
        from datetime import datetime
        
        if start_date is None:
            start_date = now_kst().strftime("%Y%m%d")
        if end_date is None:
            end_date = now_kst().strftime("%Y%m%d")
            
        # API 문서에 따른 파라미터 설정
        params = {
            "CANO": account[:8],                    # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:],          # 계좌번호 뒤 2자리
            "STRT_ORD_DT": start_date,             # 시작주문일자
            "END_ORD_DT": end_date,                # 종료주문일자
            "SLL_BUY_DVSN_CD": "00",              # 00:전체, 01:매도, 02:매수
            "CCLD_NCCS_DVSN": order_status,       # 00:전체, 01:체결, 02:미체결
            "SORT_SQN": "DS",                     # AS:정순, DS:역순
            "STRT_ODNO": "",                      # 시작주문번호 (공란)
            "PDNO": "",                           # 상품번호 (공란시 전체)
            "MKET_ID_CD": "",                     # 시장ID코드 (공란)
            "CTX_AREA_FK200": "",                 # 연속조회키 (최초조회시 공란)
            "CTX_AREA_NK200": ""                  # 연속조회키 (최초조회시 공란)
        }
        
        try:
            # 모의투자는 VTTO5201R, 실전투자는 TTTO5201R
            tr_id = "VTTO5201R" if self.context in ['order', 'balance'] else "TTTO5201R"
            
            result = await self.request_data(tr_id, params, method="GET")
            
            if result.get('rt_cd') == '0':
                orders = result.get('output1', [])
                logger.info(f"주문 내역 조회 완료: {len(orders)}건")
                return orders
            else:
                error_msg = result.get('msg1', '알 수 없는 오류')
                logger.error(f"주문 내역 조회 실패: {error_msg}")
                if raise_on_error:
                    raise RuntimeError(f"KIS API error (VTTO5201R): {error_msg}")
                return []

        except Exception as e:
            logger.error(f"주문 내역 조회 중 오류: {e}")
            if raise_on_error:
                raise
            return []

    async def cancel_future_order(self, account: str, original_order_no: str, symbol: str = "", 
                                 quantity: int = 0) -> Dict:
        """
        선물옵션 주문 취소
        
        Args:
            account: 계좌번호 (8-2 형태)
            original_order_no: 원주문번호 (취소할 주문번호)
            symbol: 종목코드 (선택사항)
            quantity: 취소수량 (0=전량취소)
            
        Returns:
            취소 결과
        """
        
        # API 문서에 따른 Body 파라미터 설정
        params = {
            "ORD_PRCS_DVSN_CD": "02",            # 주문처리구분코드 (02: 주문전송)
            "CANO": account[:8],                  # 계좌번호 앞 8자리
            "ACNT_PRDT_CD": account[-2:],        # 계좌번호 뒤 2자리
            "RVSE_CNCL_DVSN_CD": "02",           # 정정취소구분코드 (02: 취소)
            "ORGN_ODNO": original_order_no,       # 원주문번호
            "ORD_QTY": str(quantity),            # 주문수량 (0: 전량)
            "UNIT_PRICE": "0",                   # 주문가격 (취소시 0)
            "NMPR_TYPE_CD": "01",                # 호가유형코드 (01: 지정가)
            "KRX_NMPR_CNDT_CD": "0",            # 한국거래소호가조건코드 (취소시 0)
            "RMN_QTY_YN": "Y",                  # 잔여수량여부 (Y: 전량)
            "FUOP_ITEM_DVSN_CD": "",            # 선물옵션종목구분코드 (공란)
            "ORD_DVSN_CD": "01"                 # 주문구분코드 (취소시 01)
        }
        
        try:
            # 모의투자는 VTTO1103U, 실전투자는 TTTO1103U
            tr_id = "VTTO1103U" if self.context in ['order', 'balance'] else "TTTO1103U"
            
            result = await self.request_data(tr_id, params, method="POST")
            
            if result.get('rt_cd') == '0':
                output = result.get('output', {})
                logger.info(f"주문 취소 성공: {original_order_no}")
                return {
                    'success': True,
                    'order_no': output.get('ODNO', ''),
                    'original_order_no': output.get('ORGN_ODNO', ''),
                    'message': result.get('msg1', '')
                }
            else:
                error_msg = result.get('msg1', '알 수 없는 오류')
                logger.error(f"주문 취소 실패: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'order_no': original_order_no
                }
                
        except Exception as e:
            logger.error(f"주문 취소 중 오류: {e}")
            return {
                'success': False,
                'error': str(e),
                'order_no': original_order_no
            }

    async def close(self):
        """리소스 정리"""
        if self._session:
            await self._session.close()
            self._session = None
        if self.token_manager:
            await self.token_manager.close()