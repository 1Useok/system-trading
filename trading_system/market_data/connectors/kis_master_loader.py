# -*- coding: utf-8 -*-
"""KIS REST API 기반 선물 마스터 데이터 로더.

KIS는 전일 OHLC를 한 번에 주는 배치 데이터가 없으므로
- 현재가/전일종가: `RestApi.get_future_price()` (`FHMIF10000000`)
- 전일 고가/저가: `RestApi.get_future_daily_ohlc()` (`FHKIF03020100`, 일봉조회)
두 REST 호출을 조합해 전일 기준가/고가/저가를 구성합니다.

주의(설계 결정): 근월물 단축코드 자동판별(`FHPIF05030200` 선물전광판)은
이 저장소에 실제 응답 필드 스키마를 확인할 수 있는 예시/문서가 없어 추측으로
구현하지 않았습니다. 근월물 단축코드는 만기가 바뀔 때만 갱신되는 값이므로,
config에서 명시적으로 주입받는 방식을 택했습니다.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


class KISMasterLoader:
    """선물 종목별 전일 OHLC/기준가를 REST로 조회해 캐싱하는 로더."""

    def __init__(self, rest_api, market_code: str = "F", request_spacing_sec: float = 1.1):
        """
        Args:
            rest_api: `trading_system.api.kis.rest_api.RestApi` 인스턴스
            market_code: FID_COND_MRKT_DIV_CODE (F: 지수선물)
            request_spacing_sec: REST 호출 사이 간격. 연속 호출하면 KIS 초당 요청 제한에 걸린다.
        """
        self.rest_api = rest_api
        self.market_code = market_code
        self.request_spacing_sec = request_spacing_sec
        self._master: Dict[str, Dict[str, Any]] = {}

    async def load(self, symbols: Iterable[str], symbol_market_map: Optional[Dict[str, str]] = None) -> bool:
        """주어진 종목(단축코드) 목록에 대해 전일 OHLC/기준가를 조회해 캐싱합니다.

        Args:
            symbols: 조회할 단축코드 목록
            symbol_market_map: {symbol: market_label} (예: {"A01612": "KP200"}).
                `get_nearest_contract(market)`으로 조회하려면 필요합니다. 생략 가능.

        Returns:
            하나라도 성공적으로 로드됐으면 True.
        """
        symbols = list(symbols)
        symbol_market_map = symbol_market_map or {}
        if not symbols:
            logger.warning("KISMasterLoader.load: 종목 목록이 비어있습니다.")
            return False

        loaded = 0
        for symbol in symbols:
            try:
                price_info = await self.rest_api.get_future_price(symbol)
                await asyncio.sleep(self.request_spacing_sec)
                daily_ohlc = await self.rest_api.get_future_daily_ohlc(symbol, market_code=self.market_code)
                await asyncio.sleep(self.request_spacing_sec)

                if not price_info and not daily_ohlc:
                    logger.warning(f"KISMasterLoader: {symbol} 마스터 데이터 조회 실패")
                    continue

                self._master[symbol] = {
                    "symbol": symbol,
                    "market": symbol_market_map.get(symbol),
                    # 기준가(전일종가)
                    "base_price": price_info.get("prev_close") or daily_ohlc.get("close", 0.0),
                    "prev_close": price_info.get("prev_close", 0.0),
                    # Vol Breaking 계열이 필요로 하는 전일 고가/저가
                    "prev_high": daily_ohlc.get("high", 0.0),
                    "prev_low": daily_ohlc.get("low", 0.0),
                    "prev_date": daily_ohlc.get("date", ""),
                    "current_price": price_info.get("current_price", 0.0),
                }
                loaded += 1
                logger.info(
                    f"KISMasterLoader: {symbol} 로드 완료 "
                    f"(base={self._master[symbol]['base_price']}, "
                    f"prev_high={self._master[symbol]['prev_high']}, "
                    f"prev_low={self._master[symbol]['prev_low']})"
                )
            except Exception as e:
                logger.error(f"KISMasterLoader: {symbol} 로드 중 오류: {e}")

        return loaded > 0

    def get_master(self, symbol: str) -> Optional[Dict[str, Any]]:
        """캐싱된 종목 마스터 정보를 반환합니다. 없으면 None."""
        return self._master.get(symbol)

    def get_all(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._master)

    def get_nearest_contract(self, market: str) -> Optional[Dict[str, Any]]:
        """market(예: 'KP200'/'KQ150')에 대응하는 근월물 마스터 정보를 반환합니다.

        주의: 이 메서드는 `load()`가 이미 caller가 넘겨준 근월물 단축코드로
        호출됐다는 전제로 동작합니다(위 클래스 docstring 참고). 자동 판별은
        하지 않으므로, market -> symbol 매핑은 config에서 관리하세요.
        """
        for info in self._master.values():
            if info.get("market") == market:
                return info
        return None

    def as_prev_day_ohlc_feature(self) -> "PrevDayOHLCFeature":
        """변동성 돌파 전략이 `feature_dict["last_days_ohlc_price"]`로 쓰는 어댑터를 반환합니다."""
        return PrevDayOHLCFeature(self)


class PrevDayOHLCFeature:
    """`KISMasterLoader` 캐시를 `{symbol: {YYYYMMDD: {"futs_hgpr", "futs_lwpr"}}}` 형태로 노출합니다.

    `VolBreakingLong/Short._update_vol_size()`가 기대하는 `call_feature()` 인터페이스입니다.
    `load()`가 끝난 뒤의 캐시를 읽기만 하므로 네트워크 호출이 없습니다.
    """

    def __init__(self, loader: KISMasterLoader):
        self._loader = loader

    def call_feature(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        out: Dict[str, Dict[str, Dict[str, float]]] = {}
        for symbol, info in self._loader.get_all().items():
            date = info.get("prev_date")
            if date and info.get("prev_high") and info.get("prev_low"):
                out[symbol] = {date: {"futs_hgpr": info["prev_high"], "futs_lwpr": info["prev_low"]}}
        return out
