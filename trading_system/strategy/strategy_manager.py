# -*- coding: utf-8 -*-
"""
전략 관리 모듈

`StrategyManager`는 시스템의 구성(composition) 레이어입니다.
시세 / 주문 / 체결통보 경로의 객체를 만들고 서로 연결하며, 전략 생성 시 의존성을 주입합니다.

1) 구성(주입) 레이어로서의 SSOT
   - 전략이 `broker`를 직접 선택하지 않도록 하고,
     `execution_routing.resolve_broker()` + `config.yaml`의 `execution.*` 규칙으로만
     주문 라우팅을 결정합니다.

2) 이벤트/주문 경로의 분리 고정
   - Market Data: `KISConnector` → `MarketDataHub` → `Strategy.on_quote/on_filled`
   - Execution Command: `Strategy` → (주입된) `OrderService` → `OrderManager` → `Driver`
   - ExecutionReport: `NewBrokerConnector`/`KISAckConnector` → `ExecutionReportRouter.on_ack()` → `Strategy.on_ack`

3) `strategy_id` 기반 ACK 라우팅
   - 주문 시 `OrderService`가 `client_order_id`를 만들고 `OrderRegistry`에 등록
   - ACK 수신 시 `ExecutionReportRouter`가 `ack.strategy_id` 또는 `client_order_id`로
     특정 전략으로 타겟 디스패치
"""
import asyncio
import inspect
import logging
import os
from typing import Any, Dict, Optional, Type

from trading_system.order.execution_routing import resolve_broker
from trading_system.order.order_manager import OrderManager
from trading_system.order.order_service import OrderService
from trading_system.execution_report.order_registry import OrderRegistry
from trading_system.market_data.market_data_hub import MarketDataHub
from trading_system.execution_report.execution_report_router import ExecutionReportRouter
from trading_system.execution_report.connectors.new_broker_connector import NewBrokerConnector
from trading_system.execution_report.connectors.kis_ack_connector import KISAckConnector
from trading_system.strategy.execution.strategy_executor import StrategyExecutor

# KIS 모듈은 설정에 따라 lazy import합니다.
# - api_config에 KIS 설정이 있을 때만 import (aiohttp/pandas 필요)

logger = logging.getLogger(__name__)


class StrategyManager:

    def __init__(
        self,
        api_config: Optional[Dict[str, Any]],
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        executor: Optional[StrategyExecutor] = None,
        state_dir: Optional[str] = None,
    ):
        """
        Args:
            api_config: `ConfigLoader.get_config()` 결과
            state_dir: 재시작 복구용 상태 파일 디렉터리(주문 매핑, 실행 리포트 체크포인트).
                None이면 메모리에만 둔다(테스트/드라이런용).
        """
        self.api_config: Dict[str, Any] = api_config or {}
        self.state_dir = state_dir

        # KIS 초기화 (설정에 KIS가 있을 때만 import & 생성)
        #
        # 토큰 분리: 선물옵션 실시간 시세(H0IFCNT0/H0IFASP0)는 KIS 정책상 "실전계좌 전용"
        # TR_ID라 VPS(모의투자) 토큰으로는 구독해도 데이터가 오지 않는다. 반대로 주문/잔고/
        # 체결통보는 계좌가 존재하는 VPS(모의투자) 토큰을 써야 한다.
        #   KISTokenManager.get_instance_by_context('strategy')  # -> PROD, 시세용
        #   RestApi(config, context='order')                     # -> VPS, 주문/잔고용
        self.token_manager = None      # VPS: 주문/잔고/체결통보
        self.market_data_token_manager = None  # PROD: 실시간 시세(WebSocket)
        self.rest_api = None
        if self._has_kis_config(self.api_config):
            from trading_system.api.kis.token_manager import KISTokenManager
            from trading_system.api.kis.rest_api import RestApi

            self.token_manager = KISTokenManager()
            self.rest_api = RestApi(self.api_config, context='order')

            prod_cfg = self.api_config.get('prod_api') or {}
            if not prod_cfg.get('app_key') or not prod_cfg.get('app_secret'):
                logger.warning(
                    "PROD_KIS_APP_KEY/PROD_KIS_APP_SECRET이 .env에 없습니다. "
                    "실시간 선물 시세(H0IFCNT0/H0IFASP0)는 실전계좌 전용이라 "
                    "VPS 키만으로는 데이터가 오지 않을 수 있습니다."
                )
            self.market_data_token_manager = KISTokenManager.get_instance_by_context('strategy')
            self.market_data_token_manager.configure(prod_cfg)

            logger.info("KIS initialized (order: vps token_manager + rest_api, market data: prod token_manager)")
        else:
            logger.info("No KIS config - skipping KIS init")

        # strategy_id 기반 ACK 라우팅을 위한 registry
        # 장중 재시작 후에도 이전 주문의 체결통보가 원래 전략으로 가도록 매핑을 파일로 남긴다.
        self.order_registry = OrderRegistry.for_trading_day(state_dir) if state_dir else OrderRegistry()

        # 단일 asyncio 루프 전제:
        # - 전략 콜백 실행은 `StrategyExecutor`만 수행
        # - Hub/Router는 executor로 publish만 수행
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError as e:
                raise RuntimeError(
                    "StrategyManager requires a running asyncio loop. "
                    "Use `asyncio.run(...)` and pass `loop=asyncio.get_running_loop()` (or create a loop explicitly)."
                ) from e

        self.loop = loop
        self.executor = executor or StrategyExecutor(loop=self.loop)

        # 시세와 체결통보는 물리적으로 분리된 경로로 흐른다:
        # - Market Data는 `MarketDataHub`로
        # - ExecutionReport(ACK)는 `ExecutionReportRouter`로
        self.market_data_hub = MarketDataHub(loop=self.loop, executor=self.executor)
        self.execution_report_router = ExecutionReportRouter(order_registry=self.order_registry, loop=self.loop, executor=self.executor)

        # 주문 경로: Strategy -> OrderService -> OrderManager -> Driver
        self.order_manager = OrderManager(self._build_broker_config(self.api_config))

        # Connector 구성
        # - Market Data: KISConnector (market-data-only)
        # - ExecutionReport: NewBrokerConnector

        # KIS 시장 데이터 커넥터 (KIS 설정이 있을 때만) - PROD 토큰으로 시세 구독
        self.kis_connector = None
        if self.market_data_token_manager is not None:
            from trading_system.market_data.connectors.kis_connector import KISConnector
            self.kis_connector = KISConnector(self.market_data_token_manager)
            self.market_data_hub.add_connector(self.kis_connector)

        # ExecutionReport 전용 커넥터(네트워크 구현 전에도 드라이런 가능)
        self.new_broker_connector = NewBrokerConnector(
            broker_name="new_broker",
            checkpoint_path=os.path.join(state_dir, "new_broker_checkpoint.json") if state_dir else None,
        )
        self.execution_report_router.add_connector(self.new_broker_connector)

        # KIS 체결통보 커넥터 (KIS 설정이 있고 execution.allow_kis_ack가 true일 때만)
        #
        # 체결통보는 "내 주문"(VPS 계좌) 기준이므로 시세용(PROD) RestStreamer와
        # 공유하지 않고, VPS 토큰으로 인증하는 별도 RestStreamer를 새로 만든다.
        # 두 RestStreamer는 각자 _approval_key를 하나만 들고 있어서(PROD/VPS 중
        # 하나), 시세(PROD)와 체결통보(VPS)를 같은 인스턴스로 같이 구독할 수 없다.
        self.kis_ack_connector = None
        execution_cfg = self.api_config.get("execution") or {}
        if self.token_manager is not None and execution_cfg.get("allow_kis_ack", True):
            vps_cfg = self.api_config.get("vps_api") or {}
            hts_id = vps_cfg.get("hts_id") or ""
            if not hts_id:
                logger.warning(
                    "KIS HTS ID(.env의 KIS_HTS_ID)가 설정되지 않아 KISAckConnector를 "
                    "생성하지 않습니다. 체결통보 수신이 불가능한 상태입니다."
                )
            else:
                from trading_system.api.kis.rest_streamer import RestStreamer
                self._kis_ack_streamer = RestStreamer(self.token_manager)  # VPS -> 모의투자 WebSocket
                self.kis_ack_connector = KISAckConnector(
                    streamer=self._kis_ack_streamer,
                    hts_id=hts_id,
                    order_registry=self.order_registry,
                    broker_name="kis",
                    rest_api=self.rest_api,
                    account=vps_cfg.get("account_no") or "",
                )
                self.execution_report_router.add_connector(self.kis_ack_connector)
                logger.info("KISAckConnector initialized (vps token)")

    @staticmethod
    def _has_kis_config(api_config: Dict[str, Any]) -> bool:
        """KIS 관련 설정이 존재하는지 확인합니다."""
        if not api_config:
            return False
        # KIS 설정이 있는 경우: api_key 또는 kis 관련 설정이 존재
        if api_config.get("api_key") or api_config.get("kis"):
            return True
        # execution에서 kis를 사용하는 경우
        execution_cfg = api_config.get("execution") or {}
        if execution_cfg.get("default_broker") == "kis":
            return True
        return False

    @staticmethod
    def _build_broker_config(api_config: Dict[str, Any]) -> Dict[str, dict]:
        """OrderManager에 주입할 broker_config 구성.

        - `kis` 드라이버는 RestApi가 필요로 하는 전체 config(dict)를 받습니다.
        - `new_broker`는 별도 설정(dict)만 받습니다.
        """
        broker_cfg: Dict[str, dict] = {}

        # kis는 KIS 설정이 있을 때만 추가
        if StrategyManager._has_kis_config(api_config):
            broker_cfg["kis"] = api_config

        # 신규 브로커는 별도 섹션에서 활성화
        brokers_section = api_config.get("brokers") or {}
        if "new_broker" in brokers_section:
            broker_cfg["new_broker"] = brokers_section.get("new_broker") or {}

        return broker_cfg

    def get_order_service(self, strategy_name: str, symbol: Optional[str] = None) -> OrderService:
        """전략에 주입할 OrderService를 생성합니다.

        라우팅(브로커 선택)은 오직 config 기반으로 결정됩니다.
        """
        # 라우팅은 오직 config(execution.*)에서만 결정됩니다.
        execution_cfg = self.api_config.get("execution") or {}
        broker = resolve_broker(execution_cfg, strategy_name=strategy_name, symbol=symbol)
        return self.order_manager.for_broker(
            broker,
            strategy_id=strategy_name,
            order_registry=self.order_registry,
        )

    def register_strategy(self, strategy) -> None:
        """전략을 MarketDataHub/ExecutionReportRouter에 모두 등록합니다."""
        self.market_data_hub.register_strategy(strategy)
        self.execution_report_router.register_strategy(strategy)

        # executor 등록은 StrategyManager가 SSOT
        strategy_id = getattr(strategy, "strategy_id", None) or getattr(strategy, "strategy_name", None)
        if strategy_id and not self.executor.is_registered(strategy_id=strategy_id):
            self.executor.register(strategy_id=strategy_id, strategy=strategy)

    def build_strategy(
        self,
        strategy_cls: Type,
        *args,
        strategy_name: Optional[str] = None,
        symbol: Optional[str] = None,
        **kwargs,
    ):
        """전략 객체를 생성하면서, 가능한 경우 표준 의존성(`OrderService` 등)을 자동 주입합니다.

        - 전략은 `OrderService`만 사용하고 브로커 선택을 직접 하지 않습니다.
        - 브로커 선택은 `get_order_service(strategy_name, symbol)`에서만 결정됩니다.
        """
        inferred_name = strategy_name
        if inferred_name is None:
            if args and isinstance(args[0], str):
                inferred_name = args[0]
            else:
                inferred_name = kwargs.get("_strategy_name") or kwargs.get("strategy_name")

        sig = inspect.signature(strategy_cls.__init__)
        params = sig.parameters

        # 기존 전략 시그니처 호환:
        # - 과거에는 `market_data_hub`로 이벤트 허브를 주입했으나,
        #   Market Data와 ACK가 분리되어 있으므로 market data hub만 주입합니다.
        if "market_data_hub" in params and "market_data_hub" not in kwargs:
            kwargs["market_data_hub"] = self.market_data_hub
        if "rest_api" in params and "rest_api" not in kwargs and self.rest_api is not None:
            kwargs["rest_api"] = self.rest_api
        if "order_service" in params and "order_service" not in kwargs and inferred_name:
            kwargs["order_service"] = self.get_order_service(strategy_name=inferred_name, symbol=symbol)

        return strategy_cls(*args, **kwargs)

    def ingest_execution_reports(self, reports) -> None:
        """드라이런/테스트를 위해 외부 ExecutionReport 이벤트를 내부 이벤트 버스로 주입합니다."""
        self.new_broker_connector.ingest_many(reports)

    async def start_kis_ack(self) -> bool:
        """KIS 체결통보(H0IFCNI0) 구독 시작."""
        if self.kis_ack_connector is None:
            logger.warning(
                "KISAckConnector is not initialized (KIS_HTS_ID 설정을 확인하세요)"
            )
            return False
        return await self.kis_ack_connector.start()

    async def stop_kis_ack(self) -> None:
        """KIS 체결통보 구독 종료."""
        if self.kis_ack_connector is not None:
            await self.kis_ack_connector.stop()

    async def start_kis(self, symbols: list) -> bool:
        """KIS 시세 커넥터를 시작하고 종목을 구독합니다.

        Args:
            symbols: 구독할 KIS 단축코드 목록 (예: ["A01612"])

        Returns:
            성공 여부
        """
        if self.kis_connector is None:
            logger.warning("KISConnector is not initialized")
            return False
        if not symbols:
            logger.warning("start_kis: 구독할 종목이 없습니다.")
            return False

        for symbol in symbols:
            try:
                await self.kis_connector.start(symbol)
            except Exception as e:
                logger.error(f"start_kis: {symbol} 구독 실패: {e}")

        logger.info(f"KIS market data started with symbols: {symbols}")
        return True

    async def stop_kis(self) -> None:
        """KIS 시세 커넥터를 중지합니다."""
        if self.kis_connector is None:
            return
        for symbol in list(self.kis_connector.subscribed_symbols):
            await self.kis_connector.stop(symbol)
        logger.info("KIS market data stopped")