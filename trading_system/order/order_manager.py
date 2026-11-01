import logging
from typing import Dict

# 각 드라이버는 설정에 해당 broker가 있을 때만 lazy import합니다.
# - KIS: aiohttp/pandas 필요 → KIS 설정이 있을 때만 import
# - NewBroker: 항상 사용 가능

logger = logging.getLogger(__name__)

class OrderManager:
    """
    모든 브로커(증권사/DMA) 주문·조회 드라이버를 관리하는 registry.

    설계 의도(옵션 A):
    - `OrderManager`는 "어떤 broker key가 어떤 Driver로 연결되는지"만 책임집니다.
    - 전략 친화 API(브로커 선택 제거, client_order_id 생성/등록 등)는 `OrderService`가 담당합니다.

    즉,
    - 구성 레이어(예: `StrategyManager`)가 `resolve_broker()`로 broker를 결정
    - `OrderManager.for_broker()`로 broker가 바인딩된 `OrderService`를 생성
    - 전략은 그 `OrderService`만 사용
    하는 구조를 고정합니다.
    """
    def __init__(self, broker_config: Dict[str, dict], kill_switch=None):
        """
        broker_config:
            {
                "kis": { ... },
                "yak": { ... },
                ...
            }
        """
        self.drivers = {}
        # 모든 OrderService가 공유하는 Kill Switch(발동 시 리스크를 늘리는 신규 주문 차단).
        from trading_system.risk.kill_switch import KillSwitch
        self.kill_switch = kill_switch or KillSwitch()
        # NOTE: 이 로딩 로직 자체가 broker registry의 단일 진실(SSOT)입니다.
        #       전략/상위 레이어는 broker 문자열("kis", "new_broker")의 의미를
        #       "라우팅 키"로만 취급하고, 구현체/프로토콜(REST/MQ/TCP)은 Driver 내부에 숨깁니다.
        #       각 드라이버는 설정에 포함된 경우에만 import & 생성됩니다.
        if "kis" in broker_config:
            from trading_system.order.brokers.kis_driver import KISDriver
            self.drivers["kis"] = KISDriver(broker_config["kis"])
            logger.info("Loaded KIS driver")
        if "new_broker" in broker_config:
            from trading_system.order.brokers.new_broker_driver import NewBrokerDriver
            self.drivers["new_broker"] = NewBrokerDriver(broker_config["new_broker"])
            logger.info("Loaded NewBroker driver")

    def get_driver(self, broker: str):
        """broker key로 Driver를 조회합니다.

        - 전략이 직접 호출하는 것이 아니라, `OrderService`/`OrderManager` 내부에서만 사용되는 것을 권장합니다.
        """
        if broker not in self.drivers:
            raise ValueError(f"Unknown broker: {broker}")
        return self.drivers[broker]

    def for_broker(
        self,
        broker: str,
        *,
        strategy_id: str = "",
        order_registry=None,
    ):
        """브로커가 바인딩된 주문 서비스(`OrderService`)를 생성합니다.

        - 옵션 A 고정: `OrderManager`는 driver registry 역할, 전략 친화 API는 `OrderService`가 담당
        - 생성 경로를 표준화하여(SSOT) 곳곳에서 `OrderService(...)`를 직접 만들지 않게 합니다.
        """
        # 순환 import 방지
        from trading_system.order.order_service import OrderService

        return OrderService(
            order_manager=self,
            broker=broker,
            strategy_id=strategy_id,
            order_registry=order_registry,
        )

    ## -------- Stock Orders --------
    async def place_stock_order(self, broker, **params):
        logger.info(f"[OrderManager] Placing STOCK order via {broker}")
        return await self.get_driver(broker).order_stock(**params)

    async def cancel_stock_order(self, broker, **params):
        logger.info(f"[OrderManager] Cancelling STOCK order via {broker}")
        return await self.get_driver(broker).cancel_stock_order(**params)

    async def get_stock_balance(self, broker, **params):
        logger.info(f"[OrderManager] Getting STOCK balance via {broker}")
        return await self.get_driver(broker).get_stock_balance(**params)

    async def get_stock_orders(self, broker, **params):
        logger.info(f"[OrderManager] Getting STOCK orders via {broker}")
        return await self.get_driver(broker).get_stock_orders(**params)

    async def get_stock_price(self, broker, **params):
        logger.info(f"[OrderManager] Getting STOCK price via {broker}")
        return await self.get_driver(broker).get_stock_price(**params)

    ## -------- Future Orders --------
    async def place_future_order(self, broker, **params):
        logger.info(f"[OrderManager] Placing FUTURE order via {broker}")
        # NOTE: 주문 결과(Received/Filled/Rejected/...)는 여기서 반환값으로만 끝나지 않고,
        #       외부의 ExecutionReport가 `QuoteManager.on_ack()`로 들어와 전략/Book을 갱신해야
        #       정합성이 유지됩니다.
        return await self.get_driver(broker).order_future(**params)

    async def cancel_future_order(self, broker, **params):
        logger.info(f"[OrderManager] Cancelling FUTURE order via {broker}")
        return await self.get_driver(broker).cancel_future_order(**params)

    async def get_future_account_balance(self, broker, **params):
        logger.info(f"[OrderManager] Getting FUTURE account balance via {broker}")
        return await self.get_driver(broker).get_future_account_balance(**params)

    async def get_future_orders(self, broker, **params):
        logger.info(f"[OrderManager] Getting FUTURE orders via {broker}")
        return await self.get_driver(broker).get_future_orders(**params)

    async def get_future_price(self, broker, **params):
        logger.info(f"[OrderManager] Getting FUTURE price via {broker}")
        return await self.get_driver(broker).get_future_price(**params)
