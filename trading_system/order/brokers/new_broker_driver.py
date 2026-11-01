import logging
import uuid
from typing import Any, Dict, Optional

from trading_system.types.enums import BuySell, PriceType


logger = logging.getLogger(__name__)


class NewBrokerDriver:
    """신규 DMA/외부 주문 프로그램용 Driver 스켈레톤.

    현재는 실제 전송(HTTP/TCP/MQ/IPC)을 구현하지 않고,
    인터페이스와 파라미터/반환 형태를 고정하는 것이 목표입니다.
    """

    def __init__(self, config: Dict[str, Any]):
        # NOTE: 외부 DMA 연동 정보(전송 방식/MQ 주소/포트 등)는 여기 config로 주입될 예정입니다.
        #       현재는 dummy 모드로만 동작합니다.
        self.config = config or {}

    # --- STOCK (현재 스코프 밖) ---
    async def order_stock(self, **kwargs):  # pragma: no cover
        raise NotImplementedError("NewBrokerDriver.order_stock is not implemented")

    async def cancel_stock_order(self, **kwargs):  # pragma: no cover
        raise NotImplementedError("NewBrokerDriver.cancel_stock_order is not implemented")

    async def get_stock_balance(self, **kwargs):  # pragma: no cover
        raise NotImplementedError("NewBrokerDriver.get_stock_balance is not implemented")

    async def get_stock_orders(self, **kwargs):  # pragma: no cover
        raise NotImplementedError("NewBrokerDriver.get_stock_orders is not implemented")

    async def get_stock_price(self, **kwargs):  # pragma: no cover
        raise NotImplementedError("NewBrokerDriver.get_stock_price is not implemented")

    # --- FUTURE ---
    async def order_future(
        self,
        symbol: str,
        quantity: int,
        price: float,
        side: BuySell,
        price_type: PriceType = PriceType.LIMIT,
        client_order_id: Optional[str] = None,
        time_in_force: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """외부 DMA로 주문 커맨드를 전송.

        현재는 더미 응답을 반환합니다.

        반환 포맷 의도:
        - KIS `RestApi.order_future()`가 dict 응답을 반환하는 패턴과 유사하게 맞춰,
          전략/상위 코드가 일관되게 `output.order_id`(또는 `output.ODNO`) 등을 추출할 수 있게 합니다.
        - 실전 연동에서는 이 값이 "주문 접수"를 보장하지 않을 수 있으며,
          최종 정합성은 반드시 ExecutionReport(ACK 이벤트)로 맞춥니다.
        """
        if client_order_id is None:
            client_order_id = str(uuid.uuid4())

        logger.info(
            "[NewBrokerDriver] order_future(symbol=%s, qty=%s, price=%s, side=%s, price_type=%s, client_order_id=%s, tif=%s)",
            symbol,
            quantity,
            price,
            getattr(side, 'name', side),
            getattr(price_type, 'name', price_type),
            client_order_id,
            time_in_force,
        )

        # dummy 모드: 연동 DMA의 전송 계층(HTTP/TCP/MQ/IPC)이 정해지면 이 지점에서 OrderCommand를 전송한다.
        return {
            "rt_cd": "0",
            "msg1": "DUMMY_OK",
            "output": {
                "client_order_id": client_order_id,
                "order_id": f"NB-{client_order_id}",
                "symbol": symbol,
                "qty": quantity,
                "price": price,
            },
        }

    async def cancel_future_order(
        self,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        logger.info(
            "[NewBrokerDriver] cancel_future_order(order_id=%s, client_order_id=%s)",
            order_id,
            client_order_id,
        )
        return {"rt_cd": "0", "msg1": "DUMMY_OK"}

    async def get_future_orders(self, **kwargs) -> Dict[str, Any]:
        logger.info("[NewBrokerDriver] get_future_orders")
        return {"rt_cd": "0", "msg1": "DUMMY_OK", "output": []}

    async def get_future_account_balance(self, **kwargs) -> Dict[str, Any]:
        logger.info("[NewBrokerDriver] get_future_account_balance")
        return {"rt_cd": "0", "msg1": "DUMMY_OK", "output": {}}

    async def get_future_price(self, **kwargs) -> Dict[str, Any]:
        logger.info("[NewBrokerDriver] get_future_price")
        return {"rt_cd": "1", "msg1": "NOT_SUPPORTED"}
