"""전략이 사용하는 주문 실행 Facade(`OrderService`).

설계 의도:
- 전략은 브로커/프로토콜/드라이버를 직접 선택하지 않고, 주입된 `OrderService`만 호출한다.
- 브로커 선택은 구성(설정) 레이어(`StrategyManager` + `execution_routing.resolve_broker`)에서만 일어난다.
- 주문을 식별하기 위한 `client_order_id`를 OMS 내부에서 생성하고,
  ACK 라우팅을 위해 `OrderRegistry`에 매핑을 남긴다.

즉 호출 흐름은 다음과 같습니다.

`Strategy` → `OrderService` → `OrderManager` → `Driver(KIS/NewBroker)` → (외부)
"""

import datetime
import logging
import uuid
from dataclasses import dataclass
from typing import Optional

from trading_system.types.enums import BuySell, PriceType
from trading_system.order.order_manager import OrderManager
from trading_system.execution_report.order_registry import OrderRegistry

logger = logging.getLogger(__name__)


def _default_client_order_id(prefix: str = "") -> str:
    """OMS 내부에서 생성하는 idempotency key.

    - 외부 DMA/브로커로 주문 커맨드를 보낼 때 함께 전달되며
      "동일 주문 의도" 재전송 시 중복 실행을 방지하는 키로 사용됩니다.
    - 기본적으로 `strategy_id`를 prefix로 포함해, 운영 로그/추적이 쉬워지게 합니다.
    """
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
    rand = uuid.uuid4().hex[:8]
    if prefix:
        return f"{prefix}-{ts}-{rand}"
    return f"{ts}-{rand}"


@dataclass(frozen=True)
class OrderService:
    """전략에 주입되는 '브로커 바인딩' 주문 서비스.

    전략은 `broker` 문자열이나 프로토콜을 알지 않고, 표준 메서드만 호출합니다.

    이 클래스는 "비즈니스 로직"(전략 판단)을 담지 않으며,
    아래 역할만 수행하는 얇은 레이어로 유지하는 것이 원칙입니다.
    - `client_order_id` 생성
    - (옵션) `OrderRegistry`에 라우팅 매핑 등록
    - 바인딩된 broker로 `OrderManager` 호출
    """

    order_manager: OrderManager
    broker: str
    strategy_id: str = ""
    order_registry: Optional[OrderRegistry] = None

    async def place_future_order(
        self,
        symbol: str,
        quantity: int,
        price: float,
        side: BuySell,
        price_type: PriceType = PriceType.LIMIT,
        account: Optional[str] = None,
        client_order_id: Optional[str] = None,
        time_in_force: Optional[str] = None,
        risk_reducing: bool = False,
        **kwargs,
    ):
        """선물 주문을 실행합니다.

        `risk_reducing=True`는 청산/손절처럼 포지션을 줄이는 주문이다. Kill Switch가 발동된
        상태에서는 이 주문만 통과하고, 나머지는 브로커로 보내지 않고 거부 응답(rt_cd != '0')을 돌려준다.

        주의:
        - `broker`는 이미 바인딩되어 있으므로, 전략은 인자로 전달하지 않습니다.
        - `filled`(시장 틱)과 달리, "내 주문 체결"은 반드시 `FuturesSimpleOrderAck(ack_type=Filled)`
          경로로 들어와야 하므로, 주문의 결과는 `NewBrokerConnector`/`KISConnector(enable_ack)`가
          만든 ACK 이벤트를 통해 수신됩니다.
        """
        kill_switch = getattr(self.order_manager, "kill_switch", None)
        if kill_switch is not None and not risk_reducing:
            rejection = kill_switch.rejection()
            if rejection is not None:
                logger.warning(f"[OrderService] {self.strategy_id} order blocked by kill switch: {symbol} {side} {quantity}")
                return rejection

        if client_order_id is None:
            client_order_id = _default_client_order_id(self.strategy_id)

        # client_order_id -> strategy_id 매핑은 SSOT(OrderRegistry)에 기록
        if self.order_registry is not None and self.strategy_id:
            self.order_registry.register(client_order_id=client_order_id, strategy_id=self.strategy_id)

        params = {
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "side": side,
            "price_type": price_type,
            "client_order_id": client_order_id,
            "time_in_force": time_in_force,
        }
        if account is not None:
            params["account"] = account
        params.update(kwargs)

        # NOTE: broker 결정은 구성 레이어에서 이미 완료됨
        result = await self.order_manager.place_future_order(self.broker, **params)

        # 브로커가 체결통보에서 client_order_id를 그대로 돌려주지 않는 경우(KIS 등),
        # 여기서 받은 실제 주문번호(order_no)를 client_order_id와 미리 매핑해둔다.
        # ACK 커넥터는 이 매핑으로 client_order_id -> strategy_id를 2단계 역참조한다.
        if self.order_registry is not None:
            order_no = self._extract_order_no(result)
            if order_no:
                self.order_registry.register_order_no(order_no=order_no, client_order_id=client_order_id)

        return result

    @staticmethod
    def _extract_order_no(result) -> Optional[str]:
        """브로커별로 다른 응답 형태에서 실제 주문번호를 추출합니다.

        - KIS: {"output": {"ODNO": ...}}
        - new_broker: {"output": {"order_id": ...}} 또는 {"order_num": ...}
        """
        if not isinstance(result, dict):
            return None
        output = result.get("output")
        if isinstance(output, dict):
            for key in ("ODNO", "order_id", "order_num"):
                value = output.get(key)
                if value:
                    return str(value)
        for key in ("order_id", "order_num"):
            value = result.get(key)
            if value:
                return str(value)
        return None

    async def cancel_future_order(
        self,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        account: Optional[str] = None,
        **kwargs,
    ):
        """선물 주문 취소를 요청합니다.

        외부 DMA/브로커 구현에 따라 `order_id`(브로커 오더번호) 또는 `client_order_id`로 취소합니다.
        """
        params = {
            "order_id": order_id,
            "client_order_id": client_order_id,
        }
        if account is not None:
            params["account"] = account
        params.update(kwargs)
        return await self.order_manager.cancel_future_order(self.broker, **params)

    async def get_future_account_balance(self, **params):
        """브로커 바인딩된 선물 계좌 잔고/포지션 조회.

        - 시작 시 스냅샷/복구(향후)에서 사용될 수 있습니다.
        - 반환 타입은 브로커 드라이버 구현에 따라 다를 수 있으므로,
          전략은 가능한 한 이 값을 직접 해석하기보다 전용 전략(예: `KrxExitAll`)에서만 사용합니다.
        """
        return await self.order_manager.get_future_account_balance(self.broker, **params)

    async def get_future_orders(self, **params):
        """브로커 바인딩된 선물 주문 조회(복구/검증용)."""
        return await self.order_manager.get_future_orders(self.broker, **params)
