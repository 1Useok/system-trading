"""`client_order_id` ↔ `strategy_id` 매핑(라우팅) 저장소.

왜 필요한가?
- 시스템은 ExecutionReport/ACK(`FuturesSimpleOrderAck`)를 전략으로 전달할 때
  "어느 전략이 낸 주문의 결과인지"를 알아야 합니다.
- 전략이 `ack.broker`를 필터링하는 방식은 전략이 실행 경로를 의식하게 만들고(결합도↑)
  ACK 라우팅을 불명확하게 만들므로 쓰지 않습니다.

설계:
- 주문을 낼 때 `OrderService`가 `client_order_id`를 생성
- `OrderRegistry`에 `client_order_id -> strategy_id`를 SSOT로 등록
- ACK 수신 시 `ExecutionReportRouter`가
  1) `ack.strategy_id`가 있으면 그것을 우선 사용하고
  2) 없으면 `ack.client_order_id`로 registry를 역참조
  3) 그래도 못 찾으면 drop + warning (오배달 방지)
  하는 방식으로 타겟 디스패치합니다.

주의/한계:
- 현재 구현은 메모리(in-process) 기반이므로 프로세스 재시작 시 매핑이 초기화됩니다.
  운영 환경에서 "재시작/복구"까지 다루려면 DB/파일/외부 DMA 쪽 저장 등으로 확장해야 합니다.
- `QuoteManager`/`NewBrokerConnector`는 동기 호출 경로가 많아 thread-safe가 유리하므로
  간단한 `threading.Lock`을 사용합니다.
"""

import threading
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class OrderRegistryEntry:
    client_order_id: str
    strategy_id: str


class OrderRegistry:
    """주문 라우팅을 위한 `client_order_id -> strategy_id` 매핑 저장소(SSOT)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._by_client_order_id: Dict[str, OrderRegistryEntry] = {}
        # 브로커가 발급한 실제 주문번호(order_no) -> client_order_id.
        # 브로커의 체결통보/응답은 보통 client_order_id가 아니라 브로커 고유의 주문번호만
        # 담고 있으므로(예: KIS의 `ODNO`/`oder_no`), 주문을 낼 때(REST 응답으로 order_no를
        # 동기적으로 받는 시점) 이 매핑을 등록해두고, ACK 수신 시 이 매핑으로 client_order_id를
        # 역참조한 뒤 위 `_by_client_order_id`로 strategy_id까지 2단계로 역참조합니다.
        self._order_no_to_client_order_id: Dict[str, str] = {}

    def register(self, client_order_id: str, strategy_id: str) -> None:
        """주문 의도(`client_order_id`)가 어느 전략에서 나왔는지 기록합니다.

        - 동일한 `client_order_id`가 다시 등록되면 마지막 값으로 덮어씁니다.
          (일반적으로 `client_order_id`는 유일해야 합니다.)
        """
        if not client_order_id or not strategy_id:
            return
        with self._lock:
            self._by_client_order_id[client_order_id] = OrderRegistryEntry(
                client_order_id=client_order_id,
                strategy_id=strategy_id,
            )

    def resolve_strategy_id(self, client_order_id: str) -> Optional[str]:
        """ACK에서 들어온 `client_order_id`로 전략 식별자를 역참조합니다."""
        if not client_order_id:
            return None
        with self._lock:
            entry = self._by_client_order_id.get(client_order_id)
            return entry.strategy_id if entry else None

    def register_order_no(self, order_no: str, client_order_id: str) -> None:
        """브로커 주문번호(`order_no`) <-> `client_order_id` 매핑을 기록합니다.

        주문 전송 REST 응답으로 브로커의 실제 주문번호를 받는 시점(예: KIS의
        `ODNO`)에 호출해야 합니다. 체결통보가 `client_order_id`를 그대로 echo
        해주지 않는 브로커(KIS 등)에서, 나중에 `order_no`만 가진 체결통보가
        도착했을 때 원래 주문의 `client_order_id`를 찾기 위해 필요합니다.
        """
        if not order_no or not client_order_id:
            return
        with self._lock:
            self._order_no_to_client_order_id[str(order_no)] = client_order_id

    def resolve_client_order_id_by_order_no(self, order_no: str) -> Optional[str]:
        """브로커 주문번호(`order_no`)로 `client_order_id`를 역참조합니다."""
        if not order_no:
            return None
        with self._lock:
            return self._order_no_to_client_order_id.get(str(order_no))
