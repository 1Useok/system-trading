# -*- coding: utf-8 -*-
"""KIS 실시간체결통보(H0IFCNI0) 커넥터.

`RestStreamer`(KIS WebSocket)를 통해 내 주문의 접수/체결/거절 통보를 수신하고,
`FuturesSimpleOrderAck`로 변환해 `ExecutionReportRouter`(manager)로 전달합니다.
브로커는 체결통보에 우리가 발급한 `client_order_id`를 돌려주지 않고 자신의
주문번호(order_no)만 주므로, 주문 시점에 `OrderService`가 등록해 둔
`OrderRegistry`의 order_no -> client_order_id 매핑으로 원래 주문을 정확히 복원합니다.
(order_no로 새 id를 지어내면 Router가 대상 전략을 찾지 못해 ACK가 모두 drop된다.)

주의(실거래 투입 전 확인 필요):
- `RestStreamer._parse_realtime_data()`의 H0IFCNI0 필드 인덱스는 검증되지 않은
  추정 매핑입니다. 실제 모의투자 체결통보 원문으로 재검증이 필요합니다.
- 재연결(disconnect) 이후 누락된 체결통보 복구는 `reconcile_via_rest()`를
  수동으로(또는 main.py의 주기 작업으로) 호출해 REST 조회로 보정해야 합니다.
  자동 트리거는 아직 구현하지 않았습니다.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Set

from trading_system.types.enums import AckType, BuySell, QuoteType
from trading_system.types.events import FuturesSimpleOrderAck
from trading_system.execution_report.order_registry import OrderRegistry

logger = logging.getLogger(__name__)


def _truthy_flag(value: Any) -> bool:
    """KIS의 Y/N 또는 1/0 형태 플래그 필드를 관용적으로 참으로 판정."""
    if value is None:
        return False
    v = str(value).strip().upper()
    return v in ("1", "Y", "YES", "TRUE")


class KISAckConnector:
    """KIS 실시간체결통보 커넥터. `NewBrokerConnector`와
    동일한 `set_manager()` 인터페이스를 제공합니다."""

    def __init__(
        self,
        streamer,
        hts_id: str,
        order_registry: OrderRegistry,
        broker_name: str = "kis",
        manager=None,
    ):
        self.streamer = streamer
        self.hts_id = hts_id
        self.order_registry = order_registry
        self.broker_name = broker_name
        self.manager = manager

        # event_id 기반 중복 제거(`NewBrokerConnector` 수준의 안전장치를 여기에도 둔다).
        self._seen_event_ids: Set[str] = set()

    def set_manager(self, manager) -> None:
        self.manager = manager

    async def start(self) -> bool:
        """체결통보 구독을 시작합니다."""
        if not self.hts_id:
            logger.warning("KISAckConnector: HTS ID가 설정되지 않아 체결통보를 구독할 수 없습니다.")
            return False
        try:
            await self.streamer.subscribe(self.hts_id, quote_type=QuoteType.EXECUTION, callback=self._on_execution)
            logger.info(f"KISAckConnector: 체결통보 구독 시작 (hts_id={self.hts_id})")
            return True
        except Exception as e:
            logger.error(f"KISAckConnector: 체결통보 구독 실패: {e}")
            return False

    async def stop(self) -> None:
        try:
            await self.streamer.unsubscribe(self.hts_id)
        except Exception as e:
            logger.debug(f"KISAckConnector: 구독 해제 중 오류(무시): {e}")

    def _on_execution(self, data: Dict[str, Any]) -> None:
        """`RestStreamer`가 파싱한 체결통보 dict를 받아 ACK로 변환/dispatch."""
        ack = self._to_ack(data)
        if ack is None:
            return

        if ack.event_id and ack.event_id in self._seen_event_ids:
            logger.warning(f"KISAckConnector: 중복 체결통보 drop (event_id={ack.event_id})")
            return
        if ack.event_id:
            self._seen_event_ids.add(ack.event_id)

        if self.manager:
            self.manager.on_ack(ack)
        else:
            logger.warning("KISAckConnector: manager가 설정되지 않아 ACK를 dispatch할 수 없습니다.")

    def _to_ack(self, data: Dict[str, Any]) -> Optional[FuturesSimpleOrderAck]:
        order_no = str(data.get("order_num") or "").strip()
        if not order_no:
            logger.warning("KISAckConnector: order_num 없는 체결통보, drop")
            return None

        rfus_yn = data.get("rfus_yn")
        cntg_yn = data.get("cntg_yn")
        acpt_yn = data.get("acpt_yn")

        if _truthy_flag(rfus_yn):
            ack_type = AckType.Rejected
        elif _truthy_flag(cntg_yn):
            ack_type = AckType.Filled
        elif _truthy_flag(acpt_yn):
            ack_type = AckType.Received
        else:
            # 관례가 다를 수 있어(0/1 반전 등) 방어적으로 Nothing 대신 Received로 처리하지 않고
            # 명시적으로 Nothing을 반환해 잘못된 상태 갱신을 막는다.
            logger.warning(
                f"KISAckConnector: ack_type 판정 불가 (rfus={rfus_yn}, cntg={cntg_yn}, acpt={acpt_yn}) - drop"
            )
            return None

        buysell_raw = str(data.get("buysell") or "")
        buysell = BuySell.BUY if buysell_raw in ("02", "BUY") else (
            BuySell.SELL if buysell_raw in ("01", "SELL") else BuySell.NA
        )

        # client_order_id 복원: 주문 낼 때 OrderService가 REST 응답의 order_no를
        # OrderRegistry에 미리 등록해둔 매핑을 사용한다(임의로 새로 만들지 않음).
        client_order_id = self.order_registry.resolve_client_order_id_by_order_no(order_no) or ""
        strategy_id = self.order_registry.resolve_strategy_id(client_order_id) if client_order_id else None

        filled_volume = int(data.get("filled_volume") or 0)
        filled_price = float(data.get("filled_price") or 0.0)
        event_id = f"kis-{order_no}-{ack_type.name}-{filled_volume}-{filled_price}"

        return FuturesSimpleOrderAck(
            ack_type=ack_type,
            inst_code=str(data.get("symbol") or ""),
            order_num=order_no,
            filled_price=filled_price,
            filled_volume=filled_volume,
            buysell=buysell,
            broker=self.broker_name,
            strategy_id=strategy_id or "",
            client_order_id=client_order_id,
            event_id=event_id,
        )

    async def reconcile_via_rest(self, order_service) -> int:
        """재연결 등으로 놓쳤을 수 있는 체결/주문상태를 REST 조회로 보정한다.

        자동으로 트리거되지 않으므로, 재연결 감지 로직(main.py 등)에서
        명시적으로 호출해야 합니다. 반환값은 보정을 시도한 주문 수입니다.

        NOTE: 이 메서드는 스캐폴딩입니다 - `get_future_orders()`의 실제 반환
        스키마(DataFrame 컬럼명)에 맞춰 별도 검증이 필요합니다.
        """
        try:
            orders = await order_service.get_future_orders()
        except Exception as e:
            logger.error(f"KISAckConnector: reconcile_via_rest 조회 실패: {e}")
            return 0

        count = 0
        try:
            df = orders[0] if isinstance(orders, tuple) else orders
            if hasattr(df, "iterrows"):
                for _, row in df.iterrows():
                    order_no = str(row.get("odno") or row.get("ODNO") or "").strip()
                    if not order_no:
                        continue
                    client_order_id = self.order_registry.resolve_client_order_id_by_order_no(order_no)
                    if not client_order_id:
                        continue
                    count += 1
        except Exception as e:
            logger.error(f"KISAckConnector: reconcile_via_rest 처리 오류: {e}")

        logger.info(f"KISAckConnector: reconcile_via_rest 완료 (확인된 주문 {count}건)")
        return count
