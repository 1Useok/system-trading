# -*- coding: utf-8 -*-
"""KIS 실시간체결통보(H0IFCNI0) 커넥터.

`RestStreamer`(KIS WebSocket)를 통해 내 주문의 접수/정정/취소/거부/체결 통보를 수신하고,
`FuturesSimpleOrderAck`로 변환해 `ExecutionReportRouter`(manager)로 전달합니다.

- 브로커는 체결통보에 우리가 발급한 `client_order_id`를 돌려주지 않고 자신의 주문번호(order_no)만
  주므로, 주문 시점에 `OrderService`가 등록해 둔 `OrderRegistry`의 order_no -> client_order_id
  매핑으로 원래 주문과 전략을 복원합니다. (order_no로 새 id를 지어내면 Router가 대상 전략을
  찾지 못해 ACK가 모두 drop된다.)
- WebSocket이 끊겼다 다시 붙으면, 끊긴 동안 놓친 체결을 REST 주문체결내역(VTTO5201R)의
  총체결수량과 비교해 차이만큼 Filled ACK로 보정합니다(`reconcile_via_rest`).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, Optional, Set

from trading_system.types.enums import AckType, BuySell, QuoteType
from trading_system.types.events import FuturesSimpleOrderAck
from trading_system.execution_report.order_registry import OrderRegistry

logger = logging.getLogger(__name__)

# ODER_KIND2: '0'이면 체결통보, 그 외('L')는 주문·정정·취소·거부 접수 통보
_ORDER_KIND_FILL = "0"
# RCTF_CLS(정정구분): 0=신규, 1=정정, 2=취소
_RCTF_REPLACE = "1"
_RCTF_CANCEL = "2"


def _to_buysell(raw: Any) -> BuySell:
    """SELN_BYOV_CLS / SLL_BUY_DVSN_CD: 01=매도, 02=매수."""
    v = str(raw or "").strip()
    if v in ("02", "BUY"):
        return BuySell.BUY
    if v in ("01", "SELL"):
        return BuySell.SELL
    return BuySell.NA


class KISAckConnector:
    """KIS 실시간체결통보 커넥터. `NewBrokerConnector`와 동일한 `set_manager()` 인터페이스를 제공합니다."""

    def __init__(
        self,
        streamer,
        hts_id: str,
        order_registry: OrderRegistry,
        broker_name: str = "kis",
        manager=None,
        rest_api=None,
        account: str = "",
    ):
        """
        Args:
            streamer: 체결통보용 `RestStreamer` (주문 계좌와 같은 서버(VPS/PROD) 토큰)
            hts_id: KIS HTS ID (체결통보 구독 키)
            order_registry: order_no <-> client_order_id <-> strategy_id 매핑 저장소
            rest_api: 재연결 후 누락 체결 보정용 REST 클라이언트(없으면 보정 생략)
            account: 보정 조회 대상 계좌번호("12345678-03")
        """
        self.streamer = streamer
        self.hts_id = hts_id
        self.order_registry = order_registry
        self.broker_name = broker_name
        self.manager = manager
        self.rest_api = rest_api
        self.account = account

        # event_id 기반 중복 제거(실전/모의 체결통보를 두 채널로 받을 때도 한 번만 반영).
        self._seen_event_ids: Set[str] = set()
        # order_no -> 지금까지 전략에 전달한 누적 체결수량(재연결 보정의 기준).
        self._filled_qty: Dict[str, int] = defaultdict(int)

    def set_manager(self, manager) -> None:
        self.manager = manager

    async def start(self) -> bool:
        """체결통보 구독을 시작합니다."""
        if not self.hts_id:
            logger.warning("KISAckConnector: HTS ID가 설정되지 않아 체결통보를 구독할 수 없습니다.")
            return False
        try:
            ok = await self.streamer.subscribe(self.hts_id, quote_type=QuoteType.EXECUTION, callback=self._on_execution)
        except Exception as e:
            logger.error(f"KISAckConnector: 체결통보 구독 실패: {e}")
            return False
        if not ok:
            logger.error("KISAckConnector: 체결통보 구독 요청을 보내지 못했습니다(WebSocket 연결 실패).")
            return False
        if hasattr(self.streamer, "add_reconnect_listener"):
            self.streamer.add_reconnect_listener(self.reconcile_via_rest)
        logger.info(f"KISAckConnector: 체결통보 구독 시작 (hts_id={self.hts_id})")
        return True

    async def stop(self) -> None:
        try:
            await self.streamer.unsubscribe(self.hts_id)
        except Exception as e:
            logger.debug(f"KISAckConnector: 구독 해제 중 오류(무시): {e}")

    # ------------------------------------------------------------------
    # 실시간 체결통보
    # ------------------------------------------------------------------
    def _on_execution(self, data: Dict[str, Any]) -> None:
        """`RestStreamer`가 파싱한 체결통보 dict를 받아 ACK로 변환/dispatch."""
        self._dispatch(self._to_ack(data))

    def _dispatch(self, ack: Optional[FuturesSimpleOrderAck]) -> None:
        if ack is None:
            return
        if ack.event_id and ack.event_id in self._seen_event_ids:
            logger.debug(f"KISAckConnector: 중복 체결통보 drop (event_id={ack.event_id})")
            return
        if ack.event_id:
            self._seen_event_ids.add(ack.event_id)
        if ack.ack_type == AckType.Filled:
            self._filled_qty[ack.order_num] += int(ack.filled_volume)

        if self.manager:
            self.manager.on_ack(ack)
        else:
            logger.warning("KISAckConnector: manager가 설정되지 않아 ACK를 dispatch할 수 없습니다.")

    def _to_ack(self, data: Dict[str, Any]) -> Optional[FuturesSimpleOrderAck]:
        order_no = str(data.get("order_num") or "").strip()
        if not order_no:
            logger.warning("KISAckConnector: order_num 없는 체결통보, drop")
            return None

        order_kind = str(data.get("order_kind") or "").strip()
        rctf_cls = str(data.get("rctf_cls") or "").strip()
        refused = str(data.get("rfus_yn") or "").strip() == "1"
        qty = int(data.get("qty") or 0)
        price = float(data.get("price") or 0.0)

        filled_volume, filled_price, cancel_or_replace_volume = 0, -1.0, 0
        if order_kind == _ORDER_KIND_FILL:
            ack_type = AckType.Filled
            filled_volume, filled_price = qty, price
        elif refused:
            ack_type = AckType.Rejected
        elif rctf_cls == _RCTF_CANCEL:
            ack_type = AckType.Canceled
            cancel_or_replace_volume = qty
        elif rctf_cls == _RCTF_REPLACE:
            ack_type = AckType.Replaced
            cancel_or_replace_volume = qty
        else:
            ack_type = AckType.Received

        # 같은 주문의 부분체결이 여러 번 올 수 있으므로 체결시각/수량/가격까지 키에 포함한다.
        event_id = f"kis-{order_no}-{ack_type.name}-{data.get('filled_time', '')}-{qty}-{price}"
        return self._make_ack(
            ack_type=ack_type,
            order_no=order_no,
            original_order_no=str(data.get("original_order_num") or ""),
            symbol=str(data.get("symbol") or ""),
            buysell=_to_buysell(data.get("buysell")),
            filled_volume=filled_volume,
            filled_price=filled_price,
            cancel_or_replace_volume=cancel_or_replace_volume,
            event_id=event_id,
        )

    def _make_ack(self, *, ack_type: AckType, order_no: str, symbol: str, buysell: BuySell,
                  event_id: str, original_order_no: str = "", filled_volume: int = 0,
                  filled_price: float = -1.0, cancel_or_replace_volume: int = 0) -> FuturesSimpleOrderAck:
        # client_order_id 복원: OrderService가 주문 응답의 order_no로 등록해 둔 매핑을 쓴다.
        # 정정/취소는 새 주문번호로 접수되고 원주문번호가 따로 오므로, 원주문번호로도 찾는다.
        client_order_id = (
            self.order_registry.resolve_client_order_id_by_order_no(order_no)
            or self.order_registry.resolve_client_order_id_by_order_no(original_order_no)
            or ""
        )
        strategy_id = self.order_registry.resolve_strategy_id(client_order_id) if client_order_id else None
        return FuturesSimpleOrderAck(
            ack_type=ack_type,
            account=self.account,
            inst_code=symbol,
            order_num=order_no,
            original_order_num=original_order_no,
            cancel_or_replace_volume=cancel_or_replace_volume,
            filled_price=filled_price,
            filled_volume=filled_volume,
            buysell=buysell,
            broker=self.broker_name,
            strategy_id=strategy_id or "",
            client_order_id=client_order_id,
            event_id=event_id,
        )

    # ------------------------------------------------------------------
    # 재연결 후 누락 체결 보정
    # ------------------------------------------------------------------
    async def reconcile_via_rest(self) -> int:
        """REST 주문체결내역의 총체결수량과 지금까지 전달한 체결수량을 비교해, 차이를 Filled ACK로 보정합니다.

        우리 시스템이 낸 주문(OrderRegistry에 order_no가 있는 주문)만 대상으로 하며, 보정 ACK의
        체결가는 해당 주문의 평균체결가(`avg_idx`)입니다. 반환값은 보정 ACK 건수입니다.
        `RestStreamer` 재연결 직후 자동으로 호출됩니다.
        """
        if self.rest_api is None or not self.account:
            return 0
        try:
            orders = await self.rest_api.inquire_order_history(self.account, order_status="01", raise_on_error=True)
        except Exception as e:
            logger.error(f"KISAckConnector: 체결 보정 조회 실패: {e}")
            return 0

        corrected = 0
        for row in orders or []:
            order_no = str(row.get("odno") or "").strip()
            if not order_no or not self.order_registry.resolve_client_order_id_by_order_no(order_no):
                continue
            try:
                total_filled = int(float(row.get("tot_ccld_qty") or 0))
                avg_price = float(row.get("avg_idx") or 0.0)
            except (TypeError, ValueError):
                continue
            missing = total_filled - self._filled_qty[order_no]
            if missing <= 0:
                continue
            self._dispatch(self._make_ack(
                ack_type=AckType.Filled,
                order_no=order_no,
                original_order_no=str(row.get("orgn_odno") or ""),
                symbol=str(row.get("pdno") or ""),
                buysell=_to_buysell(row.get("sll_buy_dvsn_cd")),
                filled_volume=missing,
                filled_price=avg_price,
                event_id=f"kis-reconcile-{order_no}-{total_filled}",
            ))
            corrected += 1
            logger.warning(f"KISAckConnector: 누락 체결 보정 order_no={order_no} qty={missing} @ {avg_price}")
        return corrected
