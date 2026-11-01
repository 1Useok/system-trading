import logging
from collections import defaultdict
from typing import Any, Dict, Iterable, Optional, Tuple

from trading_system.types.enums import AckType, BuySell
from trading_system.types.events import FuturesSimpleOrderAck

logger = logging.getLogger(__name__)


class NewBrokerConnector:
    """ExecutionReport 전용 커넥터.

    - 외부 DMA/주문 프로그램에서 전달되는 `ExecutionReport`(dict)를
      내부 표준 이벤트 `FuturesSimpleOrderAck`로 변환합니다.
    - 변환된 ACK는 **`ExecutionReportRouter.on_ack()`로 전달**되어 `strategy_id`/`client_order_id` 기반으로
      특정 전략에게만 라우팅됩니다.

    드라이런/테스트에서는 네트워크/MQ 구현 없이도 `ingest_*`로 시나리오를 재현할 수 있도록
    “테스트 입력 모드”를 기본 제공합니다.
    """

    def __init__(
        self,
        manager=None,
        broker_name: str = "new_broker",
        strict_seq: bool = True,
    ):
        self.manager = manager
        self.broker_name = broker_name
        # `strict_seq=True`면 seq 역순(out-of-order) 이벤트를 drop 합니다.
        # 운영에서 역순을 "버퍼링 후 정렬"로 처리할 수도 있지만,
        # 현재 단계에서는 정합성 깨짐을 방지하기 위해 보수적으로 drop + 로그를 선택했습니다.
        self.strict_seq = strict_seq

        # idempotency / ordering guard
        # idempotency / ordering guard
        # - event_id: at-least-once 전송에서 중복은 정상일 수 있으므로 1회만 처리
        # - seq: 단조 증가(또는 그에 준하는 정렬키)로 역순/유실을 감지
        self._seen_event_ids = defaultdict(set)  # broker -> set[event_id]
        self._last_seq = {}  # (broker, account) -> last_seq

    def set_manager(self, manager):
        self.manager = manager

    def ingest_execution_report(self, report: Dict[str, Any]) -> Optional[FuturesSimpleOrderAck]:
        """ExecutionReport 1건을 처리합니다.

        중복(event_id) 또는 역순(seq)으로 판단되면 None을 반환하고 dispatch하지 않습니다.
        """
        # 외부 메시지(dict)를 내부 표준 ACK로 변환
        ack, key = self._to_ack(report)

        # 중복 방지 (event_id)
        if ack.event_id:
            if ack.event_id in self._seen_event_ids[ack.broker]:
                logger.warning(
                    "NewBrokerConnector: duplicate event_id dropped (broker=%s, event_id=%s)",
                    ack.broker,
                    ack.event_id,
                )
                return None
            self._seen_event_ids[ack.broker].add(ack.event_id)

        # 정렬/역순 방지 (seq)
        if ack.seq is not None:
            last = self._last_seq.get(key)
            if last is not None and ack.seq < last and self.strict_seq:
                logger.warning(
                    "NewBrokerConnector: out-of-order seq dropped (key=%s, seq=%s, last_seq=%s)",
                    key,
                    ack.seq,
                    last,
                )
                return None
            # last_seq 갱신은 drop을 통과한 뒤에만
            if last is None or ack.seq > last:
                self._last_seq[key] = ack.seq

        # 최종 dispatch (ExecutionReportRouter → Strategy)
        if self.manager:
            self.manager.on_ack(ack)
        return ack

    def ingest_many(self, reports: Iterable[Dict[str, Any]]) -> Tuple[int, int]:
        """여러 건을 순서대로 ingest.

        Returns:
            (processed, dropped)
        """
        processed = 0
        dropped = 0
        for r in reports:
            if self.ingest_execution_report(r) is None:
                dropped += 1
            else:
                processed += 1
        return processed, dropped

    @staticmethod
    def _normalize_ack_type(value: Any) -> AckType:
        if isinstance(value, AckType):
            return value
        if value is None:
            return AckType.Nothing
        if isinstance(value, int):
            try:
                return AckType(value)
            except Exception:
                return AckType.Nothing

        if isinstance(value, str):
            v = value.strip().upper()
            mapping = {
                "RECEIVED": AckType.Received,
                "REJECTED": AckType.Rejected,
                "CANCELED": AckType.Canceled,
                "CANCELLED": AckType.Canceled,
                "REPLACED": AckType.Replaced,
                "FILL": AckType.Filled,
                "FILLED": AckType.Filled,
                "PARTIAL_FILL": AckType.Filled,
                "PARTIALLY_FILLED": AckType.Filled,
                "NOTHING": AckType.Nothing,
            }
            if v in mapping:
                return mapping[v]

            # AckType Enum member name
            try:
                return AckType[value]
            except Exception:
                return AckType.Nothing

        return AckType.Nothing

    @staticmethod
    def _normalize_buysell(value: Any) -> BuySell:
        if isinstance(value, BuySell):
            return value
        if value is None:
            return BuySell.NA
        if isinstance(value, str):
            v = value.strip().upper()
            if v in ("02", "BUY"):
                return BuySell.BUY
            if v in ("01", "SELL"):
                return BuySell.SELL
            return BuySell.NA
        return BuySell.NA

    def _to_ack(self, report: Dict[str, Any]) -> Tuple[FuturesSimpleOrderAck, Tuple[str, str]]:
        """ExecutionReport(dict) -> FuturesSimpleOrderAck 변환.

        Returns:
            (ack, stream_key)
        """
        # 외부 이벤트에 broker가 없으면 connector 기본 broker_name을 사용
        broker = (report.get("broker") or report.get("broker_name") or self.broker_name or "").strip()
        account = report.get("account") or report.get("account_id") or ""

        # 외부 ExecutionReport는 보통 status 문자열로 들어오며, 내부 AckType으로 매핑합니다.
        status = report.get("status")
        ack_type = self._normalize_ack_type(status)

        # 체결 증분(last_fill_*)을 filled_*로 사용
        filled_price = report.get("last_fill_price", report.get("filled_price", -1))
        filled_volume = report.get("last_fill_qty", report.get("filled_volume", 0))

        ack = FuturesSimpleOrderAck(
            ack_type=ack_type,
            account=account,
            inst_code=report.get("symbol", report.get("inst_code", "")),
            order_num=report.get("order_id", report.get("order_num", "")),
            original_order_num=report.get("original_order_id", report.get("original_order_num", "")),
            cancel_or_replace_volume=report.get("cancel_or_replace_volume", report.get("cancel_qty", 0)),
            filled_price=filled_price,
            filled_volume=filled_volume,
            buysell=self._normalize_buysell(report.get("side", report.get("buysell"))),
            broker=broker,
            strategy_id=report.get("strategy_id", report.get("strategy_name", "")) or "",
            client_order_id=report.get("client_order_id", ""),
            timestamp=report.get("timestamp"),
            event_id=report.get("event_id", report.get("exec_id", "")) or "",
            seq=report.get("seq"),
        )

        # ordering stream 단위는 (broker, account) 기준
        # - 계좌별로 독립 시퀀스를 제공하는 시스템이 많고,
        # - 최소한의 안전장치로 계좌 단위 역순/중복을 방어하기 위함입니다.
        return ack, (ack.broker, ack.account)
