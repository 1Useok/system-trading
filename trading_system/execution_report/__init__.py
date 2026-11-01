"""Execution report package.

이 패키지는 **내 주문 이벤트(ExecutionReport/ACK)** 전용 파이프라인만 포함합니다.

- 이벤트: `FuturesSimpleOrderAck`
- 전달: 기본은 `strategy_id` 기반 target dispatch
- idempotency/ordering guard(event_id/seq)는 connector/router 레벨에서 수행
"""
