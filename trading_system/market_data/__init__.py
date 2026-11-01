"""Market data package.

이 패키지는 **시장 데이터(Market Data)** 전용 파이프라인만 포함합니다.

- 이벤트: `FuturesSimpleQuote`, `FuturesSimpleFilled`
- 전달: 전략/피처에 broadcast

의도:
- ExecutionReport(내 주문 ACK)는 여기로 들어오면 안 됩니다.
  (혼용 방지를 위해 별도 패키지 `trading_system.execution_report`로 분리)
"""
