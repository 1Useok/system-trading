"""Execution report connectors.

Execution report connector는 외부에서 들어오는 `ExecutionReport`를
내부 표준 이벤트 `FuturesSimpleOrderAck`로 변환해 `ExecutionReportRouter`로 전달합니다.

시장 데이터(quote/tick)는 이 패키지로 들어오면 안 됩니다.
"""
