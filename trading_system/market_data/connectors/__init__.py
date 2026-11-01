"""Market data connectors.

Market data connector는 외부에서 들어오는 **시장 데이터(quote/tick)** 를
`FuturesSimpleQuote`/`FuturesSimpleFilled`로 변환해 `MarketDataHub`로 전달합니다.

ExecutionReport(ACK)는 이 패키지로 들어오면 안 됩니다.
"""
