"""
주문(Execution Command) 패키지

- `OrderService`: 전략에 주입되는 브로커 바인딩 주문 서비스 (client_order_id 발급, OrderRegistry 등록)
- `OrderManager`: 브로커 키(`kis`/`new_broker`)별 Driver 레지스트리
- `ExecutionRouting`: 전략명/종목 기반 브로커 라우팅 규칙 (config에서 결정)
- `brokers/`: 브로커별 Driver 구현 (KIS REST, 외부 주문 프로그램 연동용 dummy)
"""
