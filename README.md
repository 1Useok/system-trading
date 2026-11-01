# System Trading

한국투자증권(KIS) Open API 위에서 동작하는 **이벤트 기반 선물 트레이딩 시스템**입니다.
KOSPI200 선물을 대상으로 시세 수신부터 전략 판단, 주문 송신, 체결 처리, 장마감 청산까지의 경로를 갖추고 있고,
모의투자 계좌에서 실제로 실행됩니다.

- 시세(Market Data)와 내 주문의 체결/응답(Execution Report)을 **물리적으로 분리된 경로**로 처리
- 전략은 브로커를 모름: 주문은 주입된 `OrderService`로만 내고, 브로커 라우팅은 설정이 결정
- 전략 콜백은 **전략별 단일 큐 + 단일 consumer**에서 순차 실행 (ACK 우선, 시세는 backpressure 시 최신값 유지)
- 장마감 전량 청산, 재시작 시 포지션 점검, 주문 거부 시 상태 미변경 등 **운영 안전장치** 내장

---

## 아키텍처

```mermaid
graph TD
    subgraph Market Data
        REST[KIS REST 폴링<br/>FHMIF10000000] --> HUB[MarketDataHub]
        WS[KIS WebSocket<br/>H0IFCNT0 / H0IFASP0] --> KC[KISConnector] --> HUB
    end

    HUB -->|broadcast| EXE[StrategyExecutor<br/>전략별 큐 / 단일 consumer]
    EXE -->|on_quote / on_filled / on_ack / on_clock| STG[Strategy]

    subgraph Execution Command
        STG -->|place_future_order| OS[OrderService<br/>client_order_id 발급]
        OS --> REG[(OrderRegistry<br/>client_order_id ↔ strategy_id ↔ order_no)]
        OS --> OM[OrderManager] --> DRV[KISDriver<br/>REST 주문]
    end

    subgraph Execution Report
        ACKWS[KIS WebSocket<br/>H0IFCNI0 체결통보] --> AC[KISAckConnector]
        AC -->|order_no → client_order_id| REG
        AC --> ROUTER[ExecutionReportRouter]
        ROUTER -->|target dispatch| EXE
    end

    CLK[Clock scheduler<br/>매초 HHMMSS] --> STG
```

| 컴포넌트 | 위치 | 역할 |
| --- | --- | --- |
| `MarketDataHub` | `trading_system/market_data/` | 시세 이벤트(`FuturesSimpleQuote`/`FuturesSimpleFilled`)를 등록된 전략에 broadcast |
| `ExecutionReportRouter` | `trading_system/execution_report/` | 내 주문 ACK(`FuturesSimpleOrderAck`)만 받아 `strategy_id` → `client_order_id` 순으로 대상 전략을 찾고, 못 찾으면 drop (오배달 방지) |
| `StrategyExecutor` | `trading_system/strategy/execution/` | 모든 전략 콜백 실행의 단일 지점. 같은 전략의 이벤트는 순차 처리, ACK는 drop 금지 |
| `OrderService` / `OrderManager` | `trading_system/order/` | 전략에 주입되는 브로커 바인딩 주문 서비스 / 브로커별 Driver 레지스트리 |
| `OrderRegistry` | `trading_system/execution_report/` | `client_order_id ↔ strategy_id`, `order_no ↔ client_order_id` 매핑 (ACK 라우팅의 SSOT) |
| `KISAckConnector` | `trading_system/execution_report/connectors/` | 체결통보(H0IFCNI0, AES-256-CBC 암호화)를 복호화해 표준 ACK로 변환 |
| `StrategyManager` | `trading_system/strategy/` | 구성(composition) 레이어. 위 객체를 만들고 연결하며 전략 생성 시 의존성 주입 |

### 토큰 분리 (PROD / VPS)

KIS의 실시간 지수선물 시세 TR_ID(H0IFCNT0/H0IFASP0)는 **실전계좌 전용**이고, 주문·잔고·체결통보는 모의투자 계좌 기준입니다.
그래서 시세용 토큰(PROD)과 주문용 토큰(VPS)을 분리해 관리합니다. PROD 키가 없으면 REST 폴링만으로 동작합니다.

---

## 샘플 전략

`trading_system/strategy/concepts/`

| 실행 이름 | 클래스 | 로직 |
| --- | --- | --- |
| `minute_breakout` | `MinuteHighBreakoutStrategy` | 최근 N개 완결 1분봉 최고가를 상향 돌파하면 매수, 진입가 대비 손절/익절 후 재진입 |
| `vol_breakout_long` / `vol_breakout_short` | `VolBreakingLong` / `VolBreakingShort` | 기준가 ± k × 전일 고저폭 돌파 시 진입 (체크 시각 분할), 청산 구간 분할 청산, 분봉 종가 기준 손절 |
| `channel_breakout` | `CBSplitLongOnlyPct` | 장 초반(~09:30) 고가 채널 상향 돌파 시 분할 매수, 채널 대비 하락 시 분할 손절 |
| (항상 등록) | `KrxExitAll` | 15:00 이후 계좌의 모든 선물 포지션을 시장가로 청산하는 안전장치 |

---

## 실행

**요구사항:** Python 3.9 ~ 3.11 (pandas 1.x 호환), KIS Open API 모의투자 선물옵션 계좌

```bash
conda create -n trading python=3.11 -y && conda activate trading
pip install -r requirements-dev.txt
cp .env.example .env    # 모의투자 앱 키/시크릿, 계좌번호(12345678-03) 입력
```

```bash
python main.py                                    # N분 고가 돌파, dry-run (주문을 내지 않고 신호만 로깅)
python main.py --strategy vol_breakout_long
python main.py --strategy channel_breakout --send-orders   # 모의투자 계좌로 실제 주문
python main.py -v                                 # 모든 모듈 INFO 로그
```

- 기본은 **dry-run**입니다. `--send-orders`를 줄 때만 주문이 나가며, 이때 장마감 청산(`KrxExitAll`)도 실제로 주문을 냅니다.
- 종목코드는 `trading_system/config/config.yaml`의 `market_data.kis.subscriptions`에 있습니다. KOSPI200 선물은 분기마다 만기가 돌아오므로 롤오버 때 갱신해야 합니다(현재 `A01612`, 2026년 12월물).

```bash
pytest
```

---

## 운영 안전장치와 설계 결정

실제 모의투자 서버에 붙여 보면서 발견하고 반영한 것들입니다.

| 문제 | 대응 |
| --- | --- |
| KIS 초당 요청 제한(`EGW00201`)에 자주 걸림 | 시세 5초 / NAV 60초 폴링, 실패해도 루프를 죽이지 않고 다음 주기 재시도. 연속 호출이 필요한 조회 사이에 간격을 둠 |
| 잔고조회가 성공 코드(`rt_cd=0`)이면서 보유 포지션만 비어 오는 경우가 있음 | 시작 시와 장마감 청산 시 청산가능수량 조회(VTTO5105R)로 교차검증. 두 API가 상충하거나 확인이 안 되면 추측성 주문 없이 전략을 **halted** |
| 보유 포지션 판단 필드 | `tot_psbl_qty`(신규 주문 가능 포함)는 플랫 계좌에서도 0보다 크므로, 반드시 `lqd_psbl_qty1`(청산가능수량)로 판단 |
| 주문 거부를 체결로 착각 | 브로커가 접수(`rt_cd == '0'`)한 경우에만 전략 포지션 상태를 변경 |
| 브로커 체결통보는 우리가 만든 `client_order_id`를 돌려주지 않음 | 주문 응답의 `ODNO`를 `OrderRegistry`에 등록해 두고 체결통보의 주문번호로 원 주문/전략을 역참조 |
| 장마감 청산이 진입 제한 카운터(`bullet`)에 막힘 | 청산·손절은 리스크를 줄이는 동작이므로 `bullet`과 무관하게 항상 실행 |
| ACK와 시세가 같은 틱에 도착하면 하나가 유실 | `StrategyExecutor`가 완료된 이벤트를 모두 처리하도록 수정 (ACK 우선) |
| 재시작 시 기존 포지션의 손절/익절 라인 소실 | 시작 시 잔고의 평균체결단가로 전략 상태를 복원 |

### 알려진 한계

- **체결통보(H0IFCNI0) 경로는 실서버로 충분히 검증되지 않았습니다.** 필드 인덱스는 문서 기반 매핑이며, 현재 샘플 전략들은 체결통보 없이도 동작하도록 "접수 성공 = 체결" 가정을 씁니다.
- 체결통보 WebSocket 재연결 후 누락분 보정(`KISAckConnector.reconcile_via_rest`)은 수동 호출용 스캐폴딩입니다.
- `OrderRegistry`는 메모리 기반이라 재시작 시 매핑이 초기화됩니다(재시작 복구는 잔고 조회로 대신함).
- 시각 비교는 OS 로컬 시간 기준입니다. KST가 아닌 서버에서 돌리려면 타임존을 고정해야 합니다.

---

## 디렉터리

```
main.py                        # 실행 진입점 (전략 선택, 폴링 루프, clock scheduler, 시작 점검)
trading_system/
  api/kis/                     # KIS REST 클라이언트, WebSocket 스트리머, 토큰 관리
  config/                      # config.yaml(TR_ID 엔드포인트, 라우팅, 구독 종목) + .env 로더
  market_data/                 # MarketDataHub, KISConnector, 전일 OHLC 로더
  execution_report/            # ExecutionReportRouter, OrderRegistry, 체결통보 커넥터
  order/                       # OrderService, OrderManager, 브로커 Driver, 라우팅 규칙
  strategy/                    # StrategyManager, StrategyExecutor, 샘플 전략
  book/                        # 주문 장부 (ACK 기록)
  types/                       # 공통 Enum / 이벤트 타입
tests/                         # Executor 순서/backpressure, 전략 로직, 안전장치 회귀 테스트
```
