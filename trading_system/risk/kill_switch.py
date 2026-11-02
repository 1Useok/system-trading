"""Kill Switch: 신규 주문 차단 상태.

발동되면 `OrderService.place_future_order()`가 리스크를 줄이는 주문(청산/손절, `risk_reducing=True`)을
제외한 모든 주문을 브로커로 보내지 않고 거부 응답을 돌려준다. 미체결 취소와 포지션 청산은
`RiskManager.trigger()`가 이어서 수행한다. 한 번 발동되면 프로세스 안에서는 해제하지 않는다
(원인을 확인한 사람이 재시작해서 푸는 것을 전제로 한다).

전략 하나만 신규 진입을 막을 수도 있다(`block_entries`). 재시작했는데 그 전략이 복원할 수 없는
포지션이 이미 있을 때처럼, 계좌 전체가 아니라 특정 전략만 멈춰야 하는 경우에 쓴다.
"""
import logging
from typing import Dict, Optional

from trading_system.utils.clock import now_kst

logger = logging.getLogger(__name__)

REJECT_CODE = "KILL"


class KillSwitch:
    def __init__(self) -> None:
        self._active = False
        self.reason: str = ""
        self.activated_at = None
        self._blocked_strategies: Dict[str, str] = {}

    @property
    def active(self) -> bool:
        return self._active

    def activate(self, reason: str) -> bool:
        """발동합니다. 이번 호출로 새로 발동됐으면 True, 이미 발동 상태였으면 False."""
        if self._active:
            return False
        self._active = True
        self.reason = reason
        self.activated_at = now_kst()
        logger.critical(f"KILL SWITCH ACTIVATED: {reason}")
        return True

    def block_entries(self, strategy_id: str, reason: str) -> None:
        """이 전략의 신규 진입만 막습니다(청산/손절은 통과). 해제는 재시작으로 합니다."""
        self._blocked_strategies[strategy_id] = reason
        logger.critical(f"ENTRIES BLOCKED for {strategy_id}: {reason}")

    def is_blocked(self, strategy_id: str = "") -> bool:
        return self._active or strategy_id in self._blocked_strategies

    def rejection(self, strategy_id: str = "") -> Optional[dict]:
        """발동 상태(또는 이 전략이 차단된 상태)면 주문 거부 응답(rt_cd != '0')을, 아니면 None을 반환합니다."""
        if self._active:
            return {"rt_cd": REJECT_CODE, "msg1": f"kill switch active: {self.reason}"}
        if strategy_id in self._blocked_strategies:
            return {"rt_cd": REJECT_CODE, "msg1": f"entries blocked: {self._blocked_strategies[strategy_id]}"}
        return None
