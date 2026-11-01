"""Kill Switch: 신규 주문 차단 상태.

발동되면 `OrderService.place_future_order()`가 리스크를 줄이는 주문(청산/손절, `risk_reducing=True`)을
제외한 모든 주문을 브로커로 보내지 않고 거부 응답을 돌려준다. 미체결 취소와 포지션 청산은
`RiskManager.trigger()`가 이어서 수행한다. 한 번 발동되면 프로세스 안에서는 해제하지 않는다
(원인을 확인한 사람이 재시작해서 푸는 것을 전제로 한다).
"""
import logging
from typing import Optional

from trading_system.utils.clock import now_kst

logger = logging.getLogger(__name__)

REJECT_CODE = "KILL"


class KillSwitch:
    def __init__(self) -> None:
        self._active = False
        self.reason: str = ""
        self.activated_at = None

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

    def rejection(self) -> Optional[dict]:
        """발동 상태면 주문 거부 응답(rt_cd != '0')을, 아니면 None을 반환합니다."""
        if not self._active:
            return None
        return {"rt_cd": REJECT_CODE, "msg1": f"kill switch active: {self.reason}"}
