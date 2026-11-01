"""거래소 시각(KST) 기준 시계.

장 시간 판단, 주문 시각 분할, 토큰 만료 비교는 모두 한국 시각 기준이어야 한다. OS 로컬 시간
(`datetime.now()`)을 쓰면 UTC로 설정된 서버/컨테이너에서 9시간 어긋나므로 여기서 고정한다.
KST는 서머타임이 없어 고정 오프셋(UTC+9)으로 정확히 표현된다.
"""
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9), "KST")


def now_kst() -> datetime:
    """현재 KST 시각(naive). KIS가 내려주는 시각 문자열(KST, tz 없음)과 바로 비교할 수 있다."""
    return datetime.now(KST).replace(tzinfo=None)
