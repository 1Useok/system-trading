"""
샘플 전략 패키지

- MinuteHighBreakoutStrategy: N분 최고가 상향 돌파 진입 + 손절/익절
- VolBreakingLong / VolBreakingShort: 전일 고저폭 기반 변동성 돌파 (분할 진입/청산)
- CBSplitLongOnlyPct: 장 초반 고가 채널 돌파 롱 + 분할 손절
- KrxExitAll: 지정 시각 보유 포지션 전량 청산 (안전장치)
"""

from .minute_high_breakout import MinuteHighBreakoutStrategy
from .vol_breaking_long import VolBreakingLong
from .vol_breaking_short import VolBreakingShort
from .cb_split_long_only_pct import CBSplitLongOnlyPct
from .krx_exit_all import KrxExitAll

__all__ = [
    "MinuteHighBreakoutStrategy",
    "VolBreakingLong",
    "VolBreakingShort",
    "CBSplitLongOnlyPct",
    "KrxExitAll",
]
