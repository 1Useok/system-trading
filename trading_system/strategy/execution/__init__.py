"""Strategy execution runtime.

핵심 원칙: 전략 콜백(`on_quote`/`on_filled`/`on_ack`)의 실행을
단 하나의 `asyncio` 루프 위에서, 단 하나의 실행기(`StrategyExecutor`)로만 수행하는 것입니다.
"""

from .strategy_executor import StrategyExecutor

__all__ = ["StrategyExecutor"]
