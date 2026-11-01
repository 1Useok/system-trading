from __future__ import annotations

from typing import Any, Dict, Optional


def resolve_broker(
    execution_config: Optional[Dict[str, Any]],
    *,
    strategy_name: str,
    symbol: Optional[str] = None,
) -> str:
    """설정 기반으로 주문 실행 브로커(`kis`/`new_broker` 등)를 결정합니다.

    이 함수는 "전략이 브로커를 모른다"는 목표를 지키기 위한 핵심 SSOT입니다.
    - 전략 코드에서 `broker=...`를 넘기는 방식은 금지/지양
    - 구성(주입) 레이어(`StrategyManager`)에서만 브로커를 결정

    우선순위(높음 → 낮음):
      1) symbol_overrides[symbol]
      2) strategy_overrides[strategy_name]
      3) default_broker

    설정이 없으면 기본값은 `kis`.
    """
    cfg = execution_config or {}
    default_broker = (cfg.get("default_broker") or "kis").strip()

    symbol_overrides = cfg.get("symbol_overrides") or {}
    if symbol and symbol in symbol_overrides and symbol_overrides[symbol]:
        return str(symbol_overrides[symbol]).strip()

    strategy_overrides = cfg.get("strategy_overrides") or {}
    if strategy_name in strategy_overrides and strategy_overrides[strategy_name]:
        return str(strategy_overrides[strategy_name]).strip()

    return default_broker
