"""브로커 잔고 응답에서 (종목, 순포지션) 목록을 추출."""
from typing import Iterable, List, Tuple


def extract_positions(balance_result) -> List[Tuple[str, int]]:
    """브로커별 잔고 응답에서 (symbol, net_qty) 목록을 추출합니다. 매도(숏) 포지션은 음수.

    KIS(RestApi) 기준:
    - `get_future_account_balance()` -> (DataFrame, summary)
    - DataFrame row: `shtn_pdno`(단축코드), `sll_buy_dvsn_name`(매도/매수), `cblc_qty`(잔고수량)
    """
    if balance_result is None:
        return []

    if isinstance(balance_result, tuple) and balance_result:
        df = balance_result[0]
    else:
        df = balance_result

    positions: List[Tuple[str, int]] = []

    if hasattr(df, "iterrows"):
        for _, row in df.iterrows():
            symbol = str(row.get("shtn_pdno") or row.get("symbol") or row.get("inst_code") or "").strip()
            if not symbol:
                continue
            side_name = str(row.get("sll_buy_dvsn_name") or row.get("side") or "").strip()
            qty_raw = row.get("cblc_qty", row.get("net_qty", 0))
            try:
                qty = int(float(qty_raw))
            except Exception:
                qty = 0
            qty = -abs(qty) if side_name in ("SLL", "SELL", "매도") else abs(qty)
            if qty != 0:
                positions.append((symbol, qty))
        return positions

    if isinstance(df, dict):
        symbol = str(df.get("symbol") or df.get("inst_code") or "").strip()
        qty = int(df.get("net_qty", 0))
        return [(symbol, qty)] if symbol and qty else []

    if isinstance(df, list):
        for item in df:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or item.get("inst_code") or item.get("shtn_pdno") or "").strip()
            qty = int(item.get("net_qty", item.get("cblc_qty", 0)) or 0)
            if symbol and qty:
                positions.append((symbol, qty))
    return positions
