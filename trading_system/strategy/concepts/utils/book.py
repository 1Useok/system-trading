# -*- coding: utf-8 -*-
"""
Order book management for split-order strategies.
"""
import datetime
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum
from trading_system.utils.clock import now_kst


class BuySell(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass
class OrderInfo:
    """Order information record."""
    inst_code: str
    order_num: Optional[str]
    buysell: BuySell
    price: str
    volume: int
    timestamp: datetime.datetime = field(default_factory=now_kst)
    status: OrderStatus = OrderStatus.PENDING
    filled_volume: int = 0
    filled_price: float = 0.0


@dataclass
class TradeRecord:
    """Trade execution record for logging."""
    inst_code: str
    buysell: str
    price: float
    volume: int
    timestamp: datetime.datetime
    strategy_name: str
    is_closing_exit: bool = False  # True if 15:45+ closing price exit


class Book:
    """
    Order book manager for tracking orders and trades.

    Manages both real orders and runtest (backtest) orders.
    """

    def __init__(self, strategy_name: str, inst_codes: List[str]):
        """
        Initialize Book.

        Args:
            strategy_name: Name of the strategy
            inst_codes: List of instrument codes
        """
        self.strategy_name = strategy_name
        self.inst_codes = inst_codes

        # Real order tracking
        self.order_book_dict: Dict[str, List[OrderInfo]] = {
            code: [] for code in inst_codes
        }

        # Runtest order tracking
        self.runtest_order_list: List[OrderInfo] = []

        # Trade records
        self.trade_records: List[TradeRecord] = []

    def register_order_info(self, inst_code: str, order_num: Optional[str],
                           buysell: BuySell, price: str, volume: int) -> None:
        """
        Register a new order.

        Args:
            inst_code: Instrument code
            order_num: Order number from broker
            buysell: Buy or sell
            price: Order price
            volume: Order volume
        """
        order_info = OrderInfo(
            inst_code=inst_code,
            order_num=order_num,
            buysell=buysell,
            price=price,
            volume=volume
        )

        if inst_code not in self.order_book_dict:
            self.order_book_dict[inst_code] = []

        self.order_book_dict[inst_code].append(order_info)

        logging.info(f"[{self.strategy_name}] Order registered: {buysell.value} "
                    f"{volume}@{price} ({inst_code})")

    def register_runtest_order_info(self, inst_code: str, buysell: BuySell,
                                    price: str, volume: int) -> None:
        """
        Register a runtest (backtest) order.

        Args:
            inst_code: Instrument code
            buysell: Buy or sell
            price: Order price
            volume: Order volume
        """
        order_info = OrderInfo(
            inst_code=inst_code,
            order_num=None,
            buysell=buysell,
            price=price,
            volume=volume,
            status=OrderStatus.FILLED
        )
        self.runtest_order_list.append(order_info)

        logging.info(f"[{self.strategy_name}] Runtest order: {buysell.value} "
                    f"{volume}@{price} ({inst_code})")

    def register_closing_exit(self, inst_code: str, buysell: BuySell,
                             closing_price: float, volume: int) -> None:
        """
        Register a closing exit (15:45+ market close).

        This records the trade but does not actually place an order.

        Args:
            inst_code: Instrument code
            buysell: Buy or sell (exit direction)
            closing_price: Last traded price at 15:45
            volume: Position volume to close
        """
        record = TradeRecord(
            inst_code=inst_code,
            buysell=buysell.value,
            price=closing_price,
            volume=volume,
            timestamp=now_kst(),
            strategy_name=self.strategy_name,
            is_closing_exit=True
        )
        self.trade_records.append(record)

        logging.info(f"[{self.strategy_name}] Closing exit recorded: {buysell.value} "
                    f"{volume}@{closing_price} ({inst_code}) [CLOSING PRICE]")

    def process_ack(self, ack) -> None:
        """
        Process acknowledgment from broker.

        Args:
            ack: Acknowledgment object from broker
        """
        if ack.inst_code not in self.order_book_dict:
            return

        for order_info in self.order_book_dict[ack.inst_code]:
            if order_info.order_num == ack.order_num:
                # Update order status based on ack type
                if hasattr(ack, 'ack_type'):
                    if ack.ack_type.name == 'Filled':
                        order_info.filled_volume += int(ack.filled_volume)
                        order_info.filled_price = float(ack.filled_price)
                        if order_info.filled_volume >= order_info.volume:
                            order_info.status = OrderStatus.FILLED
                    elif ack.ack_type.name == 'Cancelled':
                        order_info.status = OrderStatus.CANCELLED
                    elif ack.ack_type.name == 'Rejected':
                        order_info.status = OrderStatus.REJECTED

    def get_open_orders(self, inst_code: str) -> List[OrderInfo]:
        """Get list of open (pending) orders for an instrument."""
        if inst_code not in self.order_book_dict:
            return []
        return [o for o in self.order_book_dict[inst_code]
                if o.status == OrderStatus.PENDING]

    def get_filled_orders(self, inst_code: str) -> List[OrderInfo]:
        """Get list of filled orders for an instrument."""
        if inst_code not in self.order_book_dict:
            return []
        return [o for o in self.order_book_dict[inst_code]
                if o.status == OrderStatus.FILLED]
