# -*- coding: utf-8 -*-
"""
Time utility functions for split-order strategies.
"""
import math
from typing import List


def time_to_seconds(time_int: int) -> int:
    """
    Convert HHMMSS integer format to total seconds.

    Args:
        time_int: Time in HHMMSS format (e.g., 154500 for 15:45:00)

    Returns:
        Total seconds from midnight
    """
    hours = time_int // 10000
    minutes = (time_int % 10000) // 100
    seconds = time_int % 100
    return hours * 3600 + minutes * 60 + seconds


def seconds_to_time(total_seconds: int) -> float:
    """
    Convert total seconds to HHMMSS.ffffff format.

    Args:
        total_seconds: Total seconds from midnight

    Returns:
        Time in HHMMSS format as float
    """
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return hours * 10000 + minutes * 100 + seconds


def calculate_time_list(start_time: int, end_time: int, num_divide: int) -> List[float]:
    """
    Calculate evenly distributed time points between start and end.

    Args:
        start_time: Start time in HHMMSS format
        end_time: End time in HHMMSS format
        num_divide: Number of divisions

    Returns:
        List of time points in HHMMSS format
    """
    if num_divide <= 0:
        return []

    start_seconds = time_to_seconds(start_time) * 1000  # milliseconds
    end_seconds = time_to_seconds(end_time) * 1000

    time_gap = math.floor((end_seconds - start_seconds) / num_divide)

    time_list = []
    for i in range(num_divide):
        time_ms = start_seconds + i * time_gap
        time_seconds = time_ms / 1000

        hours = int(time_seconds // 3600)
        minutes = int((time_seconds % 3600) // 60)
        seconds = time_seconds % 60

        time_format = hours * 10000 + minutes * 100 + seconds
        time_list.append(time_format)

    return time_list


def is_after_market_close(exit_order_start: int) -> bool:
    """
    Check if exit order start time is at or after 15:45:00.

    Args:
        exit_order_start: Exit order start time in HHMMSS format

    Returns:
        True if exit should be recorded only (no actual order)
    """
    return exit_order_start >= 154500
