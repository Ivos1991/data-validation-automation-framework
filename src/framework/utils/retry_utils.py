from __future__ import annotations

import operator
import time
from typing import Callable, TypeVar

from src.framework.logging.logger import get_logger

LOGGER = get_logger("trip_search.retry_utils")

T = TypeVar("T")


def retry_for_relation_to_expected_value(
    func: Callable[[], T],
    expected_value: T,
    delay_time_in_sec: float,
    max_attempts: int,
    relate: Callable[[T, T], bool],
) -> T:
    """Retry a callable until its return value no longer relates to the expected value."""
    value = func()
    for attempt in range(1, max_attempts + 1):
        LOGGER.info("Retry attempt=%s/%s function=%s", attempt, max_attempts, getattr(func, "__name__", "callable"))
        if not relate(value, expected_value):
            return value
        if attempt < max_attempts:
            time.sleep(delay_time_in_sec)
            value = func()
    return value


def retry_for_true(func: Callable[[], T], delay_time_in_sec: float, max_attempts: int) -> T:
    """Retry a callable until it returns a truthy value."""
    return retry_for_relation_to_expected_value(
        func=func,
        expected_value=True,
        delay_time_in_sec=delay_time_in_sec,
        max_attempts=max_attempts,
        relate=operator.ne,
    )
