from __future__ import annotations

import operator
import time
from functools import wraps
from typing import Callable, TypeVar

from src.framework.logging.logger import get_logger

LOGGER = get_logger("trip_search.retry_utils")

T = TypeVar("T")


def retry(max_attempts: int, delay_seconds: float):
    """Retry a callable until the expected value appears in the returned result."""

    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if "expected_value" not in kwargs:
                raise ValueError("expected_value argument was not found, you must pass it from the wrapped function")

            expected_value = kwargs.get("expected_value")
            result = None
            for attempt in range(1, max_attempts + 1):
                result = func(*args, **kwargs)
                LOGGER.info("Retry attempt=%s/%s function=%s", attempt, max_attempts, func.__name__)
                if result and expected_value in result:
                    return result
                if attempt < max_attempts:
                    time.sleep(delay_seconds)
            return result

        return wrapper

    return decorator_retry


def retry_for_true(max_attempts: int, delay_seconds: float):
    """Retry a callable until it returns a truthy value."""

    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = None
            for attempt in range(1, max_attempts + 1):
                result = func(*args, **kwargs)
                LOGGER.info("Retry attempt=%s/%s function=%s", attempt, max_attempts, func.__name__)
                if result:
                    return result
                if attempt < max_attempts:
                    time.sleep(delay_seconds)
            return result

        return wrapper

    return decorator_retry


def retry_while_assertion_error(max_attempts: int, delay_seconds: float):
    """Retry a callable while it raises an assertion error."""

    def decorator_retry(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    LOGGER.info("Retry attempt=%s/%s function=%s", attempt, max_attempts, func.__name__)
                    return func(*args, **kwargs)
                except AssertionError:
                    if attempt < max_attempts:
                        time.sleep(delay_seconds)
            return None

        return wrapper

    return decorator_retry


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


def retry_for_response_code(
    func: Callable[[], T],
    response_code: int,
    delay_time_in_sec: float,
    max_attempts: int,
) -> T:
    """Retry a response-producing callable until its status code matches the expected code."""
    return retry_for_relation_to_expected_value(
        func=func,
        expected_value=response_code,
        delay_time_in_sec=delay_time_in_sec,
        max_attempts=max_attempts,
        relate=lambda response, expected: operator.ne(response.status_code, expected),
    )


def retry_for_status_code(
    func: Callable[[], T],
    response_code: int,
    delay_time_in_sec: float,
    max_attempts: int,
) -> T:
    """Retry a response-producing callable until its status code matches the expected code."""
    return retry_for_response_code(
        func=func,
        response_code=response_code,
        delay_time_in_sec=delay_time_in_sec,
        max_attempts=max_attempts,
    )


def retry_for_response_body(
    func: Callable[[], T],
    response_json: T,
    delay_time_in_sec: float,
    max_attempts: int,
) -> T:
    """Retry a response-producing callable until its payload matches the expected body."""
    return retry_for_relation_to_expected_value(
        func=func,
        expected_value=response_json,
        delay_time_in_sec=delay_time_in_sec,
        max_attempts=max_attempts,
        relate=operator.ne,
    )
