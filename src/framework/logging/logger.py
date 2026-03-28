from __future__ import annotations

import logging
import time
from contextvars import ContextVar
from io import StringIO

import allure


_CURRENT_TEST_INFO: ContextVar[dict[str, object]] = ContextVar(
    "current_test_info",
    default={"test_name": None, "status": None, "start_time": None},
)


class TestInfoLoggingFilter(logging.Filter):
    """Attach current pytest test metadata to each log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        test_info = _CURRENT_TEST_INFO.get({})
        record.test_name = test_info.get("test_name") or "-"
        record.test_status = test_info.get("status") or "-"
        start_time = test_info.get("start_time")
        record.test_run_time = None if start_time is None else time.time() - float(start_time)
        return True


class MaxSizeFilter(logging.Filter):
    """Truncate large log messages so console and Allure output stay readable."""

    def __init__(self, max_size: int = 10000) -> None:
        super().__init__()
        self.max_size = max_size

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if len(message) > self.max_size:
            record.msg = f"{message[: self.max_size]} [TRUNCATED]"
            record.args = ()
        return True


class AllureLogHandler(logging.Handler):
    """Buffer per-test logs and attach them to Allure when the test finishes."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.name = "AllureHandler"
        self.buffer = StringIO()

    def emit(self, record: logging.LogRecord) -> None:
        self.buffer.write(self.format(record) + "\n")

    def flush(self) -> None:
        if self.buffer.tell() <= 0:
            return
        self.buffer.seek(0)
        allure.attach(
            self.buffer.getvalue(),
            name="Test Logs",
            attachment_type=allure.attachment_type.TEXT,
        )
        self.clear()

    def clear(self) -> None:
        self.buffer = StringIO()


def _formatter() -> logging.Formatter:
    return logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s [test=%(test_name)s status=%(test_status)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def setup_test_logging() -> None:
    """Configure root logging once for console and Allure-backed test reporting."""
    root_logger = logging.getLogger()
    if getattr(root_logger, "_trip_search_logging_configured", False):
        return

    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    filters: list[logging.Filter] = [TestInfoLoggingFilter(), MaxSizeFilter()]

    stream_handler = logging.StreamHandler()
    stream_handler.name = "StreamHandler"
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(_formatter())
    for handler_filter in filters:
        stream_handler.addFilter(handler_filter)

    allure_handler = AllureLogHandler()
    allure_handler.setFormatter(_formatter())
    for handler_filter in filters:
        allure_handler.addFilter(handler_filter)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(allure_handler)
    root_logger._trip_search_logging_configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a named logger bound to the shared test logging setup."""
    setup_test_logging()
    return logging.getLogger(name)


def set_current_test_info(*, test_name: str, status: str, start_time: float | None = None) -> None:
    """Set the current test metadata used by log filters."""
    current = dict(_CURRENT_TEST_INFO.get({}))
    current.update(
        {
            "test_name": test_name,
            "status": status,
            "start_time": time.time() if start_time is None else start_time,
        }
    )
    _CURRENT_TEST_INFO.set(current)


def update_current_test_status(status: str) -> None:
    """Update only the current test status."""
    current = dict(_CURRENT_TEST_INFO.get({}))
    current["status"] = status
    _CURRENT_TEST_INFO.set(current)


def clear_current_test_info() -> None:
    """Reset the current test metadata after teardown."""
    _CURRENT_TEST_INFO.set({"test_name": None, "status": None, "start_time": None})


def flush_allure_test_logs() -> None:
    """Flush buffered logs to the Allure attachment handler."""
    for handler in logging.getLogger().handlers:
        if getattr(handler, "name", "") == "AllureHandler" and hasattr(handler, "flush"):
            handler.flush()
