from __future__ import annotations

import inspect
from pathlib import Path

from assertpy import assert_that as _assert_that
from assertpy import soft_assertions

from src.framework.logging.logger import get_logger

LOGGER = get_logger("tests.assertions")


def assert_that(value, description: str | None = None):
    """Wrap assertpy assertions with a default description for reporting clarity."""
    if description is None:
        caller = inspect.currentframe().f_back
        location = Path(caller.f_code.co_filename).name
        description = f"Assertion at {location}:{caller.f_lineno}"
    LOGGER.info("ASSERT: %s", description)
    return _assert_that(value).described_as(description)
