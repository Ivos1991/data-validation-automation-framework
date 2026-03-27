from dataclasses import replace
from pathlib import Path

import pytest

from src.framework.execution.trip_search_entrypoint import TripSearchEntrypoint


@pytest.fixture()
def local_entrypoint_test_dir() -> Path:
    test_dir = Path(__file__).resolve().parent / ".local_test_data"
    test_dir.mkdir(exist_ok=True)
    return test_dir


@pytest.fixture()
def entrypoint_config(config, local_entrypoint_test_dir: Path):
    return replace(
        config,
        report_export_dir=local_entrypoint_test_dir / "exports",
    )


@pytest.fixture()
def trip_search_entrypoint(entrypoint_config) -> TripSearchEntrypoint:
    return TripSearchEntrypoint(config=entrypoint_config)
