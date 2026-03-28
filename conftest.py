from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from src.framework.config.config_manager import ConfigManager
from src.framework.connectors.db.sqlite_client import SQLiteClient
from src.framework.connectors.db.trip_queries import TripQueries
from src.framework.connectors.files.dataset_loader import DatasetLoader
from src.framework.connectors.files.synthetic_trip_dataset_builder import SyntheticTripDatasetBuilder
from src.framework.connectors.files.trip_dataset_context_loader import LoadedTripDatasetContext, TripDatasetContextLoader
from src.framework.logging.logger import (
    clear_current_test_info,
    flush_allure_test_logs,
    get_logger,
    set_current_test_info,
    setup_test_logging,
    update_current_test_status,
)
from src.framework.utils.dataframe_utils import build_expected_trip_frame

TEST_LOGGER = get_logger("tests.runtime")


def pytest_configure() -> None:
    """Configure shared logging once before test collection starts."""
    setup_test_logging()


def pytest_runtest_setup(item) -> None:
    """Log test start and initialize per-test logging context."""
    set_current_test_info(test_name=item.nodeid, status="running")
    TEST_LOGGER.info("START TEST")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Track live test status in the logging context."""
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        update_current_test_status("passed" if report.passed else "failed")
    elif report.when in {"setup", "teardown"} and report.failed:
        update_current_test_status("failed")


def pytest_runtest_teardown(item) -> None:
    """Log test end and flush the per-test Allure log attachment."""
    TEST_LOGGER.info("END TEST")
    flush_allure_test_logs()
    clear_current_test_info()


@pytest.fixture(scope="session")
def config() -> ConfigManager:
    return ConfigManager.from_env()


@pytest.fixture(scope="session")
def dataset_loader() -> DatasetLoader:
    return DatasetLoader()


@pytest.fixture(scope="session")
def synthetic_trip_dataset_builder() -> SyntheticTripDatasetBuilder:
    return SyntheticTripDatasetBuilder()


@pytest.fixture(scope="session")
def trip_dataset_context_loader(
    dataset_loader: DatasetLoader,
    synthetic_trip_dataset_builder: SyntheticTripDatasetBuilder,
) -> TripDatasetContextLoader:
    return TripDatasetContextLoader(dataset_loader=dataset_loader, synthetic_trip_dataset_builder=synthetic_trip_dataset_builder)


@pytest.fixture(scope="session")
def loaded_trip_dataset(config: ConfigManager, trip_dataset_context_loader: TripDatasetContextLoader) -> LoadedTripDatasetContext:
    return trip_dataset_context_loader.load(config)


@pytest.fixture(scope="session")
def source_dataset_path(config: ConfigManager, loaded_trip_dataset: LoadedTripDatasetContext) -> Path:
    if loaded_trip_dataset.dataset_profile == "small":
        return config.dataset_path
    return Path(f"<{loaded_trip_dataset.trip_dataset_source}>")


@pytest.fixture(scope="session")
def raw_trip_frame(loaded_trip_dataset: LoadedTripDatasetContext) -> pd.DataFrame:
    return loaded_trip_dataset.raw_trip_frame


@pytest.fixture(scope="session")
def normalized_trips(loaded_trip_dataset: LoadedTripDatasetContext) -> list:
    return loaded_trip_dataset.normalized_trips


@pytest.fixture()
def sqlite_client() -> SQLiteClient:
    db_dir = Path(__file__).resolve().parent / ".local_test_data"
    db_dir.mkdir(exist_ok=True)
    db_path = db_dir / f"trip_validation_{uuid4().hex}.sqlite"
    client = SQLiteClient(db_path)
    client.initialize_schema()
    try:
        yield client
    finally:
        client.close()
        if db_path.exists():
            db_path.unlink()


@pytest.fixture()
def seeded_trip_db(sqlite_client: SQLiteClient, normalized_trips: list) -> SQLiteClient:
    TripQueries(sqlite_client).seed_trips(normalized_trips)
    return sqlite_client


@pytest.fixture(scope="session")
def expected_trip_frame(raw_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_expected_trip_frame(raw_trip_frame)
