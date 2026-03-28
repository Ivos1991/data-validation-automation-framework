from pathlib import Path

import pytest

from src.framework.connectors.db.execution_job_queries import ExecutionJobQueries
from src.framework.execution.trip_search_async_execution import TripSearchAsyncBatchExecutor
from src.framework.connectors.files.scenario_loader import TripSearchScenarioLoader


@pytest.fixture()
def async_batch_executor(config) -> TripSearchAsyncBatchExecutor:
    return TripSearchAsyncBatchExecutor(config.numeric_tolerance)


@pytest.fixture()
def async_job_queries(seeded_trip_db):
    return ExecutionJobQueries(seeded_trip_db)


@pytest.fixture()
def async_scenario_loader(dataset_loader) -> TripSearchScenarioLoader:
    return TripSearchScenarioLoader(dataset_loader)


@pytest.fixture()
def async_batch_scenarios(loaded_trip_dataset, async_scenario_loader: TripSearchScenarioLoader):
    return async_scenario_loader.load_csv(loaded_trip_dataset.scenario_dataset_path).scenarios


@pytest.fixture()
def local_async_test_dir() -> Path:
    test_dir = Path(__file__).resolve().parent / ".local_test_data"
    test_dir.mkdir(exist_ok=True)
    return test_dir
