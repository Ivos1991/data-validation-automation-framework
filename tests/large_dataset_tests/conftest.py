from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.framework.connectors.db.sqlite_client import SQLiteClient
from src.framework.connectors.db.trip_queries import TripQueries
from src.framework.connectors.files.run_profile_loader import TripSearchRunProfileLoader
from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader
from src.framework.connectors.files.scenario_loader import LoadedTripSearchScenarioDataset, TripSearchScenarioLoader
from src.framework.connectors.files.synthetic_trip_dataset_builder import SyntheticTripDatasetBuilder
from src.framework.connectors.files.trip_dataset_context_loader import LoadedTripDatasetContext
from src.validators.reconciliation.trip_batch_validator import TripSearchBatchValidator
from src.validators.reconciliation.trip_suite_executor import TripSearchRunSuiteExecutor


@pytest.fixture(scope="session")
def large_dataset_builder() -> SyntheticTripDatasetBuilder:
    return SyntheticTripDatasetBuilder()


@pytest.fixture(scope="session")
def large_loaded_trip_dataset(config, trip_dataset_context_loader) -> LoadedTripDatasetContext:
    return trip_dataset_context_loader.load(config, dataset_profile="large")


@pytest.fixture(scope="session")
def large_dataset_profile_frame(
    large_dataset_builder: SyntheticTripDatasetBuilder,
    large_loaded_trip_dataset: LoadedTripDatasetContext,
) -> pd.DataFrame:
    return large_dataset_builder.build_profile_frame(large_loaded_trip_dataset.raw_trip_frame)


@pytest.fixture(scope="session")
def large_raw_trip_frame(large_loaded_trip_dataset: LoadedTripDatasetContext) -> pd.DataFrame:
    return large_loaded_trip_dataset.raw_trip_frame


@pytest.fixture(scope="session")
def large_normalized_trips(large_loaded_trip_dataset: LoadedTripDatasetContext) -> list:
    return large_loaded_trip_dataset.normalized_trips


@pytest.fixture(scope="session")
def large_expected_trip_frame(large_loaded_trip_dataset: LoadedTripDatasetContext) -> pd.DataFrame:
    return large_loaded_trip_dataset.expected_trip_frame


@pytest.fixture()
def large_sqlite_client() -> SQLiteClient:
    db_dir = Path(__file__).resolve().parent / ".local_test_data"
    db_dir.mkdir(exist_ok=True)
    db_path = db_dir / f"large_trip_validation_{uuid4().hex}.sqlite"
    client = SQLiteClient(db_path)
    client.initialize_schema()
    try:
        yield client
    finally:
        client.close()
        if db_path.exists():
            db_path.unlink()


@pytest.fixture()
def large_seeded_trip_db(large_sqlite_client: SQLiteClient, large_normalized_trips: list) -> SQLiteClient:
    TripQueries(large_sqlite_client).seed_trips(large_normalized_trips)
    return large_sqlite_client


@pytest.fixture()
def large_trip_search_service_api(large_seeded_trip_db) -> SearchServiceAPI:
    return SearchServiceAPI(TripQueries(large_seeded_trip_db))


@pytest.fixture()
def large_scenario_loader(dataset_loader) -> TripSearchScenarioLoader:
    return TripSearchScenarioLoader(dataset_loader)


@pytest.fixture()
def large_run_profile_loader() -> TripSearchRunProfileLoader:
    return TripSearchRunProfileLoader()


@pytest.fixture()
def large_run_suite_loader() -> TripSearchRunSuiteLoader:
    return TripSearchRunSuiteLoader()


@pytest.fixture()
def large_batch_scenario_dataset(
    large_scenario_loader: TripSearchScenarioLoader,
    large_loaded_trip_dataset: LoadedTripDatasetContext,
) -> LoadedTripSearchScenarioDataset:
    return large_scenario_loader.load_csv(large_loaded_trip_dataset.scenario_dataset_path)


@pytest.fixture()
def large_batch_scenarios(large_batch_scenario_dataset: LoadedTripSearchScenarioDataset):
    return large_batch_scenario_dataset.scenarios


@pytest.fixture()
def large_run_suite(large_run_suite_loader: TripSearchRunSuiteLoader, large_loaded_trip_dataset: LoadedTripDatasetContext):
    return large_run_suite_loader.load_json(large_loaded_trip_dataset.default_run_suite_path)


@pytest.fixture()
def large_batch_validator(config, large_trip_search_service_api: SearchServiceAPI) -> TripSearchBatchValidator:
    return TripSearchBatchValidator(large_trip_search_service_api, config.numeric_tolerance)


@pytest.fixture()
def large_run_suite_executor(
    config,
    large_trip_search_service_api: SearchServiceAPI,
    large_run_profile_loader: TripSearchRunProfileLoader,
) -> TripSearchRunSuiteExecutor:
    return TripSearchRunSuiteExecutor(
        service_api=large_trip_search_service_api,
        numeric_tolerance=config.numeric_tolerance,
        run_profile_loader=large_run_profile_loader,
    )
