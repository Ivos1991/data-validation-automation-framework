import pandas as pd
import pytest
from pathlib import Path

from src.domain.trip_search.search_models import TripSearchRunProfile, TripSearchScenarioSelection
from src.domain.trip_search.search_scenario_selector import TripSearchScenarioSelector
from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.framework.connectors.db.trip_queries import TripQueries
from src.framework.connectors.files.run_profile_loader import TripSearchRunProfileLoader
from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader
from src.framework.connectors.files.scenario_loader import LoadedTripSearchScenarioDataset, TripSearchScenarioLoader
from src.validators.reconciliation.trip_batch_validator import TripSearchBatchValidator
from src.validators.reconciliation.trip_suite_executor import TripSearchRunSuiteExecutor


@pytest.fixture()
def batch_trip_search_service_api(seeded_trip_db) -> SearchServiceAPI:
    return SearchServiceAPI(TripQueries(seeded_trip_db))


@pytest.fixture()
def local_batch_test_dir() -> Path:
    test_dir = Path(__file__).resolve().parent / ".local_test_data"
    test_dir.mkdir(exist_ok=True)
    return test_dir


@pytest.fixture()
def scenario_loader(dataset_loader) -> TripSearchScenarioLoader:
    return TripSearchScenarioLoader(dataset_loader)


@pytest.fixture()
def scenario_selector() -> TripSearchScenarioSelector:
    return TripSearchScenarioSelector()


@pytest.fixture()
def run_profile_loader() -> TripSearchRunProfileLoader:
    return TripSearchRunProfileLoader()


@pytest.fixture()
def run_suite_loader() -> TripSearchRunSuiteLoader:
    return TripSearchRunSuiteLoader()


@pytest.fixture()
def batch_scenario_dataset(loaded_trip_dataset, scenario_loader: TripSearchScenarioLoader) -> LoadedTripSearchScenarioDataset:
    return scenario_loader.load_csv(loaded_trip_dataset.scenario_dataset_path)


@pytest.fixture()
def batch_scenarios(batch_scenario_dataset: LoadedTripSearchScenarioDataset):
    return batch_scenario_dataset.scenarios


@pytest.fixture()
def default_run_profile(loaded_trip_dataset, run_profile_loader: TripSearchRunProfileLoader) -> TripSearchRunProfile:
    return run_profile_loader.load_json(loaded_trip_dataset.default_run_profile_path)


@pytest.fixture()
def default_run_suite(loaded_trip_dataset, run_suite_loader: TripSearchRunSuiteLoader):
    return run_suite_loader.load_json(loaded_trip_dataset.default_run_suite_path)


@pytest.fixture()
def batch_validation_result(config, batch_trip_search_service_api: SearchServiceAPI, expected_trip_frame: pd.DataFrame, batch_scenarios):
    validator = TripSearchBatchValidator(batch_trip_search_service_api, config.numeric_tolerance)
    return validator.validate(batch_scenarios, expected_trip_frame)


@pytest.fixture()
def filter_pack_selection() -> TripSearchScenarioSelection:
    return TripSearchScenarioSelection(pack="filters")


@pytest.fixture()
def combined_tag_selection() -> TripSearchScenarioSelection:
    return TripSearchScenarioSelection(tag="combined")


@pytest.fixture()
def negative_type_selection() -> TripSearchScenarioSelection:
    return TripSearchScenarioSelection(scenario_type="negative")


@pytest.fixture()
def run_suite_executor(config, batch_trip_search_service_api: SearchServiceAPI, run_profile_loader: TripSearchRunProfileLoader):
    return TripSearchRunSuiteExecutor(
        service_api=batch_trip_search_service_api,
        numeric_tolerance=config.numeric_tolerance,
        run_profile_loader=run_profile_loader,
    )
