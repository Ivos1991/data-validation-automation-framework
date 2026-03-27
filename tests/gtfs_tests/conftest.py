from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.framework.connectors.db.sqlite_client import SQLiteClient
from src.framework.connectors.db.trip_queries import TripQueries
from src.framework.connectors.files.gtfs_loader import GtfsDatasetLoader
from src.framework.connectors.files.run_profile_loader import TripSearchRunProfileLoader
from src.framework.connectors.files.scenario_loader import LoadedTripSearchScenarioDataset, TripSearchScenarioLoader
from src.framework.connectors.files.trip_dataset_context_loader import LoadedTripDatasetContext
from src.transformers.gtfs_trip_transformer import GtfsTripTransformer


@pytest.fixture(scope="session")
def gtfs_directory_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "raw" / "gtfs_sample"


@pytest.fixture(scope="session")
def gtfs_scenario_dataset_path() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "raw" / "gtfs_batch_trip_search_scenarios.csv"


@pytest.fixture(scope="session")
def gtfs_loaded_trip_dataset(config, trip_dataset_context_loader) -> LoadedTripDatasetContext:
    return trip_dataset_context_loader.load(config, dataset_profile="gtfs")


@pytest.fixture()
def gtfs_dataset_loader(dataset_loader) -> GtfsDatasetLoader:
    return GtfsDatasetLoader(dataset_loader)


@pytest.fixture()
def gtfs_trip_transformer() -> GtfsTripTransformer:
    return GtfsTripTransformer()


@pytest.fixture()
def gtfs_run_profile_loader() -> TripSearchRunProfileLoader:
    return TripSearchRunProfileLoader()


@pytest.fixture()
def local_gtfs_test_dir() -> Path:
    test_dir = Path(__file__).resolve().parent / ".local_test_data"
    test_dir.mkdir(exist_ok=True)
    return test_dir


@pytest.fixture()
def loaded_gtfs_dataset(gtfs_dataset_loader: GtfsDatasetLoader, gtfs_directory_path: Path):
    return gtfs_dataset_loader.load_directory(gtfs_directory_path)


@pytest.fixture()
def gtfs_transformed_trip_frame(loaded_gtfs_dataset, gtfs_trip_transformer: GtfsTripTransformer) -> pd.DataFrame:
    return gtfs_trip_transformer.transform(loaded_gtfs_dataset)


@pytest.fixture()
def gtfs_scenario_loader(dataset_loader) -> TripSearchScenarioLoader:
    return TripSearchScenarioLoader(dataset_loader)


@pytest.fixture()
def gtfs_scenario_dataset(
    gtfs_scenario_loader: TripSearchScenarioLoader,
    gtfs_loaded_trip_dataset: LoadedTripDatasetContext,
) -> LoadedTripSearchScenarioDataset:
    return gtfs_scenario_loader.load_csv(gtfs_loaded_trip_dataset.scenario_dataset_path)


@pytest.fixture()
def gtfs_sqlite_client() -> SQLiteClient:
    db_dir = Path(__file__).resolve().parent / ".local_test_data"
    db_dir.mkdir(exist_ok=True)
    db_path = db_dir / f"gtfs_trip_validation_{uuid4().hex}.sqlite"
    client = SQLiteClient(db_path)
    client.initialize_schema()
    try:
        yield client
    finally:
        client.close()
        if db_path.exists():
            db_path.unlink()


@pytest.fixture()
def gtfs_seeded_trip_db(gtfs_sqlite_client: SQLiteClient, gtfs_loaded_trip_dataset: LoadedTripDatasetContext) -> SQLiteClient:
    TripQueries(gtfs_sqlite_client).seed_trips(gtfs_loaded_trip_dataset.normalized_trips)
    return gtfs_sqlite_client


@pytest.fixture()
def gtfs_trip_search_service_api(gtfs_seeded_trip_db) -> SearchServiceAPI:
    return SearchServiceAPI(TripQueries(gtfs_seeded_trip_db))
