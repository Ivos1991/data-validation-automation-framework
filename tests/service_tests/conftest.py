import pytest

from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.framework.connectors.db.trip_queries import TripQueries


@pytest.fixture()
def trip_search_service_api(seeded_trip_db) -> SearchServiceAPI:
    return SearchServiceAPI(TripQueries(seeded_trip_db))


@pytest.fixture()
def search_criteria_any_nyc_to_bos() -> dict[str, str]:
    return {
        "origin": "NYC",
        "destination": "BOS",
        "departure_date": "2026-04-01",
    }
