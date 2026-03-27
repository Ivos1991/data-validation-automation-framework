import pandas as pd
import pytest

from src.domain.trip_search.search_service import search_by_route_and_departure_date
from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.domain.trip_search.search_service_request import SearchServiceRequest
from src.framework.connectors.db.trip_queries import TripQueries
from src.framework.utils.dataframe_utils import (
    build_aggregate_summary,
    build_carrier_count_frame,
    filter_expected_trip_frame,
)
from src.validators.aggregate.trip_aggregate_validator import TripAggregateValidator
from src.validators.reconciliation.trip_reconciliation_validator import TripReconciliationValidator


"""
Setup fixture execution order:
- load raw dataset
- normalize source rows into canonical trip models
- seed SQLite with canonical trips
- search the seeded store through the service/API/request path
- reconcile actual results against pandas-derived expected results
"""


@pytest.fixture()
def search_criteria_any_nyc_to_bos() -> dict[str, str]:
    return {
        "origin": "NYC",
        "destination": "BOS",
        "departure_date": "2026-04-01",
    }


@pytest.fixture()
def trip_search_service_api(seeded_trip_db) -> SearchServiceAPI:
    return SearchServiceAPI(TripQueries(seeded_trip_db))


@pytest.fixture()
def integration_expected_trip_frame(expected_trip_frame: pd.DataFrame, search_criteria_any_nyc_to_bos: dict[str, str]) -> pd.DataFrame:
    return filter_expected_trip_frame(expected_trip_frame, **search_criteria_any_nyc_to_bos)


@pytest.fixture()
def integration_actual_trip_frame(trip_search_service_api: SearchServiceAPI, search_criteria_any_nyc_to_bos: dict[str, str]) -> pd.DataFrame:
    request = SearchServiceRequest.build(**search_criteria_any_nyc_to_bos)
    trips = search_by_route_and_departure_date(trip_search_service_api, request)
    return pd.DataFrame([trip.to_canonical_dict() for trip in trips])


@pytest.fixture()
def integration_reconciliation_result(integration_expected_trip_frame: pd.DataFrame, integration_actual_trip_frame: pd.DataFrame):
    return TripReconciliationValidator().reconcile(integration_expected_trip_frame, integration_actual_trip_frame)


@pytest.fixture()
def integration_expected_aggregate_summary(integration_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(integration_expected_trip_frame)


@pytest.fixture()
def integration_actual_aggregate_summary(integration_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(integration_actual_trip_frame)


@pytest.fixture()
def integration_expected_carrier_count_frame(integration_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(integration_expected_trip_frame)


@pytest.fixture()
def integration_actual_carrier_count_frame(integration_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(integration_actual_trip_frame)


@pytest.fixture()
def integration_aggregate_result(
    config,
    integration_expected_aggregate_summary: pd.DataFrame,
    integration_actual_aggregate_summary: pd.DataFrame,
    integration_expected_carrier_count_frame: pd.DataFrame,
    integration_actual_carrier_count_frame: pd.DataFrame,
):
    return TripAggregateValidator(config.numeric_tolerance).validate(
        expected_summary=integration_expected_aggregate_summary,
        actual_summary=integration_actual_aggregate_summary,
        expected_carrier_counts=integration_expected_carrier_count_frame,
        actual_carrier_counts=integration_actual_carrier_count_frame,
    )
