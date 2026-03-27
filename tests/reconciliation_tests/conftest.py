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


@pytest.fixture()
def search_criteria_any_nyc_to_bos() -> dict[str, str]:
    return {
        "origin": "NYC",
        "destination": "BOS",
        "departure_date": "2026-04-01",
    }


@pytest.fixture()
def normalized_expected_trip_frame(expected_trip_frame: pd.DataFrame, search_criteria_any_nyc_to_bos: dict[str, str]) -> pd.DataFrame:
    return filter_expected_trip_frame(expected_trip_frame, **search_criteria_any_nyc_to_bos)


@pytest.fixture()
def actual_trip_frame(seeded_trip_db, search_criteria_any_nyc_to_bos: dict[str, str]) -> pd.DataFrame:
    request = SearchServiceRequest.build(**search_criteria_any_nyc_to_bos)
    trips = search_by_route_and_departure_date(SearchServiceAPI(TripQueries(seeded_trip_db)), request)
    return pd.DataFrame([trip.to_canonical_dict() for trip in trips])


@pytest.fixture()
def reconciliation_result(normalized_expected_trip_frame: pd.DataFrame, actual_trip_frame: pd.DataFrame):
    validator = TripReconciliationValidator()
    return validator.reconcile(normalized_expected_trip_frame, actual_trip_frame)


@pytest.fixture()
def expected_aggregate_summary(normalized_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(normalized_expected_trip_frame)


@pytest.fixture()
def actual_aggregate_summary(actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(actual_trip_frame)


@pytest.fixture()
def expected_carrier_count_frame(normalized_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(normalized_expected_trip_frame)


@pytest.fixture()
def actual_carrier_count_frame(actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(actual_trip_frame)


@pytest.fixture()
def aggregate_comparison_result(
    config,
    expected_aggregate_summary: pd.DataFrame,
    actual_aggregate_summary: pd.DataFrame,
    expected_carrier_count_frame: pd.DataFrame,
    actual_carrier_count_frame: pd.DataFrame,
):
    validator = TripAggregateValidator(config.numeric_tolerance)
    return validator.validate(
        expected_summary=expected_aggregate_summary,
        actual_summary=actual_aggregate_summary,
        expected_carrier_counts=expected_carrier_count_frame,
        actual_carrier_counts=actual_carrier_count_frame,
    )
