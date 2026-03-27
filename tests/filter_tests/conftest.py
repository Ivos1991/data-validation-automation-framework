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
def search_criteria_nyc_to_bos_rail() -> dict[str, str]:
    return {
        "origin": "NYC",
        "destination": "BOS",
        "departure_date": "2026-04-01",
        "carrier": "AmRail",
    }


@pytest.fixture()
def search_criteria_nyc_to_bos_nonstop() -> dict[str, str | int]:
    return {
        "origin": "NYC",
        "destination": "BOS",
        "departure_date": "2026-04-01",
        "stops_count": 0,
    }


@pytest.fixture()
def search_criteria_nyc_to_bos_rail_nonstop() -> dict[str, str | int]:
    return {
        "origin": "NYC",
        "destination": "BOS",
        "departure_date": "2026-04-01",
        "carrier": "AmRail",
        "stops_count": 0,
    }


@pytest.fixture()
def search_criteria_nyc_to_bos_rail_one_stop() -> dict[str, str | int]:
    return {
        "origin": "NYC",
        "destination": "BOS",
        "departure_date": "2026-04-01",
        "carrier": "AmRail",
        "stops_count": 1,
    }


@pytest.fixture()
def filtered_trip_search_service_api(seeded_trip_db) -> SearchServiceAPI:
    return SearchServiceAPI(TripQueries(seeded_trip_db))


@pytest.fixture()
def filtered_expected_trip_frame(expected_trip_frame: pd.DataFrame, search_criteria_nyc_to_bos_rail: dict[str, str]) -> pd.DataFrame:
    return filter_expected_trip_frame(expected_trip_frame, **search_criteria_nyc_to_bos_rail)


@pytest.fixture()
def filtered_actual_trip_frame(filtered_trip_search_service_api: SearchServiceAPI, search_criteria_nyc_to_bos_rail: dict[str, str]) -> pd.DataFrame:
    request = SearchServiceRequest.build(**search_criteria_nyc_to_bos_rail)
    trips = search_by_route_and_departure_date(filtered_trip_search_service_api, request)
    return pd.DataFrame([trip.to_canonical_dict() for trip in trips])


@pytest.fixture()
def filtered_reconciliation_result(filtered_expected_trip_frame: pd.DataFrame, filtered_actual_trip_frame: pd.DataFrame):
    return TripReconciliationValidator().reconcile(filtered_expected_trip_frame, filtered_actual_trip_frame)


@pytest.fixture()
def filtered_expected_aggregate_summary(filtered_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(filtered_expected_trip_frame)


@pytest.fixture()
def filtered_actual_aggregate_summary(filtered_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(filtered_actual_trip_frame)


@pytest.fixture()
def filtered_expected_carrier_count_frame(filtered_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(filtered_expected_trip_frame)


@pytest.fixture()
def filtered_actual_carrier_count_frame(filtered_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(filtered_actual_trip_frame)


@pytest.fixture()
def non_matching_carrier_rows(filtered_actual_trip_frame: pd.DataFrame, search_criteria_nyc_to_bos_rail: dict[str, str]) -> pd.DataFrame:
    return filtered_actual_trip_frame[
        filtered_actual_trip_frame["carrier"] != search_criteria_nyc_to_bos_rail["carrier"]
    ].reset_index(drop=True)


@pytest.fixture()
def filtered_aggregate_comparison_result(
    config,
    filtered_expected_aggregate_summary: pd.DataFrame,
    filtered_actual_aggregate_summary: pd.DataFrame,
    filtered_expected_carrier_count_frame: pd.DataFrame,
    filtered_actual_carrier_count_frame: pd.DataFrame,
):
    return TripAggregateValidator(config.numeric_tolerance).validate(
        expected_summary=filtered_expected_aggregate_summary,
        actual_summary=filtered_actual_aggregate_summary,
        expected_carrier_counts=filtered_expected_carrier_count_frame,
        actual_carrier_counts=filtered_actual_carrier_count_frame,
    )


@pytest.fixture()
def stops_filtered_expected_trip_frame(expected_trip_frame: pd.DataFrame, search_criteria_nyc_to_bos_nonstop: dict[str, str | int]) -> pd.DataFrame:
    return filter_expected_trip_frame(expected_trip_frame, **search_criteria_nyc_to_bos_nonstop)


@pytest.fixture()
def stops_filtered_actual_trip_frame(
    filtered_trip_search_service_api: SearchServiceAPI,
    search_criteria_nyc_to_bos_nonstop: dict[str, str | int],
) -> pd.DataFrame:
    request = SearchServiceRequest.build(**search_criteria_nyc_to_bos_nonstop)
    trips = search_by_route_and_departure_date(filtered_trip_search_service_api, request)
    return pd.DataFrame([trip.to_canonical_dict() for trip in trips])


@pytest.fixture()
def stops_filtered_reconciliation_result(stops_filtered_expected_trip_frame: pd.DataFrame, stops_filtered_actual_trip_frame: pd.DataFrame):
    return TripReconciliationValidator().reconcile(stops_filtered_expected_trip_frame, stops_filtered_actual_trip_frame)


@pytest.fixture()
def stops_filtered_expected_aggregate_summary(stops_filtered_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(stops_filtered_expected_trip_frame)


@pytest.fixture()
def stops_filtered_actual_aggregate_summary(stops_filtered_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(stops_filtered_actual_trip_frame)


@pytest.fixture()
def stops_filtered_expected_carrier_count_frame(stops_filtered_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(stops_filtered_expected_trip_frame)


@pytest.fixture()
def stops_filtered_actual_carrier_count_frame(stops_filtered_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(stops_filtered_actual_trip_frame)


@pytest.fixture()
def non_matching_stops_rows(
    stops_filtered_actual_trip_frame: pd.DataFrame,
    search_criteria_nyc_to_bos_nonstop: dict[str, str | int],
) -> pd.DataFrame:
    return stops_filtered_actual_trip_frame[
        stops_filtered_actual_trip_frame["stops_count"] != search_criteria_nyc_to_bos_nonstop["stops_count"]
    ].reset_index(drop=True)


@pytest.fixture()
def stops_filtered_aggregate_comparison_result(
    config,
    stops_filtered_expected_aggregate_summary: pd.DataFrame,
    stops_filtered_actual_aggregate_summary: pd.DataFrame,
    stops_filtered_expected_carrier_count_frame: pd.DataFrame,
    stops_filtered_actual_carrier_count_frame: pd.DataFrame,
):
    return TripAggregateValidator(config.numeric_tolerance).validate(
        expected_summary=stops_filtered_expected_aggregate_summary,
        actual_summary=stops_filtered_actual_aggregate_summary,
        expected_carrier_counts=stops_filtered_expected_carrier_count_frame,
        actual_carrier_counts=stops_filtered_actual_carrier_count_frame,
    )


@pytest.fixture()
def combined_filtered_expected_trip_frame(
    expected_trip_frame: pd.DataFrame,
    search_criteria_nyc_to_bos_rail_nonstop: dict[str, str | int],
) -> pd.DataFrame:
    return filter_expected_trip_frame(expected_trip_frame, **search_criteria_nyc_to_bos_rail_nonstop)


@pytest.fixture()
def combined_filtered_actual_trip_frame(
    filtered_trip_search_service_api: SearchServiceAPI,
    search_criteria_nyc_to_bos_rail_nonstop: dict[str, str | int],
) -> pd.DataFrame:
    request = SearchServiceRequest.build(**search_criteria_nyc_to_bos_rail_nonstop)
    trips = search_by_route_and_departure_date(filtered_trip_search_service_api, request)
    return pd.DataFrame([trip.to_canonical_dict() for trip in trips])


@pytest.fixture()
def combined_filtered_reconciliation_result(
    combined_filtered_expected_trip_frame: pd.DataFrame,
    combined_filtered_actual_trip_frame: pd.DataFrame,
):
    return TripReconciliationValidator().reconcile(combined_filtered_expected_trip_frame, combined_filtered_actual_trip_frame)


@pytest.fixture()
def combined_filtered_expected_aggregate_summary(combined_filtered_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(combined_filtered_expected_trip_frame)


@pytest.fixture()
def combined_filtered_actual_aggregate_summary(combined_filtered_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(combined_filtered_actual_trip_frame)


@pytest.fixture()
def combined_filtered_expected_carrier_count_frame(combined_filtered_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(combined_filtered_expected_trip_frame)


@pytest.fixture()
def combined_filtered_actual_carrier_count_frame(combined_filtered_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(combined_filtered_actual_trip_frame)


@pytest.fixture()
def combined_filter_violating_rows(
    combined_filtered_actual_trip_frame: pd.DataFrame,
    search_criteria_nyc_to_bos_rail_nonstop: dict[str, str | int],
) -> pd.DataFrame:
    return combined_filtered_actual_trip_frame[
        (combined_filtered_actual_trip_frame["carrier"] != search_criteria_nyc_to_bos_rail_nonstop["carrier"])
        | (combined_filtered_actual_trip_frame["stops_count"] != search_criteria_nyc_to_bos_rail_nonstop["stops_count"])
    ].reset_index(drop=True)


@pytest.fixture()
def combined_filtered_aggregate_comparison_result(
    config,
    combined_filtered_expected_aggregate_summary: pd.DataFrame,
    combined_filtered_actual_aggregate_summary: pd.DataFrame,
    combined_filtered_expected_carrier_count_frame: pd.DataFrame,
    combined_filtered_actual_carrier_count_frame: pd.DataFrame,
):
    return TripAggregateValidator(config.numeric_tolerance).validate(
        expected_summary=combined_filtered_expected_aggregate_summary,
        actual_summary=combined_filtered_actual_aggregate_summary,
        expected_carrier_counts=combined_filtered_expected_carrier_count_frame,
        actual_carrier_counts=combined_filtered_actual_carrier_count_frame,
    )


@pytest.fixture()
def combined_no_match_expected_trip_frame(
    expected_trip_frame: pd.DataFrame,
    search_criteria_nyc_to_bos_rail_one_stop: dict[str, str | int],
) -> pd.DataFrame:
    return filter_expected_trip_frame(expected_trip_frame, **search_criteria_nyc_to_bos_rail_one_stop)


@pytest.fixture()
def combined_no_match_actual_trip_frame(
    filtered_trip_search_service_api: SearchServiceAPI,
    search_criteria_nyc_to_bos_rail_one_stop: dict[str, str | int],
) -> pd.DataFrame:
    request = SearchServiceRequest.build(**search_criteria_nyc_to_bos_rail_one_stop)
    trips = search_by_route_and_departure_date(filtered_trip_search_service_api, request)
    return pd.DataFrame([trip.to_canonical_dict() for trip in trips])


@pytest.fixture()
def combined_no_match_reconciliation_result(
    combined_no_match_expected_trip_frame: pd.DataFrame,
    combined_no_match_actual_trip_frame: pd.DataFrame,
):
    expected_frame = combined_no_match_expected_trip_frame
    actual_frame = combined_no_match_actual_trip_frame.reindex(columns=expected_frame.columns, fill_value=None)
    return TripReconciliationValidator().reconcile(expected_frame, actual_frame)


@pytest.fixture()
def combined_no_match_expected_aggregate_summary(combined_no_match_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_aggregate_summary(combined_no_match_expected_trip_frame)


@pytest.fixture()
def combined_no_match_actual_aggregate_summary(combined_no_match_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    actual_frame = combined_no_match_actual_trip_frame
    if actual_frame.empty:
        actual_frame = pd.DataFrame(columns=["trip_id", "origin", "destination", "departure_date", "stops_count", "route_id", "carrier", "price_amount", "currency", "duration_minutes"])
    return build_aggregate_summary(actual_frame)


@pytest.fixture()
def combined_no_match_expected_carrier_count_frame(combined_no_match_expected_trip_frame: pd.DataFrame) -> pd.DataFrame:
    return build_carrier_count_frame(combined_no_match_expected_trip_frame)


@pytest.fixture()
def combined_no_match_actual_carrier_count_frame(combined_no_match_actual_trip_frame: pd.DataFrame) -> pd.DataFrame:
    if combined_no_match_actual_trip_frame.empty:
        return pd.DataFrame(columns=["carrier", "result_count"])
    return build_carrier_count_frame(combined_no_match_actual_trip_frame)


@pytest.fixture()
def combined_no_match_aggregate_comparison_result(
    config,
    combined_no_match_expected_aggregate_summary: pd.DataFrame,
    combined_no_match_actual_aggregate_summary: pd.DataFrame,
    combined_no_match_expected_carrier_count_frame: pd.DataFrame,
    combined_no_match_actual_carrier_count_frame: pd.DataFrame,
):
    return TripAggregateValidator(config.numeric_tolerance).validate(
        expected_summary=combined_no_match_expected_aggregate_summary,
        actual_summary=combined_no_match_actual_aggregate_summary,
        expected_carrier_counts=combined_no_match_expected_carrier_count_frame,
        actual_carrier_counts=combined_no_match_actual_carrier_count_frame,
    )
