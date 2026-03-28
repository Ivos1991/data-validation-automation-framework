import allure
from tests.assertions import assert_that

from src.domain.trip_search.search_service import search_by_route_and_departure_date
from src.domain.trip_search.search_service_request import SearchServiceRequest


@allure.parent_suite("Trip Search Validation")
@allure.suite("Service Tests")
@allure.sub_suite("Trip Search")
class TestSearchService:
    """Service-layer tests for the deterministic trip-search flow."""

    @allure.title("Search service returns normalized trips for a deterministic route and departure date")
    def test_search_service_expects_expected_trips_for_route_and_date(self, trip_search_service_api, search_criteria_any_nyc_to_bos):
        """Verify the service function returns normalized canonical trips."""
        request = SearchServiceRequest.build(**search_criteria_any_nyc_to_bos)
        actual_trips = search_by_route_and_departure_date(trip_search_service_api, request)

        assert_that(actual_trips, "Expected assertion for actual_trips to hold").is_length(2)
        assert_that([trip.trip_id for trip in actual_trips], "Expected assertion for [trip.trip_id for trip in actual_trips] to hold").is_equal_to(["TRIP-001", "TRIP-002"])
        assert_that(actual_trips[0].departure_date.isoformat(), "Expected assertion for actual_trips[0].departure_date.isoformat() to hold").is_equal_to("2026-04-01")
