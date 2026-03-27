import allure
from assertpy import assert_that

from src.domain.trip_search.search_service import search_by_route_and_departure_date
from src.domain.trip_search.search_service_request import SearchServiceRequest


@allure.parent_suite("Trip Search Validation")
@allure.suite("Service Tests")
@allure.sub_suite("Trip Search")
class TestSearchService:
    """Service-layer tests for the deterministic trip-search flow."""

    @allure.title("Search service returns normalized trips for a deterministic route and departure date")
    def test_search_service_returns_expected_trips(self, trip_search_service_api, search_criteria_any_nyc_to_bos):
        """Verify the service function returns normalized canonical trips."""
        request = SearchServiceRequest.build(**search_criteria_any_nyc_to_bos)
        actual_trips = search_by_route_and_departure_date(trip_search_service_api, request)

        assert_that(actual_trips).is_length(2)
        assert_that([trip.trip_id for trip in actual_trips]).is_equal_to(["TRIP-001", "TRIP-002"])
        assert_that(actual_trips[0].origin).is_equal_to("NYC")
        assert_that(actual_trips[0].destination).is_equal_to("BOS")
        assert_that(actual_trips[0].departure_date.isoformat()).is_equal_to("2026-04-01")
