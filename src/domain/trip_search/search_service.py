from __future__ import annotations

from src.domain.trip_search.search_models import Trip, TripSearchRequest
from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.domain.trip_search.search_service_request import SearchServiceRequest
from src.transformers.api_response_mapper import ApiResponseMapper


def search_by_route_and_departure_date(service_api: SearchServiceAPI, request: TripSearchRequest) -> list[Trip]:
    """Execute the canonical trip-search flow for a normalized request."""
    request_params = SearchServiceRequest.search_by_route_and_departure_date_request(request)
    payload = service_api.search_by_route_and_departure_date(request_params)
    assert "trips" in payload, "trip_search_service search response missing 'trips' payload"
    return ApiResponseMapper().map_payload(payload)
