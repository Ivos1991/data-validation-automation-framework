from __future__ import annotations

from src.domain.trip_search.search_models import TripSearchRequest
from src.framework.utils.date_utils import parse_iso_date, to_iso_date
from src.framework.utils.numeric_utils import normalize_int


class SearchServiceRequest:
    """Build normalized request objects and query parameters for trip search."""

    @staticmethod
    def build(
        origin: str,
        destination: str,
        departure_date: str,
        carrier: str | None = None,
        stops_count: int | None = None,
    ) -> TripSearchRequest:
        """Normalize external search inputs into a typed request model."""
        return TripSearchRequest(
            origin=origin.strip().upper(),
            destination=destination.strip().upper(),
            departure_date=parse_iso_date(departure_date),
            carrier=carrier.strip() if carrier else None,
            stops_count=normalize_int(stops_count) if stops_count is not None else None,
        )

    @staticmethod
    def search_by_route_and_departure_date_request(request: TripSearchRequest) -> dict[str, str | int]:
        """Convert a typed request into transport/query parameters."""
        query_params: dict[str, str | int] = {
            "origin": request.origin,
            "destination": request.destination,
            "departure_date": to_iso_date(request.departure_date),
        }
        if request.carrier:
            query_params["carrier"] = request.carrier
        if request.stops_count is not None:
            query_params["stops_count"] = request.stops_count
        return query_params
