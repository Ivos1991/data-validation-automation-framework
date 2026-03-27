from __future__ import annotations

from src.framework.connectors.db.trip_queries import TripQueries


class SearchServiceAPI:
    """Thin API-style adapter over the trip query layer."""

    def __init__(self, trip_queries: TripQueries) -> None:
        self.trip_queries = trip_queries

    def search_by_route_and_departure_date(self, request_params: dict[str, str | int]) -> dict:
        """Return raw trip rows for a route/date search request."""
        rows = self.trip_queries.find_trips_by_route_and_date(
            origin=request_params["origin"],
            destination=request_params["destination"],
            departure_date=request_params["departure_date"],
            carrier=request_params.get("carrier"),
            stops_count=request_params.get("stops_count"),
        )
        return {"trips": rows}
