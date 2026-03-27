from __future__ import annotations

from src.domain.trip_search.search_models import Trip
from src.framework.connectors.db.sqlite_client import SQLiteClient


class TripQueries:
    """Repository-style trip queries over the canonical SQLite schema."""

    def __init__(self, sqlite_client: SQLiteClient) -> None:
        self.sqlite_client = sqlite_client

    def seed_trips(self, trips: list[Trip]) -> None:
        """Insert canonical trips into SQLite for deterministic validation runs."""
        parameters = [
            (
                trip.trip_id,
                trip.origin,
                trip.destination,
                trip.departure_date.isoformat(),
                trip.stops_count,
                trip.route_id,
                trip.carrier,
                trip.price_amount,
                trip.currency,
                trip.duration_minutes,
            )
            for trip in trips
        ]
        self.sqlite_client.execute_many(
            """
            INSERT INTO trips (
                trip_id,
                origin,
                destination,
                departure_date,
                stops_count,
                route_id,
                carrier,
                price_amount,
                currency,
                duration_minutes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            parameters,
        )

    def find_trips_by_route_and_date(
        self,
        origin: str,
        destination: str,
        departure_date: str,
        carrier: str | None = None,
        stops_count: int | None = None,
    ) -> list[dict]:
        """Search trips by route/date and optional filters through SQLite."""
        query = """
            SELECT
                trip_id,
                origin,
                destination,
                departure_date,
                stops_count,
                route_id,
                carrier,
                price_amount,
                currency,
                duration_minutes
            FROM trips
            WHERE origin = ? AND destination = ? AND departure_date = ?
        """
        parameters: list[str | int] = [origin, destination, departure_date]
        if carrier:
            query += " AND carrier = ?"
            parameters.append(carrier)
        if stops_count is not None:
            query += " AND stops_count = ?"
            parameters.append(stops_count)
        query += " ORDER BY trip_id"
        return self.sqlite_client.fetch_all(
            query,
            tuple(parameters),
        )
