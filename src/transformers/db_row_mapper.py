from __future__ import annotations

from src.domain.trip_search.search_models import Trip
from src.transformers.trip_model_mapper import TripModelMapper


class DbRowMapper:
    """Map raw database rows into canonical trip models."""

    def __init__(self) -> None:
        self.trip_model_mapper = TripModelMapper()

    def map_row(self, raw_db_row: dict) -> Trip:
        """Map one database row into a canonical trip."""
        return self.trip_model_mapper.from_raw_row(raw_db_row)

    def map_rows(self, raw_db_rows: list[dict]) -> list[Trip]:
        """Map multiple database rows into canonical trips."""
        return [self.map_row(row) for row in raw_db_rows]
