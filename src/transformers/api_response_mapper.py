from __future__ import annotations

from src.domain.trip_search.search_models import Trip
from src.transformers.trip_model_mapper import TripModelMapper


class ApiResponseMapper:
    """Map API-style payloads into canonical trip models."""

    def __init__(self) -> None:
        self.trip_model_mapper = TripModelMapper()

    def map_payload(self, raw_payload: dict) -> list[Trip]:
        """Map the `trips` payload into canonical trip entities."""
        return [self.trip_model_mapper.from_raw_row(item) for item in raw_payload.get("trips", [])]
