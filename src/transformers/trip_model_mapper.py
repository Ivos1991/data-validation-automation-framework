from __future__ import annotations

import pandas as pd

from src.domain.trip_search.search_models import Trip
from src.framework.utils.date_utils import parse_iso_date
from src.framework.utils.numeric_utils import normalize_float, normalize_int


class TripModelMapper:
    """Map normalized trip rows into canonical Trip entities."""

    def from_raw_row(self, raw_row: dict) -> Trip:
        """Map one raw trip row into the canonical Trip model."""
        return Trip(
            trip_id=str(raw_row["trip_id"]).strip(),
            origin=str(raw_row["origin"]).strip().upper(),
            destination=str(raw_row["destination"]).strip().upper(),
            departure_date=parse_iso_date(raw_row["departure_date"]),
            stops_count=normalize_int(raw_row["stops_count"]),
            route_id=str(raw_row["route_id"]).strip().upper(),
            carrier=str(raw_row["carrier"]).strip(),
            price_amount=normalize_float(raw_row["price_amount"]),
            currency=str(raw_row["currency"]).strip().upper(),
            duration_minutes=normalize_int(raw_row["duration_minutes"]),
        )

    def from_dataframe(self, raw_trip_frame: pd.DataFrame) -> list[Trip]:
        """Map a raw trip dataframe into canonical Trip entities."""
        return [self.from_raw_row(record) for record in raw_trip_frame.to_dict(orient="records")]
