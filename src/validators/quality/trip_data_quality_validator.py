from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.framework.utils.date_utils import parse_iso_date


@dataclass(frozen=True)
class DataQualityResult:
    """Dataset-quality output for canonical trip source data."""

    is_valid: bool
    duplicate_trip_ids: pd.DataFrame
    invalid_departure_dates: pd.DataFrame


class TripDataQualityValidator:
    """Validate lightweight quality constraints on canonical trip datasets."""

    def validate(self, raw_trip_frame: pd.DataFrame) -> DataQualityResult:
        """Return duplicate-id and invalid-date issues for a raw trip dataset."""
        duplicate_trip_ids = raw_trip_frame[
            raw_trip_frame.duplicated(subset=["trip_id"], keep=False)
        ].sort_values("trip_id")

        invalid_date_mask = raw_trip_frame["departure_date"].apply(self._is_invalid_date)
        invalid_departure_dates = raw_trip_frame[invalid_date_mask]

        return DataQualityResult(
            is_valid=duplicate_trip_ids.empty and invalid_departure_dates.empty,
            duplicate_trip_ids=duplicate_trip_ids.reset_index(drop=True),
            invalid_departure_dates=invalid_departure_dates.reset_index(drop=True),
        )

    @staticmethod
    def _is_invalid_date(raw_value: str) -> bool:
        try:
            parse_iso_date(raw_value)
        except ValueError:
            return True
        return False
