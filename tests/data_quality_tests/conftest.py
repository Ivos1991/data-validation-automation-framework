import pandas as pd
import pytest

from src.validators.quality.trip_data_quality_validator import TripDataQualityValidator


@pytest.fixture()
def source_dataset_with_duplicate_trip_id(raw_trip_frame: pd.DataFrame) -> pd.DataFrame:
    duplicate_row = raw_trip_frame.iloc[[0]].copy()
    duplicate_row["departure_date"] = "2026-04-03"
    return pd.concat([raw_trip_frame, duplicate_row], ignore_index=True)


@pytest.fixture()
def source_dataset_with_invalid_departure_date(raw_trip_frame: pd.DataFrame) -> pd.DataFrame:
    invalid_frame = raw_trip_frame.copy()
    invalid_frame.loc[0, "departure_date"] = "2026-02-30"
    return invalid_frame


@pytest.fixture()
def trip_data_quality_validator() -> TripDataQualityValidator:
    return TripDataQualityValidator()
