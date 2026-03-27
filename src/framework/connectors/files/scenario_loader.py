from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.domain.trip_search.search_models import TripSearchScenario
from src.framework.connectors.files.dataset_loader import DatasetLoader
from src.framework.utils.date_utils import parse_iso_date, to_iso_date
from src.framework.utils.numeric_utils import normalize_int
from src.validators.quality.trip_search_scenario_preflight_validator import (
    ScenarioPreflightResult,
    ScenarioPreflightValidationError,
    TripSearchScenarioPreflightValidator,
)


@dataclass(frozen=True)
class LoadedTripSearchScenarioDataset:
    """Normalized scenario dataset plus the preflight result used to accept it."""

    scenario_frame: pd.DataFrame
    scenarios: list[TripSearchScenario]
    preflight_result: ScenarioPreflightResult


class TripSearchScenarioLoader:
    """Load, normalize, and preflight-check external scenario CSV files."""

    REQUIRED_COLUMNS = ["scenario_id", "origin", "destination", "departure_date"]
    OPTIONAL_COLUMNS = ["carrier", "stops_count", "pack", "tag", "scenario_type"]

    def __init__(
        self,
        dataset_loader: DatasetLoader | None = None,
        preflight_validator: TripSearchScenarioPreflightValidator | None = None,
    ) -> None:
        self.dataset_loader = dataset_loader or DatasetLoader()
        self.preflight_validator = preflight_validator or TripSearchScenarioPreflightValidator()

    def load_csv(self, dataset_path: Path) -> LoadedTripSearchScenarioDataset:
        """Read a scenario CSV, normalize it, and return typed scenarios."""
        scenario_frame = self.dataset_loader.load_csv(dataset_path).copy()
        self._validate_schema(scenario_frame)
        normalized_frame = self._normalize_frame(scenario_frame)
        preflight_result = self.preflight_validator.validate(normalized_frame, scenario_frame)
        if not preflight_result.is_valid:
            raise ScenarioPreflightValidationError(
                "Scenario dataset failed preflight validation",
                preflight_result,
            )
        scenarios = [self._build_scenario(record) for record in normalized_frame.to_dict(orient="records")]
        return LoadedTripSearchScenarioDataset(
            scenario_frame=normalized_frame,
            scenarios=scenarios,
            preflight_result=preflight_result,
        )

    def _validate_schema(self, scenario_frame: pd.DataFrame) -> None:
        missing_columns = [column for column in self.REQUIRED_COLUMNS if column not in scenario_frame.columns]
        if missing_columns:
            raise ValueError(f"Missing required scenario columns: {', '.join(missing_columns)}")

    def _normalize_frame(self, scenario_frame: pd.DataFrame) -> pd.DataFrame:
        normalized_frame = scenario_frame.copy()
        for column_name in self.OPTIONAL_COLUMNS:
            if column_name not in normalized_frame.columns:
                normalized_frame[column_name] = None

        normalized_frame["scenario_id"] = normalized_frame["scenario_id"].map(self._require_non_blank_string("scenario_id"))
        normalized_frame["origin"] = normalized_frame["origin"].map(self._require_non_blank_string("origin")).str.upper()
        normalized_frame["destination"] = normalized_frame["destination"].map(self._require_non_blank_string("destination")).str.upper()
        normalized_frame["departure_date"] = normalized_frame["departure_date"].map(self._normalize_departure_date)
        normalized_frame["carrier"] = normalized_frame["carrier"].map(self._normalize_optional_string)
        normalized_frame["stops_count"] = normalized_frame["stops_count"].map(self._normalize_optional_stops_count)
        normalized_frame["pack"] = normalized_frame["pack"].map(self._normalize_optional_metadata)
        normalized_frame["tag"] = normalized_frame["tag"].map(self._normalize_optional_metadata)
        normalized_frame["scenario_type"] = normalized_frame["scenario_type"].map(self._normalize_optional_metadata)
        normalized_frame["carrier"] = normalized_frame["carrier"].astype(object).where(pd.notna(normalized_frame["carrier"]), None)
        normalized_frame["stops_count"] = normalized_frame["stops_count"].map(
            lambda value: None if value is None or pd.isna(value) else normalize_int(value)
        ).astype(object)
        for column_name in ["pack", "tag", "scenario_type"]:
            normalized_frame[column_name] = normalized_frame[column_name].astype(object).where(pd.notna(normalized_frame[column_name]), None)
        return normalized_frame[self.REQUIRED_COLUMNS + self.OPTIONAL_COLUMNS]

    def _build_scenario(self, record: dict[str, object]) -> TripSearchScenario:
        return TripSearchScenario(
            scenario_id=str(record["scenario_id"]),
            origin=str(record["origin"]),
            destination=str(record["destination"]),
            departure_date=str(record["departure_date"]),
            carrier=None if record["carrier"] is None or pd.isna(record["carrier"]) else str(record["carrier"]),
            stops_count=None if record["stops_count"] is None or pd.isna(record["stops_count"]) else normalize_int(record["stops_count"]),
            pack=None if record["pack"] is None or pd.isna(record["pack"]) else str(record["pack"]),
            tag=None if record["tag"] is None or pd.isna(record["tag"]) else str(record["tag"]),
            scenario_type=None if record["scenario_type"] is None or pd.isna(record["scenario_type"]) else str(record["scenario_type"]),
        )

    @staticmethod
    def _require_non_blank_string(field_name: str):
        def normalize(value: object) -> str:
            if value is None or pd.isna(value):
                raise ValueError(f"Scenario field '{field_name}' must not be blank")
            normalized_value = str(value).strip()
            if not normalized_value:
                raise ValueError(f"Scenario field '{field_name}' must not be blank")
            return normalized_value

        return normalize

    @staticmethod
    def _normalize_departure_date(value: object) -> str:
        if value is None or pd.isna(value):
            raise ValueError("Scenario field 'departure_date' must not be blank")
        try:
            return to_iso_date(parse_iso_date(str(value).strip()))
        except ValueError as error:
            raise ValueError(f"Scenario field 'departure_date' is invalid: {value}") from error

    @staticmethod
    def _normalize_optional_string(value: object) -> str | None:
        if value is None or pd.isna(value):
            return None
        normalized_value = str(value).strip()
        return normalized_value or None

    @staticmethod
    def _normalize_optional_metadata(value: object) -> str | None:
        if value is None or pd.isna(value):
            return None
        normalized_value = str(value).strip().lower()
        return normalized_value or None

    @staticmethod
    def _normalize_optional_stops_count(value: object) -> int | None:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, float):
            if not value.is_integer():
                raise ValueError(f"Scenario field 'stops_count' is invalid: {value}")
            return normalize_int(value)
        normalized_value = str(value).strip()
        if not normalized_value:
            return None
        try:
            if "." in normalized_value:
                numeric_value = float(normalized_value)
                if not numeric_value.is_integer():
                    raise ValueError
                return normalize_int(numeric_value)
            return normalize_int(normalized_value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"Scenario field 'stops_count' is invalid: {value}") from error
