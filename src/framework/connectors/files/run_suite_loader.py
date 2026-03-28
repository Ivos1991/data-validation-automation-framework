from __future__ import annotations

import json
from pathlib import Path

from src.domain.trip_search.dataset_profiles import normalize_dataset_profile
from src.domain.trip_search.search_models import (
    TripSearchRunProfileReference,
    TripSearchRunSuite,
    TripSearchRunSuitePolicy,
)
from src.framework.logging.logger import get_logger

LOGGER = get_logger("trip_search.run_suite_loader")


class TripSearchRunSuiteLoader:
    """Load and validate external suite-definition JSON files."""

    REQUIRED_FIELDS = ["suite_id", "suite_label", "run_profiles"]

    def load_json(self, suite_path: Path) -> TripSearchRunSuite:
        """Read a run suite from disk and return the typed model."""
        LOGGER.info("Loading run suite from %s", suite_path)
        with suite_path.open("r", encoding="utf-8") as suite_file:
            raw_suite = json.load(suite_file)
        return self._build_suite(raw_suite, suite_path.parent)

    def _build_suite(self, raw_suite: dict, suite_dir: Path) -> TripSearchRunSuite:
        self._validate_schema(raw_suite)
        run_profiles = self._build_run_profile_references(raw_suite["run_profiles"], suite_dir)
        return TripSearchRunSuite(
            suite_id=self._normalize_required_field(raw_suite["suite_id"], "suite_id"),
            suite_label=self._normalize_required_field(raw_suite["suite_label"], "suite_label"),
            description=self._normalize_optional_text(raw_suite.get("description")),
            dataset_profile=normalize_dataset_profile(raw_suite.get("dataset_profile"), "dataset_profile", allow_none=True),
            run_profiles=run_profiles,
            policy=self._build_policy(raw_suite.get("policy", {})),
        )

    def _validate_schema(self, raw_suite: dict) -> None:
        missing_fields = [field_name for field_name in self.REQUIRED_FIELDS if field_name not in raw_suite]
        if missing_fields:
            raise ValueError(f"Missing required run-suite fields: {', '.join(missing_fields)}")
        if not isinstance(raw_suite["run_profiles"], list) or not raw_suite["run_profiles"]:
            raise ValueError("Run-suite field 'run_profiles' must be a non-empty list")

    def _build_run_profile_references(self, raw_run_profiles: list[object], suite_dir: Path) -> list[TripSearchRunProfileReference]:
        run_profile_references: list[TripSearchRunProfileReference] = []
        for raw_reference in raw_run_profiles:
            if not isinstance(raw_reference, dict) or "profile_path" not in raw_reference:
                raise ValueError("Each run-suite profile entry must contain 'profile_path'")
            normalized_path = self._normalize_required_field(raw_reference["profile_path"], "profile_path")
            run_profile_references.append(
                TripSearchRunProfileReference(profile_path=(suite_dir / normalized_path).resolve())
            )
        return run_profile_references

    def _build_policy(self, raw_policy: object) -> TripSearchRunSuitePolicy:
        if raw_policy is None:
            return TripSearchRunSuitePolicy()
        if not isinstance(raw_policy, dict):
            raise ValueError("Run-suite field 'policy' must be an object when provided")

        stop_on_first_failed_run = self._normalize_bool(raw_policy.get("stop_on_first_failed_run", False), "stop_on_first_failed_run")
        continue_on_failure = self._normalize_bool(raw_policy.get("continue_on_failure", True), "continue_on_failure")
        minimum_passed_scenarios = self._normalize_optional_int(raw_policy.get("minimum_passed_scenarios"), "minimum_passed_scenarios")
        minimum_pass_rate = self._normalize_optional_float(raw_policy.get("minimum_pass_rate"), "minimum_pass_rate")
        maximum_failed_runs = self._normalize_optional_int(raw_policy.get("maximum_failed_runs"), "maximum_failed_runs")

        if minimum_pass_rate is not None and not 0 <= minimum_pass_rate <= 1:
            raise ValueError("Run-suite field 'minimum_pass_rate' must be between 0 and 1")
        if minimum_passed_scenarios is not None and minimum_passed_scenarios < 0:
            raise ValueError("Run-suite field 'minimum_passed_scenarios' must be zero or greater")
        if maximum_failed_runs is not None and maximum_failed_runs < 0:
            raise ValueError("Run-suite field 'maximum_failed_runs' must be zero or greater")

        return TripSearchRunSuitePolicy(
            stop_on_first_failed_run=stop_on_first_failed_run,
            continue_on_failure=continue_on_failure,
            minimum_passed_scenarios=minimum_passed_scenarios,
            minimum_pass_rate=minimum_pass_rate,
            maximum_failed_runs=maximum_failed_runs,
        )

    @staticmethod
    def _normalize_required_field(value: object, field_name: str) -> str:
        if value is None:
            raise ValueError(f"Run-suite field '{field_name}' must not be blank")
        normalized_value = str(value).strip()
        if not normalized_value:
            raise ValueError(f"Run-suite field '{field_name}' must not be blank")
        return normalized_value

    @staticmethod
    def _normalize_optional_text(value: object) -> str | None:
        if value is None:
            return None
        normalized_value = str(value).strip()
        return normalized_value or None

    @staticmethod
    def _normalize_bool(value: object, field_name: str) -> bool:
        if isinstance(value, bool):
            return value
        raise ValueError(f"Run-suite field '{field_name}' must be a boolean")

    @staticmethod
    def _normalize_optional_int(value: object, field_name: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError(f"Run-suite field '{field_name}' must be an integer")
        try:
            return int(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"Run-suite field '{field_name}' must be an integer") from error

    @staticmethod
    def _normalize_optional_float(value: object, field_name: str) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"Run-suite field '{field_name}' must be numeric") from error
