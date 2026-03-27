from __future__ import annotations

import json
from pathlib import Path

from src.domain.trip_search.dataset_profiles import normalize_dataset_profile
from src.domain.trip_search.search_models import TripSearchRunProfile


class TripSearchRunProfileLoader:
    """Load and validate external run-profile JSON files."""

    REQUIRED_FIELDS = ["run_id", "run_label"]
    OPTIONAL_FIELDS = ["selected_pack", "selected_tag", "selected_scenario_type", "dataset_profile", "description"]

    def load_json(self, profile_path: Path) -> TripSearchRunProfile:
        """Read a run profile from disk and return the typed model."""
        with profile_path.open("r", encoding="utf-8") as profile_file:
            raw_profile = json.load(profile_file)
        return self._build_profile(raw_profile)

    def _build_profile(self, raw_profile: dict) -> TripSearchRunProfile:
        self._validate_schema(raw_profile)
        return TripSearchRunProfile(
            run_id=self._normalize_required_field(raw_profile["run_id"], "run_id"),
            run_label=self._normalize_required_field(raw_profile["run_label"], "run_label"),
            selected_pack=self._normalize_optional_selector(raw_profile.get("selected_pack")),
            selected_tag=self._normalize_optional_selector(raw_profile.get("selected_tag")),
            selected_scenario_type=self._normalize_optional_selector(raw_profile.get("selected_scenario_type")),
            dataset_profile=normalize_dataset_profile(raw_profile.get("dataset_profile"), "dataset_profile", allow_none=True),
            description=self._normalize_optional_text(raw_profile.get("description")),
        )

    def _validate_schema(self, raw_profile: dict) -> None:
        missing_fields = [field_name for field_name in self.REQUIRED_FIELDS if field_name not in raw_profile]
        if missing_fields:
            raise ValueError(f"Missing required run-profile fields: {', '.join(missing_fields)}")

    @staticmethod
    def _normalize_required_field(value: object, field_name: str) -> str:
        if value is None:
            raise ValueError(f"Run-profile field '{field_name}' must not be blank")
        normalized_value = str(value).strip()
        if not normalized_value:
            raise ValueError(f"Run-profile field '{field_name}' must not be blank")
        return normalized_value

    @staticmethod
    def _normalize_optional_selector(value: object) -> str | None:
        if value is None:
            return None
        normalized_value = str(value).strip().lower()
        return normalized_value or None

    @staticmethod
    def _normalize_optional_text(value: object) -> str | None:
        if value is None:
            return None
        normalized_value = str(value).strip()
        return normalized_value or None
