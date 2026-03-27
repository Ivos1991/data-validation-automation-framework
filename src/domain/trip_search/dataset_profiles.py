from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_DATASET_PROFILE = "small"
SUPPORTED_DATASET_PROFILES = ("small", "large", "gtfs")


@dataclass(frozen=True)
class DatasetProfileDefinition:
    """Resolved asset definition for one supported dataset profile."""

    name: str
    trip_dataset_source: str
    scenario_dataset_asset: str
    default_run_profile_asset: str
    default_run_suite_asset: str

    def resolve_scenario_dataset_path(self, repo_root: Path) -> Path:
        """Resolve the scenario dataset path for this profile."""
        return repo_root / self.scenario_dataset_asset

    def resolve_default_run_profile_path(self, repo_root: Path) -> Path:
        """Resolve the default run-profile path for this profile."""
        return repo_root / self.default_run_profile_asset

    def resolve_default_run_suite_path(self, repo_root: Path) -> Path:
        """Resolve the default suite path for this profile."""
        return repo_root / self.default_run_suite_asset


DATASET_PROFILE_DEFINITIONS = {
    "small": DatasetProfileDefinition(
        name="small",
        trip_dataset_source="csv:data/raw/sample_trips.csv",
        scenario_dataset_asset="data/raw/batch_trip_search_scenarios.csv",
        default_run_profile_asset="data/raw/default_trip_search_run_profile.json",
        default_run_suite_asset="data/raw/default_trip_search_run_suite.json",
    ),
    "large": DatasetProfileDefinition(
        name="large",
        trip_dataset_source="synthetic:large",
        scenario_dataset_asset="data/raw/large_batch_trip_search_scenarios.csv",
        default_run_profile_asset="data/raw/large_filters_trip_search_run_profile.json",
        default_run_suite_asset="data/raw/large_trip_search_run_suite.json",
    ),
    "gtfs": DatasetProfileDefinition(
        name="gtfs",
        trip_dataset_source="gtfs:data/raw/gtfs_sample",
        scenario_dataset_asset="data/raw/gtfs_batch_trip_search_scenarios.csv",
        default_run_profile_asset="data/raw/gtfs_default_trip_search_run_profile.json",
        default_run_suite_asset="data/raw/gtfs_trip_search_run_suite.json",
    ),
}


def normalize_dataset_profile(
    value: object,
    field_name: str = "dataset_profile",
    *,
    allow_none: bool = False,
) -> str | None:
    """Validate and normalize a dataset-profile value."""
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"Field '{field_name}' must not be blank")

    normalized_value = str(value).strip().lower()
    if not normalized_value:
        if allow_none:
            return None
        raise ValueError(f"Field '{field_name}' must not be blank")
    if normalized_value not in SUPPORTED_DATASET_PROFILES:
        supported_profiles = ", ".join(SUPPORTED_DATASET_PROFILES)
        raise ValueError(f"Field '{field_name}' must be one of: {supported_profiles}")
    return normalized_value


def get_dataset_profile_definition(profile_name: str) -> DatasetProfileDefinition:
    """Return the registered asset definition for a dataset profile."""
    normalized_profile = normalize_dataset_profile(profile_name, "dataset_profile")
    return DATASET_PROFILE_DEFINITIONS[normalized_profile]
