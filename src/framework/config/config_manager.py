from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from src.domain.trip_search.dataset_profiles import (
    DEFAULT_DATASET_PROFILE,
    get_dataset_profile_definition,
    normalize_dataset_profile,
)


@dataclass(frozen=True)
class ConfigManager:
    """Environment-backed configuration for validation execution."""

    dataset_profile: str
    trip_dataset_source: str
    dataset_path: Path | None
    scenario_dataset_path: Path
    run_profile_path: Path
    run_suite_path: Path
    report_export_dir: Path | None
    sqlite_db_path: str
    search_api_mode: str
    numeric_tolerance: float
    attach_allure_artifacts: bool

    @classmethod
    def from_env(cls) -> "ConfigManager":
        """Load configuration from environment variables and dataset defaults."""
        load_dotenv()
        repo_root = Path(__file__).resolve().parents[3]
        dataset_profile = normalize_dataset_profile(os.getenv("TRIP_DATASET_PROFILE", DEFAULT_DATASET_PROFILE), "TRIP_DATASET_PROFILE")
        profile_definition = get_dataset_profile_definition(dataset_profile)
        trip_dataset_source_env = os.getenv("TRIP_DATASET_SOURCE", "").strip()
        trip_dataset_source = trip_dataset_source_env or profile_definition.trip_dataset_source
        dataset_path_env = os.getenv("TRIP_DATASET_PATH", "").strip()
        dataset_path = (repo_root / dataset_path_env) if dataset_path_env else cls._path_from_dataset_source(repo_root, trip_dataset_source)
        scenario_dataset_path = repo_root / os.getenv("TRIP_BATCH_SCENARIOS_PATH", profile_definition.scenario_dataset_asset)
        run_profile_path = repo_root / os.getenv("TRIP_RUN_PROFILE_PATH", profile_definition.default_run_profile_asset)
        run_suite_path = repo_root / os.getenv("TRIP_RUN_SUITE_PATH", profile_definition.default_run_suite_asset)
        report_export_dir_env = os.getenv("TRIP_REPORT_EXPORT_DIR", "").strip()
        return cls(
            dataset_profile=dataset_profile,
            trip_dataset_source=trip_dataset_source,
            dataset_path=dataset_path,
            scenario_dataset_path=scenario_dataset_path,
            run_profile_path=run_profile_path,
            run_suite_path=run_suite_path,
            report_export_dir=(repo_root / report_export_dir_env) if report_export_dir_env else None,
            sqlite_db_path=os.getenv("SQLITE_DB_PATH", ":memory:"),
            search_api_mode=os.getenv("SEARCH_API_MODE", "sqlite"),
            numeric_tolerance=float(os.getenv("NUMERIC_TOLERANCE", "0.0001")),
            attach_allure_artifacts=os.getenv("ATTACH_ALLURE_ARTIFACTS", "true").lower() == "true",
        )

    @staticmethod
    def _path_from_dataset_source(repo_root: Path, trip_dataset_source: str) -> Path | None:
        """Resolve a local CSV path when the dataset source points at a managed file."""
        if trip_dataset_source.startswith("csv:"):
            return repo_root / trip_dataset_source.removeprefix("csv:")
        return None
