from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.domain.trip_search.dataset_profiles import (
    DEFAULT_DATASET_PROFILE,
    get_dataset_profile_definition,
    normalize_dataset_profile,
)
from src.domain.trip_search.search_models import TripSearchRunProfile, TripSearchRunSuite
from src.framework.config.config_manager import ConfigManager
from src.framework.connectors.files.dataset_loader import DatasetLoader
from src.framework.connectors.files.gtfs_loader import GtfsDatasetLoader
from src.framework.logging.logger import get_logger
from src.framework.connectors.files.synthetic_trip_dataset_builder import SyntheticTripDatasetBuilder
from src.framework.utils.dataframe_utils import build_expected_trip_frame
from src.transformers.gtfs_trip_transformer import GtfsTripTransformer
from src.transformers.trip_model_mapper import TripModelMapper

LOGGER = get_logger("trip_search.dataset_context_loader")


@dataclass(frozen=True)
class LoadedTripDatasetContext:
    """Resolved dataset assets plus normalized trip data for one execution context."""

    dataset_profile: str
    trip_dataset_source: str
    scenario_dataset_path: Path
    default_run_profile_path: Path
    default_run_suite_path: Path
    raw_trip_frame: pd.DataFrame
    normalized_trips: list
    expected_trip_frame: pd.DataFrame


class TripDatasetContextLoader:
    """Resolve dataset-profile assets and normalize them into canonical trip data."""

    def __init__(
        self,
        dataset_loader: DatasetLoader | None = None,
        synthetic_trip_dataset_builder: SyntheticTripDatasetBuilder | None = None,
        gtfs_dataset_loader: GtfsDatasetLoader | None = None,
        gtfs_trip_transformer: GtfsTripTransformer | None = None,
        trip_model_mapper: TripModelMapper | None = None,
    ) -> None:
        self.dataset_loader = dataset_loader or DatasetLoader()
        self.synthetic_trip_dataset_builder = synthetic_trip_dataset_builder or SyntheticTripDatasetBuilder()
        self.gtfs_dataset_loader = gtfs_dataset_loader or GtfsDatasetLoader(self.dataset_loader)
        self.gtfs_trip_transformer = gtfs_trip_transformer or GtfsTripTransformer()
        self.trip_model_mapper = trip_model_mapper or TripModelMapper()

    def load(
        self,
        config: ConfigManager,
        dataset_profile: str | None = None,
        run_profile: TripSearchRunProfile | None = None,
        run_suite: TripSearchRunSuite | None = None,
    ) -> LoadedTripDatasetContext:
        """Load the full trip dataset context for batch, run-profile, or suite execution."""
        repo_root = Path(__file__).resolve().parents[4]
        resolved_profile = self.resolve_dataset_profile(
            config=config,
            dataset_profile=dataset_profile,
            run_profile=run_profile,
            run_suite=run_suite,
        )
        profile_definition = get_dataset_profile_definition(resolved_profile)
        trip_dataset_source = config.trip_dataset_source if resolved_profile == config.dataset_profile else profile_definition.trip_dataset_source
        LOGGER.info("Resolved dataset profile '%s' with source '%s'", resolved_profile, trip_dataset_source)
        if resolved_profile == config.dataset_profile:
            scenario_dataset_path = config.scenario_dataset_path
            default_run_profile_path = config.run_profile_path
            default_run_suite_path = config.run_suite_path
        else:
            scenario_dataset_path = profile_definition.resolve_scenario_dataset_path(repo_root)
            default_run_profile_path = profile_definition.resolve_default_run_profile_path(repo_root)
            default_run_suite_path = profile_definition.resolve_default_run_suite_path(repo_root)
        self._ensure_existing_path(scenario_dataset_path, "scenario_dataset_path")
        self._ensure_existing_path(default_run_profile_path, "default_run_profile_path")
        self._ensure_existing_path(default_run_suite_path, "default_run_suite_path")
        raw_trip_frame = self._load_raw_trip_frame(config, resolved_profile, trip_dataset_source, repo_root)
        LOGGER.info("Loaded %s raw trip rows for dataset profile '%s'", len(raw_trip_frame), resolved_profile)
        normalized_trips = self.trip_model_mapper.from_dataframe(raw_trip_frame)
        expected_trip_frame = build_expected_trip_frame(raw_trip_frame)
        return LoadedTripDatasetContext(
            dataset_profile=resolved_profile,
            trip_dataset_source=trip_dataset_source,
            scenario_dataset_path=scenario_dataset_path,
            default_run_profile_path=default_run_profile_path,
            default_run_suite_path=default_run_suite_path,
            raw_trip_frame=raw_trip_frame,
            normalized_trips=normalized_trips,
            expected_trip_frame=expected_trip_frame,
        )

    def resolve_dataset_profile(
        self,
        config: ConfigManager,
        dataset_profile: str | None = None,
        run_profile: TripSearchRunProfile | None = None,
        run_suite: TripSearchRunSuite | None = None,
    ) -> str:
        """Resolve dataset-profile precedence across explicit input, run assets, and config."""
        if dataset_profile is not None:
            return normalize_dataset_profile(dataset_profile, "dataset_profile")
        if run_profile is not None and run_profile.dataset_profile is not None:
            return normalize_dataset_profile(run_profile.dataset_profile, "run_profile.dataset_profile")
        if run_suite is not None and run_suite.dataset_profile is not None:
            return normalize_dataset_profile(run_suite.dataset_profile, "run_suite.dataset_profile")
        return normalize_dataset_profile(config.dataset_profile, "config.dataset_profile") or DEFAULT_DATASET_PROFILE

    def _load_raw_trip_frame(
        self,
        config: ConfigManager,
        dataset_profile: str,
        trip_dataset_source: str,
        repo_root: Path,
    ) -> pd.DataFrame:
        """Load the raw canonical trip frame from CSV, synthetic, or GTFS sources."""
        if trip_dataset_source.startswith("csv:"):
            dataset_path = config.dataset_path if dataset_profile == config.dataset_profile and config.dataset_path is not None else repo_root / trip_dataset_source.removeprefix("csv:")
            LOGGER.info("Loading CSV-backed trip dataset from %s", dataset_path)
            return self.dataset_loader.load_csv(dataset_path)
        if trip_dataset_source == "synthetic:large":
            LOGGER.info("Building synthetic large trip dataset")
            return self.synthetic_trip_dataset_builder.build_large_dataframe()
        if trip_dataset_source.startswith("gtfs:"):
            gtfs_directory = repo_root / trip_dataset_source.removeprefix("gtfs:")
            LOGGER.info("Loading GTFS-backed trip dataset from %s", gtfs_directory)
            loaded_gtfs_dataset = self.gtfs_dataset_loader.load_directory(gtfs_directory)
            return self.gtfs_trip_transformer.transform(loaded_gtfs_dataset)
        raise ValueError(f"Unsupported trip dataset source for profile '{dataset_profile}': {trip_dataset_source}")

    @staticmethod
    def _ensure_existing_path(asset_path: Path, field_name: str) -> None:
        """Fail fast when a resolved profile asset does not exist."""
        if not asset_path.exists():
            raise FileNotFoundError(f"Resolved {field_name} does not exist: {asset_path}")
