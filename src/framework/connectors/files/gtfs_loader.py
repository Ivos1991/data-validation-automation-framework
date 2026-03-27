from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.framework.connectors.files.dataset_loader import DatasetLoader


@dataclass(frozen=True)
class LoadedGtfsDataset:
    """Loaded GTFS subset required by the narrow trip-derivation flow."""

    agency_frame: pd.DataFrame
    routes_frame: pd.DataFrame
    trips_frame: pd.DataFrame
    calendar_frame: pd.DataFrame
    stop_times_frame: pd.DataFrame
    stops_frame: pd.DataFrame
    calendar_dates_frame: pd.DataFrame
    fare_attributes_frame: pd.DataFrame
    fare_rules_frame: pd.DataFrame


class GtfsDatasetLoader:
    """Load the supported GTFS subset from a local directory."""

    REQUIRED_FILES = {
        "agency_frame": "agency.txt",
        "routes_frame": "routes.txt",
        "trips_frame": "trips.txt",
        "calendar_frame": "calendar.txt",
        "stop_times_frame": "stop_times.txt",
        "stops_frame": "stops.txt",
        "calendar_dates_frame": "calendar_dates.txt",
        "fare_attributes_frame": "fare_attributes.txt",
        "fare_rules_frame": "fare_rules.txt",
    }

    def __init__(self, dataset_loader: DatasetLoader | None = None) -> None:
        self.dataset_loader = dataset_loader or DatasetLoader()

    def load_directory(self, gtfs_directory: Path) -> LoadedGtfsDataset:
        """Read the supported GTFS files from a directory and return typed frames."""
        if not gtfs_directory.exists() or not gtfs_directory.is_dir():
            raise FileNotFoundError(f"GTFS dataset directory does not exist: {gtfs_directory}")

        loaded_frames: dict[str, pd.DataFrame] = {}
        for field_name, file_name in self.REQUIRED_FILES.items():
            file_path = gtfs_directory / file_name
            if not file_path.exists():
                raise FileNotFoundError(f"Missing required GTFS file: {file_name}")
            loaded_frames[field_name] = self.dataset_loader.load_csv(file_path)

        return LoadedGtfsDataset(**loaded_frames)
