from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.framework.logging.logger import get_logger

LOGGER = get_logger("trip_search.dataset_loader")


class DatasetLoader:
    """Minimal CSV loader used by dataset, scenario, and GTFS file connectors."""

    def load_csv(self, dataset_path: Path) -> pd.DataFrame:
        """Load a CSV file into a dataframe."""
        LOGGER.info("Loading CSV dataset from %s", dataset_path)
        return pd.read_csv(dataset_path)
