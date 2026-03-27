from __future__ import annotations

from pathlib import Path

import pandas as pd


class DatasetLoader:
    """Minimal CSV loader used by dataset, scenario, and GTFS file connectors."""

    def load_csv(self, dataset_path: Path) -> pd.DataFrame:
        """Load a CSV file into a dataframe."""
        return pd.read_csv(dataset_path)
