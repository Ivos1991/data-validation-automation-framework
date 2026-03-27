from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


SYNTHETIC_TRIP_DATASET_COLUMNS = [
    "trip_id",
    "origin",
    "destination",
    "departure_date",
    "stops_count",
    "route_id",
    "carrier",
    "price_amount",
    "currency",
    "duration_minutes",
]


@dataclass(frozen=True)
class SyntheticTripDatasetSpec:
    """Deterministic shape definition for the larger synthetic trip dataset."""

    origins: tuple[str, ...] = ("NYC", "BOS", "WAS", "PHL", "CHI")
    departure_dates: tuple[str, ...] = ("2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04")
    carriers: tuple[str, ...] = ("AmRail", "BudgetBus", "SkyJet")
    stops_counts: tuple[int, ...] = (0, 1, 2)
    currency: str = "USD"


class SyntheticTripDatasetBuilder:
    """Build the deterministic large synthetic trip dataset used for regression coverage."""

    def __init__(self, spec: SyntheticTripDatasetSpec | None = None) -> None:
        self.spec = spec or SyntheticTripDatasetSpec()

    def build_large_dataframe(self) -> pd.DataFrame:
        """Build the larger deterministic trip dataframe."""
        rows: list[dict[str, object]] = []
        trip_index = 1
        for departure_date in self.spec.departure_dates:
            date_offset = int(departure_date[-2:]) - 1
            for origin in self.spec.origins:
                for destination in self.spec.origins:
                    if origin == destination:
                        continue
                    route_seed = self._route_seed(origin, destination)
                    route_id = f"{origin}-{destination}"
                    for carrier in self.spec.carriers:
                        for stops_count in self.spec.stops_counts:
                            rows.append(
                                {
                                    "trip_id": f"SYN-{trip_index:04d}",
                                    "origin": origin,
                                    "destination": destination,
                                    "departure_date": departure_date,
                                    "stops_count": stops_count,
                                    "route_id": route_id,
                                    "carrier": carrier,
                                    "price_amount": self._price_amount(route_seed, carrier, stops_count, date_offset),
                                    "currency": self.spec.currency,
                                    "duration_minutes": self._duration_minutes(route_seed, carrier, stops_count, date_offset),
                                }
                            )
                            trip_index += 1
        return pd.DataFrame(rows, columns=SYNTHETIC_TRIP_DATASET_COLUMNS)

    def write_csv(self, output_path: Path) -> Path:
        """Write the large synthetic dataset to CSV."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.build_large_dataframe().to_csv(output_path, index=False)
        return output_path

    def build_profile_frame(self, trip_frame: pd.DataFrame | None = None) -> pd.DataFrame:
        """Build a compact profile summary for reporting the generated dataset."""
        frame = trip_frame if trip_frame is not None else self.build_large_dataframe()
        return pd.DataFrame(
            [
                {
                    "dataset_name": "large_synthetic_trip_dataset",
                    "row_count": int(len(frame)),
                    "unique_origins": int(frame["origin"].nunique()),
                    "unique_destinations": int(frame["destination"].nunique()),
                    "unique_departure_dates": int(frame["departure_date"].nunique()),
                    "unique_carriers": int(frame["carrier"].nunique()),
                    "unique_stops_count_values": int(frame["stops_count"].nunique()),
                }
            ]
        )

    @staticmethod
    def _route_seed(origin: str, destination: str) -> int:
        return sum(ord(character) for character in f"{origin}{destination}") % 37

    @staticmethod
    def _price_amount(route_seed: int, carrier: str, stops_count: int, date_offset: int) -> float:
        carrier_price_offset = {
            "AmRail": 55.0,
            "BudgetBus": 20.0,
            "SkyJet": 95.0,
        }[carrier]
        stop_price_offset = {
            0: 18.0,
            1: 9.0,
            2: 0.0,
        }[stops_count]
        return round(45.0 + route_seed + carrier_price_offset + stop_price_offset + (date_offset * 2.0), 2)

    @staticmethod
    def _duration_minutes(route_seed: int, carrier: str, stops_count: int, date_offset: int) -> int:
        carrier_duration_offset = {
            "AmRail": 110,
            "BudgetBus": 180,
            "SkyJet": 60,
        }[carrier]
        return int(90 + (route_seed * 3) + carrier_duration_offset + (stops_count * 40) + (date_offset * 3))
