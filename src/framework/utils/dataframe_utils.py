from __future__ import annotations

import pandas as pd

from src.framework.utils.date_utils import to_iso_date
from src.framework.utils.numeric_utils import normalize_float, normalize_int


CANONICAL_TRIP_COLUMNS = [
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

AGGREGATE_SUMMARY_COLUMNS = [
    "result_count",
    "min_price",
    "max_price",
    "average_price",
]


def build_expected_trip_frame(raw_trip_frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize a raw trip dataframe into the canonical expected-result frame."""
    normalized_frame = raw_trip_frame.copy()
    normalized_frame["departure_date"] = normalized_frame["departure_date"].map(to_iso_date)
    normalized_frame["stops_count"] = normalized_frame["stops_count"].map(normalize_int)
    normalized_frame["price_amount"] = normalized_frame["price_amount"].map(normalize_float)
    normalized_frame["duration_minutes"] = normalized_frame["duration_minutes"].map(normalize_int)
    normalized_frame["origin"] = normalized_frame["origin"].str.strip().str.upper()
    normalized_frame["destination"] = normalized_frame["destination"].str.strip().str.upper()
    normalized_frame["route_id"] = normalized_frame["route_id"].str.strip().str.upper()
    normalized_frame["currency"] = normalized_frame["currency"].str.strip().str.upper()
    normalized_frame["carrier"] = normalized_frame["carrier"].str.strip()
    return normalized_frame[CANONICAL_TRIP_COLUMNS].sort_values("trip_id").reset_index(drop=True)


def filter_expected_trip_frame(
    expected_trip_frame: pd.DataFrame,
    origin: str,
    destination: str,
    departure_date: str,
    carrier: str | None = None,
    stops_count: int | None = None,
) -> pd.DataFrame:
    """Return the canonical expected subset for the supplied search filters."""
    mask = (
        (expected_trip_frame["origin"] == origin.strip().upper())
        & (expected_trip_frame["destination"] == destination.strip().upper())
        & (expected_trip_frame["departure_date"] == to_iso_date(departure_date))
    )
    if carrier:
        mask = mask & (expected_trip_frame["carrier"] == carrier.strip())
    if stops_count is not None:
        mask = mask & (expected_trip_frame["stops_count"] == normalize_int(stops_count))
    filtered = expected_trip_frame[mask]
    return filtered.sort_values("trip_id").reset_index(drop=True)


def build_aggregate_summary(trip_frame: pd.DataFrame) -> pd.DataFrame:
    """Build the aggregate summary frame used by aggregate validation."""
    if trip_frame.empty:
        return pd.DataFrame(
            [
                {
                    "result_count": 0,
                    "min_price": None,
                    "max_price": None,
                    "average_price": None,
                }
            ],
            columns=AGGREGATE_SUMMARY_COLUMNS,
        )
    summary = pd.DataFrame(
        [
            {
                "result_count": int(len(trip_frame)),
                "min_price": normalize_float(trip_frame["price_amount"].min()),
                "max_price": normalize_float(trip_frame["price_amount"].max()),
                "average_price": normalize_float(trip_frame["price_amount"].mean()),
            }
        ]
    )
    return summary[AGGREGATE_SUMMARY_COLUMNS]


def build_carrier_count_frame(trip_frame: pd.DataFrame) -> pd.DataFrame:
    """Build grouped carrier counts for aggregate comparison."""
    carrier_counts = (
        trip_frame.groupby("carrier", as_index=False)
        .size()
        .rename(columns={"size": "result_count"})
        .sort_values("carrier")
        .reset_index(drop=True)
    )
    return carrier_counts
