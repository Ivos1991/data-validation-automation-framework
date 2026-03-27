from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.framework.utils.dataframe_utils import CANONICAL_TRIP_COLUMNS
from src.framework.utils.numeric_utils import values_match


@dataclass(frozen=True)
class ReconciliationResult:
    """Row-level reconciliation output for one expected/actual comparison."""

    is_match: bool
    missing_rows: pd.DataFrame
    unexpected_rows: pd.DataFrame
    mismatched_fields: pd.DataFrame
    expected_rows_count: int
    actual_rows_count: int


class TripReconciliationValidator:
    """Compare canonical trip rows and report missing, unexpected, and mismatched data."""

    def reconcile(
        self,
        expected_frame: pd.DataFrame,
        actual_frame: pd.DataFrame,
    ) -> ReconciliationResult:
        """Reconcile one expected trip frame against one actual trip frame."""
        expected = expected_frame[CANONICAL_TRIP_COLUMNS].sort_values("trip_id").reset_index(drop=True)
        actual = actual_frame[CANONICAL_TRIP_COLUMNS].sort_values("trip_id").reset_index(drop=True)

        missing_rows = expected[~expected["trip_id"].isin(actual["trip_id"])].reset_index(drop=True)
        unexpected_rows = actual[~actual["trip_id"].isin(expected["trip_id"])].reset_index(drop=True)

        shared_expected = expected[expected["trip_id"].isin(actual["trip_id"])].set_index("trip_id")
        shared_actual = actual[actual["trip_id"].isin(expected["trip_id"])].set_index("trip_id")

        mismatches: list[dict] = []
        for trip_id in shared_expected.index.intersection(shared_actual.index):
            expected_row = shared_expected.loc[trip_id]
            actual_row = shared_actual.loc[trip_id]
            for field_name in CANONICAL_TRIP_COLUMNS:
                if field_name == "trip_id":
                    continue
                values_differ = expected_row[field_name] != actual_row[field_name]
                if field_name == "price_amount":
                    values_differ = not values_match(float(expected_row[field_name]), float(actual_row[field_name]), 0.0001)
                if values_differ:
                    mismatches.append(
                        {
                            "trip_id": trip_id,
                            "field_name": field_name,
                            "expected_value": expected_row[field_name],
                            "actual_value": actual_row[field_name],
                        }
                    )

        mismatched_fields = pd.DataFrame(
            mismatches,
            columns=["trip_id", "field_name", "expected_value", "actual_value"],
        )

        return ReconciliationResult(
            is_match=missing_rows.empty and unexpected_rows.empty and mismatched_fields.empty,
            missing_rows=missing_rows,
            unexpected_rows=unexpected_rows,
            mismatched_fields=mismatched_fields,
            expected_rows_count=len(expected),
            actual_rows_count=len(actual),
        )
