from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.framework.utils.dataframe_utils import AGGREGATE_SUMMARY_COLUMNS
from src.framework.utils.numeric_utils import values_match


@dataclass(frozen=True)
class AggregateComparisonResult:
    """Aggregate validation output for expected versus actual result sets."""

    is_match: bool
    summary_mismatches: pd.DataFrame
    carrier_count_mismatches: pd.DataFrame
    expected_summary: pd.DataFrame
    actual_summary: pd.DataFrame
    expected_carrier_counts: pd.DataFrame
    actual_carrier_counts: pd.DataFrame


class TripAggregateValidator:
    """Compare aggregate summaries and grouped carrier counts."""

    def __init__(self, numeric_tolerance: float) -> None:
        self.numeric_tolerance = numeric_tolerance

    def validate(
        self,
        expected_summary: pd.DataFrame,
        actual_summary: pd.DataFrame,
        expected_carrier_counts: pd.DataFrame,
        actual_carrier_counts: pd.DataFrame,
    ) -> AggregateComparisonResult:
        """Return the aggregate comparison result for one expected/actual pair."""
        summary_mismatches = self._build_summary_mismatches(expected_summary, actual_summary)
        carrier_count_mismatches = self._build_carrier_count_mismatches(expected_carrier_counts, actual_carrier_counts)
        return AggregateComparisonResult(
            is_match=summary_mismatches.empty and carrier_count_mismatches.empty,
            summary_mismatches=summary_mismatches,
            carrier_count_mismatches=carrier_count_mismatches,
            expected_summary=expected_summary,
            actual_summary=actual_summary,
            expected_carrier_counts=expected_carrier_counts,
            actual_carrier_counts=actual_carrier_counts,
        )

    def _build_summary_mismatches(self, expected_summary: pd.DataFrame, actual_summary: pd.DataFrame) -> pd.DataFrame:
        expected_row = expected_summary.iloc[0]
        actual_row = actual_summary.iloc[0]
        mismatches: list[dict] = []
        for field_name in AGGREGATE_SUMMARY_COLUMNS:
            expected_value = expected_row[field_name]
            actual_value = actual_row[field_name]
            values_differ = expected_value != actual_value
            if field_name in {"min_price", "max_price", "average_price"}:
                both_missing = pd.isna(expected_value) and pd.isna(actual_value)
                if both_missing:
                    values_differ = False
                elif pd.isna(expected_value) or pd.isna(actual_value):
                    values_differ = True
                else:
                    values_differ = not values_match(float(expected_value), float(actual_value), self.numeric_tolerance)
            if values_differ:
                mismatches.append(
                    {
                        "field_name": field_name,
                        "expected_value": expected_value,
                        "actual_value": actual_value,
                    }
                )
        return pd.DataFrame(mismatches, columns=["field_name", "expected_value", "actual_value"])

    @staticmethod
    def _build_carrier_count_mismatches(
        expected_carrier_counts: pd.DataFrame,
        actual_carrier_counts: pd.DataFrame,
    ) -> pd.DataFrame:
        merged = expected_carrier_counts.merge(
            actual_carrier_counts,
            on="carrier",
            how="outer",
            suffixes=("_expected", "_actual"),
        ).fillna(0)
        mismatches = merged[merged["result_count_expected"] != merged["result_count_actual"]].copy()
        if mismatches.empty:
            return pd.DataFrame(columns=["carrier", "expected_count", "actual_count"])
        mismatches = mismatches.rename(
            columns={
                "result_count_expected": "expected_count",
                "result_count_actual": "actual_count",
            }
        )
        mismatches["expected_count"] = mismatches["expected_count"].astype(int)
        mismatches["actual_count"] = mismatches["actual_count"].astype(int)
        return mismatches[["carrier", "expected_count", "actual_count"]].sort_values("carrier").reset_index(drop=True)
