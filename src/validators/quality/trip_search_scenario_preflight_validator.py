from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


SCENARIO_PRECHECK_SUMMARY_COLUMNS = [
    "scenario_count",
    "issue_count",
    "is_valid",
]

SCENARIO_PRECHECK_ISSUE_COLUMNS = [
    "scenario_id",
    "issue_code",
    "issue_message",
]


@dataclass(frozen=True)
class ScenarioPreflightResult:
    """Scenario-dataset preflight output used before batch execution."""

    is_valid: bool
    summary_frame: pd.DataFrame
    issues_frame: pd.DataFrame


class ScenarioPreflightValidationError(ValueError):
    """Raised when an external scenario dataset fails preflight validation."""

    def __init__(self, message: str, preflight_result: ScenarioPreflightResult) -> None:
        super().__init__(message)
        self.preflight_result = preflight_result


class TripSearchScenarioPreflightValidator:
    """Validate external scenario datasets before any batch execution starts."""

    def validate(self, normalized_scenario_frame: pd.DataFrame, raw_scenario_frame: pd.DataFrame) -> ScenarioPreflightResult:
        """Return structured preflight issues for a normalized scenario dataset."""
        issues: list[dict[str, object]] = []
        issues.extend(self._find_duplicate_scenario_ids(normalized_scenario_frame))
        issues.extend(self._find_duplicate_logical_scenarios(normalized_scenario_frame))
        issues.extend(self._find_invalid_stops_count(normalized_scenario_frame))
        issues.extend(self._find_contradictory_routes(normalized_scenario_frame))
        issues.extend(self._find_non_normalized_optional_filters(raw_scenario_frame, normalized_scenario_frame))

        issues_frame = pd.DataFrame(issues, columns=SCENARIO_PRECHECK_ISSUE_COLUMNS)
        summary_frame = pd.DataFrame(
            [
                {
                    "scenario_count": int(len(normalized_scenario_frame)),
                    "issue_count": int(len(issues_frame)),
                    "is_valid": issues_frame.empty,
                }
            ],
            columns=SCENARIO_PRECHECK_SUMMARY_COLUMNS,
        )
        return ScenarioPreflightResult(
            is_valid=issues_frame.empty,
            summary_frame=summary_frame,
            issues_frame=issues_frame,
        )

    @staticmethod
    def _find_duplicate_scenario_ids(normalized_scenario_frame: pd.DataFrame) -> list[dict[str, object]]:
        duplicate_rows = normalized_scenario_frame[normalized_scenario_frame["scenario_id"].duplicated(keep=False)]
        return [
            {
                "scenario_id": row["scenario_id"],
                "issue_code": "duplicate_scenario_id",
                "issue_message": f"Scenario id '{row['scenario_id']}' is duplicated",
            }
            for _, row in duplicate_rows.iterrows()
        ]

    @staticmethod
    def _find_duplicate_logical_scenarios(normalized_scenario_frame: pd.DataFrame) -> list[dict[str, object]]:
        logical_key_columns = ["origin", "destination", "departure_date", "carrier", "stops_count"]
        logical_key_frame = normalized_scenario_frame[["scenario_id", *logical_key_columns]].copy()
        logical_key_frame["carrier"] = logical_key_frame["carrier"].fillna("<none>")
        logical_key_frame["stops_count"] = logical_key_frame["stops_count"].fillna(-1)

        duplicate_rows = logical_key_frame[logical_key_frame.duplicated(subset=logical_key_columns, keep=False)]
        issues: list[dict[str, object]] = []
        for _, row in duplicate_rows.iterrows():
            issues.append(
                {
                    "scenario_id": row["scenario_id"],
                    "issue_code": "duplicate_logical_scenario",
                    "issue_message": (
                        "Scenario duplicates another logical scenario for the same "
                        f"origin/destination/date/carrier/stops_count combination"
                    ),
                }
            )
        return issues

    @staticmethod
    def _find_invalid_stops_count(normalized_scenario_frame: pd.DataFrame) -> list[dict[str, object]]:
        stops_frame = normalized_scenario_frame[normalized_scenario_frame["stops_count"].notna()].copy()
        if stops_frame.empty:
            return []
        stops_frame["stops_count"] = stops_frame["stops_count"].map(int)
        invalid_rows = stops_frame[stops_frame["stops_count"] < 0]
        return [
            {
                "scenario_id": row["scenario_id"],
                "issue_code": "invalid_stops_count",
                "issue_message": f"stops_count must be zero or greater, got {row['stops_count']}",
            }
            for _, row in invalid_rows.iterrows()
        ]

    @staticmethod
    def _find_contradictory_routes(normalized_scenario_frame: pd.DataFrame) -> list[dict[str, object]]:
        contradictory_rows = normalized_scenario_frame[
            normalized_scenario_frame["origin"] == normalized_scenario_frame["destination"]
        ]
        return [
            {
                "scenario_id": row["scenario_id"],
                "issue_code": "contradictory_route",
                "issue_message": "origin and destination must differ for a trip-search scenario",
            }
            for _, row in contradictory_rows.iterrows()
        ]

    @staticmethod
    def _find_non_normalized_optional_filters(
        raw_scenario_frame: pd.DataFrame,
        normalized_scenario_frame: pd.DataFrame,
    ) -> list[dict[str, object]]:
        issues: list[dict[str, object]] = []
        if "carrier" not in raw_scenario_frame.columns:
            return issues

        for row_index, raw_row in raw_scenario_frame.iterrows():
            scenario_id = normalized_scenario_frame.iloc[row_index]["scenario_id"]
            raw_carrier = raw_row.get("carrier")
            normalized_carrier = normalized_scenario_frame.iloc[row_index]["carrier"]

            if raw_carrier is None or pd.isna(raw_carrier):
                continue

            trimmed_carrier = str(raw_carrier).strip()
            if not trimmed_carrier:
                issues.append(
                    {
                        "scenario_id": scenario_id,
                        "issue_code": "blank_optional_carrier",
                        "issue_message": "carrier was provided but is blank after trimming",
                    }
                )
                continue

            if trimmed_carrier != raw_carrier or normalized_carrier != trimmed_carrier:
                issues.append(
                    {
                        "scenario_id": scenario_id,
                        "issue_code": "non_normalized_carrier",
                        "issue_message": "carrier must already be normalized without surrounding whitespace",
                    }
                )

        return issues
