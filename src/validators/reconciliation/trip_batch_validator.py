from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import pandas as pd

from src.domain.trip_search.dataset_profiles import DEFAULT_DATASET_PROFILE, get_dataset_profile_definition
from src.domain.trip_search.search_models import TripSearchRunProfile, TripSearchScenario, TripSearchScenarioSelection
from src.domain.trip_search.search_scenario_selector import TripSearchScenarioSelector
from src.domain.trip_search.search_service import search_by_route_and_departure_date
from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.domain.trip_search.search_service_request import SearchServiceRequest
from src.framework.utils.dataframe_utils import (
    CANONICAL_TRIP_COLUMNS,
    build_aggregate_summary,
    build_carrier_count_frame,
    filter_expected_trip_frame,
)
from src.validators.aggregate.trip_aggregate_validator import AggregateComparisonResult, TripAggregateValidator
from src.validators.quality.trip_search_scenario_preflight_validator import ScenarioPreflightResult
from src.validators.reconciliation.trip_reconciliation_validator import ReconciliationResult, TripReconciliationValidator


RUN_SUMMARY_COLUMNS = [
    "run_id",
    "run_label",
    "dataset_profile",
    "scenario_dataset_asset",
    "selected_pack",
    "selected_tag",
    "selected_scenario_type",
    "total_scenarios",
    "passed_scenarios",
    "failed_scenarios",
    "preflight_failed_scenarios",
    "execution_failed_scenarios",
    "aggregate_match_count",
    "row_reconciliation_failure_count",
    "filter_failure_count",
    "duration_ms",
]

ISSUE_CATEGORY_COLUMNS = [
    "issue_category",
    "issue_count",
]

PACK_SUMMARY_COLUMNS = [
    "pack",
    "total_scenarios",
    "passed_scenarios",
    "failed_scenarios",
]


@dataclass(frozen=True)
class BatchScenarioResult:
    """Detailed validation output for a single executed scenario."""

    scenario: TripSearchScenario
    expected_trip_frame: pd.DataFrame
    actual_trip_frame: pd.DataFrame
    reconciliation_result: ReconciliationResult
    aggregate_result: AggregateComparisonResult
    duration_ms: int

    @property
    def is_pass(self) -> bool:
        """Return whether both row and aggregate validation passed."""
        return self.reconciliation_result.is_match and self.aggregate_result.is_match

    @property
    def issue_categories(self) -> list[str]:
        """Return the issue categories triggered by this scenario result."""
        issue_categories: list[str] = []
        if not self.reconciliation_result.is_match:
            issue_categories.append("row_reconciliation")
        if not self.aggregate_result.is_match:
            issue_categories.append("aggregate_mismatch")
        if self._has_filter_failure():
            issue_categories.append("filter_correctness")
        return issue_categories

    def _has_filter_failure(self) -> bool:
        if self.scenario.carrier is None and self.scenario.stops_count is None:
            return False

        violation_mask = pd.Series(False, index=self.actual_trip_frame.index, dtype=bool)
        if self.scenario.carrier is not None:
            violation_mask = violation_mask | (self.actual_trip_frame["carrier"] != self.scenario.carrier)
        if self.scenario.stops_count is not None:
            violation_mask = violation_mask | (self.actual_trip_frame["stops_count"] != self.scenario.stops_count)
        return bool(violation_mask.any())


@dataclass(frozen=True)
class BatchValidationResult:
    """Batch validation output including per-scenario and run-level summaries."""

    scenario_results: list[BatchScenarioResult]
    summary_frame: pd.DataFrame
    run_summary_frame: pd.DataFrame
    issue_category_frame: pd.DataFrame
    pack_summary_frame: pd.DataFrame


class TripSearchBatchValidator:
    """Execute row-level and aggregate validation across many scenarios."""

    def __init__(self, service_api: SearchServiceAPI, numeric_tolerance: float) -> None:
        self.service_api = service_api
        self.reconciliation_validator = TripReconciliationValidator()
        self.aggregate_validator = TripAggregateValidator(numeric_tolerance)
        self.scenario_selector = TripSearchScenarioSelector()

    def validate(
        self,
        scenarios: list[TripSearchScenario],
        expected_trip_frame: pd.DataFrame,
        selection: TripSearchScenarioSelection | None = None,
        run_profile: TripSearchRunProfile | None = None,
        dataset_profile: str = DEFAULT_DATASET_PROFILE,
        scenario_dataset_asset: str = "unknown",
    ) -> BatchValidationResult:
        """Validate a scenario subset and return scenario-level plus run-level rollups."""
        start_time = perf_counter()
        resolved_scenario_dataset_asset = self._resolve_scenario_dataset_asset(dataset_profile, scenario_dataset_asset)
        effective_selection = run_profile.to_selection() if run_profile is not None else selection
        selected_scenarios = self.scenario_selector.select(scenarios, effective_selection)
        scenario_results = [self._validate_scenario(scenario, expected_trip_frame) for scenario in selected_scenarios]
        summary_frame = self._build_summary_frame(scenario_results)
        duration_ms = int((perf_counter() - start_time) * 1000)
        return BatchValidationResult(
            scenario_results=scenario_results,
            summary_frame=summary_frame,
            run_summary_frame=self._build_run_summary_frame(
                selection=effective_selection,
                run_profile=run_profile,
                dataset_profile=dataset_profile,
                scenario_dataset_asset=resolved_scenario_dataset_asset,
                total_scenarios=len(selected_scenarios),
                scenario_results=scenario_results,
                preflight_failed_scenarios=0,
                duration_ms=duration_ms,
            ),
            issue_category_frame=self._build_issue_category_frame(scenario_results),
            pack_summary_frame=self._build_pack_summary_frame(scenario_results),
        )

    @staticmethod
    def build_preflight_blocked_result(
        preflight_result: ScenarioPreflightResult,
        selection: TripSearchScenarioSelection | None = None,
        run_profile: TripSearchRunProfile | None = None,
        dataset_profile: str = DEFAULT_DATASET_PROFILE,
        scenario_dataset_asset: str = "unknown",
    ) -> BatchValidationResult:
        """Build a run-level result for scenario packs blocked by preflight validation."""
        scenario_count = int(preflight_result.summary_frame.iloc[0]["scenario_count"])
        issue_count = int(preflight_result.summary_frame.iloc[0]["issue_count"])
        resolved_scenario_dataset_asset = TripSearchBatchValidator._resolve_scenario_dataset_asset(
            dataset_profile,
            scenario_dataset_asset,
        )
        selection_summary = (run_profile.to_selection() if run_profile is not None else selection or TripSearchScenarioSelection()).to_summary_dict()
        run_summary = TripSearchBatchValidator._build_run_profile_summary(run_profile)
        run_summary_frame = pd.DataFrame(
            [
                {
                    **run_summary,
                    "dataset_profile": dataset_profile,
                    "scenario_dataset_asset": resolved_scenario_dataset_asset,
                    **selection_summary,
                    "total_scenarios": scenario_count,
                    "passed_scenarios": 0,
                    "failed_scenarios": scenario_count,
                    "preflight_failed_scenarios": scenario_count,
                    "execution_failed_scenarios": 0,
                    "aggregate_match_count": 0,
                    "row_reconciliation_failure_count": 0,
                    "filter_failure_count": 0,
                    "duration_ms": 0,
                }
            ],
            columns=RUN_SUMMARY_COLUMNS,
        )
        issue_category_frame = pd.DataFrame(
            [{"issue_category": "preflight", "issue_count": issue_count}],
            columns=ISSUE_CATEGORY_COLUMNS,
        )
        return BatchValidationResult(
            scenario_results=[],
            summary_frame=pd.DataFrame(
                columns=[
                    "scenario_id",
                    "pack",
                    "tag",
                    "scenario_type",
                    "is_pass",
                    "expected_rows",
                    "actual_rows",
                    "missing_row_count",
                    "unexpected_row_count",
                    "aggregates_match",
                    "issue_categories",
                    "duration_ms",
                ]
            ),
            run_summary_frame=run_summary_frame,
            issue_category_frame=issue_category_frame,
            pack_summary_frame=pd.DataFrame(columns=PACK_SUMMARY_COLUMNS),
        )

    def _validate_scenario(self, scenario: TripSearchScenario, expected_trip_frame: pd.DataFrame) -> BatchScenarioResult:
        start_time = perf_counter()
        scenario_filters = scenario.to_search_filters()
        expected_subset = filter_expected_trip_frame(expected_trip_frame, **scenario_filters)
        request = SearchServiceRequest.build(**scenario_filters)
        actual_trips = search_by_route_and_departure_date(self.service_api, request)
        actual_subset = self._build_actual_trip_frame(actual_trips)
        reconciliation_result = self.reconciliation_validator.reconcile(expected_subset, actual_subset)

        expected_summary = build_aggregate_summary(expected_subset)
        actual_summary = build_aggregate_summary(actual_subset)
        expected_carrier_counts = build_carrier_count_frame(expected_subset)
        actual_carrier_counts = build_carrier_count_frame(actual_subset)
        aggregate_result = self.aggregate_validator.validate(
            expected_summary=expected_summary,
            actual_summary=actual_summary,
            expected_carrier_counts=expected_carrier_counts,
            actual_carrier_counts=actual_carrier_counts,
        )
        duration_ms = int((perf_counter() - start_time) * 1000)

        return BatchScenarioResult(
            scenario=scenario,
            expected_trip_frame=expected_subset,
            actual_trip_frame=actual_subset,
            reconciliation_result=reconciliation_result,
            aggregate_result=aggregate_result,
            duration_ms=duration_ms,
        )

    @staticmethod
    def _build_actual_trip_frame(actual_trips: list) -> pd.DataFrame:
        if not actual_trips:
            return pd.DataFrame(columns=CANONICAL_TRIP_COLUMNS)
        return pd.DataFrame([trip.to_canonical_dict() for trip in actual_trips], columns=CANONICAL_TRIP_COLUMNS)

    @staticmethod
    def _build_summary_row(result: BatchScenarioResult) -> dict[str, object]:
        return {
            "scenario_id": result.scenario.scenario_id,
            "pack": result.scenario.pack or "unassigned",
            "tag": result.scenario.tag or "unassigned",
            "scenario_type": result.scenario.scenario_type or "unassigned",
            "is_pass": result.is_pass,
            "expected_rows": result.reconciliation_result.expected_rows_count,
            "actual_rows": result.reconciliation_result.actual_rows_count,
            "missing_row_count": len(result.reconciliation_result.missing_rows),
            "unexpected_row_count": len(result.reconciliation_result.unexpected_rows),
            "aggregates_match": result.aggregate_result.is_match,
            "issue_categories": ",".join(result.issue_categories),
            "duration_ms": result.duration_ms,
        }

    @staticmethod
    def _build_run_summary_frame(
        selection: TripSearchScenarioSelection | None,
        run_profile: TripSearchRunProfile | None,
        dataset_profile: str,
        scenario_dataset_asset: str,
        total_scenarios: int,
        scenario_results: list[BatchScenarioResult],
        preflight_failed_scenarios: int,
        duration_ms: int,
    ) -> pd.DataFrame:
        selection_summary = (selection or TripSearchScenarioSelection()).to_summary_dict()
        run_summary = TripSearchBatchValidator._build_run_profile_summary(run_profile)
        passed_scenarios = sum(1 for result in scenario_results if result.is_pass)
        failed_scenarios = total_scenarios - passed_scenarios
        execution_failed_scenarios = failed_scenarios - preflight_failed_scenarios
        aggregate_match_count = sum(1 for result in scenario_results if result.aggregate_result.is_match)
        row_reconciliation_failure_count = sum(1 for result in scenario_results if not result.reconciliation_result.is_match)
        filter_failure_count = sum(1 for result in scenario_results if "filter_correctness" in result.issue_categories)
        return pd.DataFrame(
            [
                {
                    **run_summary,
                    "dataset_profile": dataset_profile,
                    "scenario_dataset_asset": scenario_dataset_asset,
                    **selection_summary,
                    "total_scenarios": total_scenarios,
                    "passed_scenarios": passed_scenarios,
                    "failed_scenarios": failed_scenarios,
                    "preflight_failed_scenarios": preflight_failed_scenarios,
                    "execution_failed_scenarios": execution_failed_scenarios,
                    "aggregate_match_count": aggregate_match_count,
                    "row_reconciliation_failure_count": row_reconciliation_failure_count,
                    "filter_failure_count": filter_failure_count,
                    "duration_ms": duration_ms,
                }
            ],
            columns=RUN_SUMMARY_COLUMNS,
        )

    @staticmethod
    def _build_run_profile_summary(run_profile: TripSearchRunProfile | None) -> dict[str, str]:
        if run_profile is None:
            return {
                "run_id": "adhoc",
                "run_label": "Ad Hoc Run",
            }
        return {
            "run_id": run_profile.run_id,
            "run_label": run_profile.run_label,
        }

    @staticmethod
    def _resolve_scenario_dataset_asset(dataset_profile: str, scenario_dataset_asset: str) -> str:
        if scenario_dataset_asset != "unknown":
            return scenario_dataset_asset
        return Path(get_dataset_profile_definition(dataset_profile).scenario_dataset_asset).name

    @staticmethod
    def _build_issue_category_frame(scenario_results: list[BatchScenarioResult]) -> pd.DataFrame:
        issue_counts = {
            "preflight": 0,
            "row_reconciliation": sum(1 for result in scenario_results if "row_reconciliation" in result.issue_categories),
            "aggregate_mismatch": sum(1 for result in scenario_results if "aggregate_mismatch" in result.issue_categories),
            "filter_correctness": sum(1 for result in scenario_results if "filter_correctness" in result.issue_categories),
        }
        return pd.DataFrame(
            [{"issue_category": category, "issue_count": count} for category, count in issue_counts.items()],
            columns=ISSUE_CATEGORY_COLUMNS,
        )

    @staticmethod
    def _build_summary_frame(scenario_results: list[BatchScenarioResult]) -> pd.DataFrame:
        if not scenario_results:
            return pd.DataFrame(
                columns=[
                    "scenario_id",
                    "pack",
                    "tag",
                    "scenario_type",
                    "is_pass",
                    "expected_rows",
                    "actual_rows",
                    "missing_row_count",
                    "unexpected_row_count",
                    "aggregates_match",
                    "issue_categories",
                    "duration_ms",
                ]
            )
        return pd.DataFrame([TripSearchBatchValidator._build_summary_row(result) for result in scenario_results])

    @staticmethod
    def _build_pack_summary_frame(scenario_results: list[BatchScenarioResult]) -> pd.DataFrame:
        if not scenario_results:
            return pd.DataFrame(columns=PACK_SUMMARY_COLUMNS)

        summary_rows: list[dict[str, object]] = []
        packs = sorted({result.scenario.pack or "unassigned" for result in scenario_results})
        for pack in packs:
            pack_results = [result for result in scenario_results if (result.scenario.pack or "unassigned") == pack]
            summary_rows.append(
                {
                    "pack": pack,
                    "total_scenarios": len(pack_results),
                    "passed_scenarios": sum(1 for result in pack_results if result.is_pass),
                    "failed_scenarios": sum(1 for result in pack_results if not result.is_pass),
                }
            )
        return pd.DataFrame(summary_rows, columns=PACK_SUMMARY_COLUMNS)
