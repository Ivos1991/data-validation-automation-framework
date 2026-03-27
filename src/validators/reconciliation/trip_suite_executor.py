from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.domain.trip_search.search_models import TripSearchRunProfile, TripSearchRunSuite, TripSearchRunSuitePolicy
from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.framework.connectors.files.run_profile_loader import TripSearchRunProfileLoader
from src.validators.reconciliation.trip_batch_validator import BatchValidationResult, TripSearchBatchValidator


SUITE_SUMMARY_COLUMNS = [
    "suite_id",
    "suite_label",
    "dataset_profile",
    "scenario_dataset_asset",
    "suite_status",
    "stopped_early",
    "stop_on_first_failed_run",
    "continue_on_failure",
    "minimum_passed_scenarios",
    "minimum_pass_rate",
    "maximum_failed_runs",
    "minimum_passed_scenarios_met",
    "minimum_pass_rate_met",
    "maximum_failed_runs_met",
    "total_runs",
    "total_scenarios_executed",
    "total_passed_scenarios",
    "total_failed_scenarios",
    "total_preflight_failed_runs",
]

SUITE_RUN_SUMMARY_COLUMNS = [
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
    "run_status",
]


@dataclass(frozen=True)
class SuiteRunResult:
    """Executed run-profile result inside a suite."""

    run_profile: TripSearchRunProfile
    batch_result: BatchValidationResult


@dataclass(frozen=True)
class TripSearchRunSuiteResult:
    """Suite execution output including suite-level rollups and per-run summaries."""

    suite: TripSearchRunSuite
    run_results: list[SuiteRunResult]
    suite_summary_frame: pd.DataFrame
    suite_run_summary_frame: pd.DataFrame
    issue_category_rollup_frame: pd.DataFrame


class TripSearchRunSuiteExecutor:
    """Execute ordered run profiles and evaluate suite-level policies."""

    def __init__(
        self,
        service_api: SearchServiceAPI,
        numeric_tolerance: float,
        run_profile_loader: TripSearchRunProfileLoader | None = None,
        batch_validator: TripSearchBatchValidator | None = None,
    ) -> None:
        self.batch_validator = batch_validator or TripSearchBatchValidator(service_api, numeric_tolerance)
        self.run_profile_loader = run_profile_loader or TripSearchRunProfileLoader()

    def execute(
        self,
        suite: TripSearchRunSuite,
        scenarios,
        expected_trip_frame: pd.DataFrame,
        dataset_profile: str = "small",
        scenario_dataset_asset: str = "unknown",
    ) -> TripSearchRunSuiteResult:
        """Execute a suite through the existing batch validator and roll up the results."""
        run_results: list[SuiteRunResult] = []
        stopped_early = False
        for run_profile_reference in suite.run_profiles:
            run_profile = self.run_profile_loader.load_json(run_profile_reference.profile_path)
            batch_result = self.batch_validator.validate(
                scenarios,
                expected_trip_frame,
                run_profile=run_profile,
                dataset_profile=run_profile.dataset_profile or suite.dataset_profile or dataset_profile,
                scenario_dataset_asset=scenario_dataset_asset,
            )
            run_results.append(SuiteRunResult(run_profile=run_profile, batch_result=batch_result))
            if self._should_stop_early(suite.policy, run_results[-1].batch_result, run_results):
                stopped_early = True
                break

        suite_run_summary_frame = self._build_suite_run_summary_frame(run_results)
        suite_summary_frame = self._build_suite_summary_frame(
            suite,
            run_results,
            stopped_early,
            dataset_profile=(
                suite.dataset_profile
                or (run_results[0].batch_result.run_summary_frame.iloc[0]["dataset_profile"] if run_results else dataset_profile)
            ),
            scenario_dataset_asset=(
                run_results[0].batch_result.run_summary_frame.iloc[0]["scenario_dataset_asset"]
                if run_results
                else scenario_dataset_asset
            ),
        )
        issue_category_rollup_frame = self._build_issue_category_rollup_frame(run_results)
        return TripSearchRunSuiteResult(
            suite=suite,
            run_results=run_results,
            suite_summary_frame=suite_summary_frame,
            suite_run_summary_frame=suite_run_summary_frame,
            issue_category_rollup_frame=issue_category_rollup_frame,
        )

    @staticmethod
    def _build_suite_summary_frame(
        suite: TripSearchRunSuite,
        run_results: list[SuiteRunResult],
        stopped_early: bool,
        dataset_profile: str,
        scenario_dataset_asset: str,
    ) -> pd.DataFrame:
        total_scenarios_executed = sum(int(result.batch_result.run_summary_frame.iloc[0]["total_scenarios"]) for result in run_results)
        total_passed_scenarios = sum(int(result.batch_result.run_summary_frame.iloc[0]["passed_scenarios"]) for result in run_results)
        total_failed_scenarios = sum(int(result.batch_result.run_summary_frame.iloc[0]["failed_scenarios"]) for result in run_results)
        total_preflight_failed_runs = sum(
            1 for result in run_results if int(result.batch_result.run_summary_frame.iloc[0]["preflight_failed_scenarios"]) > 0
        )
        minimum_passed_scenarios_met = suite.policy.minimum_passed_scenarios is None or total_passed_scenarios >= suite.policy.minimum_passed_scenarios
        pass_rate = (total_passed_scenarios / total_scenarios_executed) if total_scenarios_executed else 0.0
        minimum_pass_rate_met = suite.policy.minimum_pass_rate is None or pass_rate >= suite.policy.minimum_pass_rate
        failed_run_count = sum(1 for result in run_results if TripSearchRunSuiteExecutor._run_status(result.batch_result) != "passed")
        maximum_failed_runs_met = suite.policy.maximum_failed_runs is None or failed_run_count <= suite.policy.maximum_failed_runs
        suite_status = TripSearchRunSuiteExecutor._suite_status(
            run_results=run_results,
            policy=suite.policy,
            minimum_passed_scenarios_met=minimum_passed_scenarios_met,
            minimum_pass_rate_met=minimum_pass_rate_met,
            maximum_failed_runs_met=maximum_failed_runs_met,
            stopped_early=stopped_early,
        )
        return pd.DataFrame(
            [
                {
                    "suite_id": suite.suite_id,
                    "suite_label": suite.suite_label,
                    "dataset_profile": dataset_profile,
                    "scenario_dataset_asset": scenario_dataset_asset,
                    "suite_status": suite_status,
                    "stopped_early": stopped_early,
                    "stop_on_first_failed_run": suite.policy.stop_on_first_failed_run,
                    "continue_on_failure": suite.policy.continue_on_failure,
                    "minimum_passed_scenarios": suite.policy.minimum_passed_scenarios,
                    "minimum_pass_rate": suite.policy.minimum_pass_rate,
                    "maximum_failed_runs": suite.policy.maximum_failed_runs,
                    "minimum_passed_scenarios_met": minimum_passed_scenarios_met,
                    "minimum_pass_rate_met": minimum_pass_rate_met,
                    "maximum_failed_runs_met": maximum_failed_runs_met,
                    "total_runs": len(run_results),
                    "total_scenarios_executed": total_scenarios_executed,
                    "total_passed_scenarios": total_passed_scenarios,
                    "total_failed_scenarios": total_failed_scenarios,
                    "total_preflight_failed_runs": total_preflight_failed_runs,
                }
            ],
            columns=SUITE_SUMMARY_COLUMNS,
        )

    @staticmethod
    def _build_suite_run_summary_frame(run_results: list[SuiteRunResult]) -> pd.DataFrame:
        if not run_results:
            return pd.DataFrame(columns=SUITE_RUN_SUMMARY_COLUMNS)
        rows = []
        for result in run_results:
            row = result.batch_result.run_summary_frame.iloc[0][
                [
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
                ]
            ].to_dict()
            row["run_status"] = TripSearchRunSuiteExecutor._run_status(result.batch_result)
            rows.append(row)
        return pd.DataFrame(rows, columns=SUITE_RUN_SUMMARY_COLUMNS)

    @staticmethod
    def _build_issue_category_rollup_frame(run_results: list[SuiteRunResult]) -> pd.DataFrame:
        issue_counts: dict[str, int] = {}
        for result in run_results:
            for _, row in result.batch_result.issue_category_frame.iterrows():
                category = str(row["issue_category"])
                issue_counts[category] = issue_counts.get(category, 0) + int(row["issue_count"])
        return pd.DataFrame(
            [{"issue_category": category, "issue_count": count} for category, count in sorted(issue_counts.items())],
            columns=["issue_category", "issue_count"],
        )

    @staticmethod
    def _run_status(batch_result: BatchValidationResult) -> str:
        run_summary = batch_result.run_summary_frame.iloc[0]
        if int(run_summary["preflight_failed_scenarios"]) > 0:
            return "blocked"
        if int(run_summary["failed_scenarios"]) == 0:
            return "passed"
        return "failed"

    @staticmethod
    def _should_stop_early(
        policy: TripSearchRunSuitePolicy,
        batch_result: BatchValidationResult,
        run_results: list[SuiteRunResult],
    ) -> bool:
        if policy.stop_on_first_failed_run and TripSearchRunSuiteExecutor._run_status(batch_result) != "passed":
            return True
        if not policy.continue_on_failure and TripSearchRunSuiteExecutor._run_status(batch_result) != "passed":
            return True
        if policy.maximum_failed_runs is not None:
            failed_run_count = sum(
                1 for result in run_results if TripSearchRunSuiteExecutor._run_status(result.batch_result) != "passed"
            )
            if failed_run_count > policy.maximum_failed_runs:
                return True
        return False

    @staticmethod
    def _suite_status(
        run_results: list[SuiteRunResult],
        policy: TripSearchRunSuitePolicy,
        minimum_passed_scenarios_met: bool,
        minimum_pass_rate_met: bool,
        maximum_failed_runs_met: bool,
        stopped_early: bool,
    ) -> str:
        if any(TripSearchRunSuiteExecutor._run_status(result.batch_result) == "blocked" for result in run_results):
            return "blocked"
        if not minimum_passed_scenarios_met or not minimum_pass_rate_met or not maximum_failed_runs_met:
            return "failed"
        if all(TripSearchRunSuiteExecutor._run_status(result.batch_result) == "passed" for result in run_results):
            return "passed"
        if stopped_early or any(TripSearchRunSuiteExecutor._run_status(result.batch_result) == "failed" for result in run_results):
            return "partial"
        return "failed"
