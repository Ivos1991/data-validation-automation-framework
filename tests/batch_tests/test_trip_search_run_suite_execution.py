from pathlib import Path
from uuid import uuid4

import allure
import json
from assertpy import assert_that, soft_assertions

from src.domain.trip_search.search_models import TripSearchRunSuitePolicy
from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader
from src.framework.reporting.allure_helpers import attach_dataframe
from src.framework.reporting.trip_search_reporting import build_suite_reporting_bundle
from src.validators.reconciliation.trip_batch_validator import TripSearchBatchValidator
from src.validators.reconciliation.trip_suite_executor import TripSearchRunSuiteExecutor


class FaultInjectingSuiteSearchServiceAPI:
    def __init__(self, wrapped_service_api) -> None:
        """Wrap the real API and force one suite run to fail deterministically."""
        self.wrapped_service_api = wrapped_service_api

    def search_by_route_and_departure_date(self, request_params):
        """Return an empty result for the injected failing scenario."""
        payload = self.wrapped_service_api.search_by_route_and_departure_date(request_params)
        if request_params.get("carrier") == "AmRail" and request_params.get("stops_count") == 0:
            return {"trips": []}
        return payload


class PreflightBlockedBatchValidator:
    def __init__(self, blocked_result) -> None:
        """Return a fixed preflight-blocked batch result for suite tests."""
        self.blocked_result = blocked_result

    def validate(
        self,
        scenarios,
        expected_trip_frame,
        selection=None,
        run_profile=None,
        dataset_profile="small",
        scenario_dataset_asset="unknown",
    ):
        """Return the injected blocked result without executing validation."""
        return self.blocked_result


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Run Suite Execution")
class TestTripSearchRunSuiteExecution:
    """Suite-execution tests for run sequencing, policy handling, and rollups."""

    @allure.title("Run suite executes multiple run profiles in sequence")
    def test_run_suite_executes_multiple_profiles(
        self,
        default_run_suite,
        batch_scenarios,
        expected_trip_frame,
        run_suite_executor,
    ):
        """Verify a suite executes its run profiles sequentially with clean rollups."""
        suite_result = run_suite_executor.execute(default_run_suite, batch_scenarios, expected_trip_frame)
        build_suite_reporting_bundle(suite_result).attach_to_allure("suite")

        with soft_assertions():
            assert_that(suite_result.run_results).is_length(2)
            assert_that(suite_result.suite_run_summary_frame["run_id"].tolist()).is_equal_to(
                ["smoke-run", "filters-pack-run"]
            )
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"])).is_equal_to(2)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_scenarios_executed"])).is_equal_to(3)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_passed_scenarios"])).is_equal_to(3)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_failed_scenarios"])).is_equal_to(0)
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"]).is_equal_to("passed")
            assert_that(bool(suite_result.suite_summary_frame.loc[0, "stopped_early"])).is_false()

    @allure.title("Run suite summary rolls up mixed results across runs")
    def test_run_suite_summary_reports_mixed_results(
        self,
        config,
        default_run_suite,
        batch_scenarios,
        expected_trip_frame,
        batch_trip_search_service_api,
        run_profile_loader,
    ):
        """Verify mixed run outcomes are rolled up into suite summaries and issue counts."""
        executor = TripSearchRunSuiteExecutor(
            service_api=FaultInjectingSuiteSearchServiceAPI(batch_trip_search_service_api),
            numeric_tolerance=config.numeric_tolerance,
            run_profile_loader=run_profile_loader,
        )
        suite_result = executor.execute(default_run_suite, batch_scenarios, expected_trip_frame)
        build_suite_reporting_bundle(suite_result).attach_to_allure("mixed-suite")

        issue_rollup = suite_result.issue_category_rollup_frame.set_index("issue_category")
        with soft_assertions():
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"])).is_equal_to(2)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_passed_scenarios"])).is_equal_to(2)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_failed_scenarios"])).is_equal_to(1)
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"]).is_equal_to("partial")
            assert_that(int(issue_rollup.loc["row_reconciliation", "issue_count"])).is_equal_to(1)
            assert_that(int(issue_rollup.loc["aggregate_mismatch", "issue_count"])).is_equal_to(1)

    @allure.title("Run suite loader and executor support suite files built in tests")
    def test_run_suite_executes_test_defined_suite(
        self,
        local_batch_test_dir: Path,
        config,
        run_profile_loader,
        batch_trip_search_service_api,
        batch_scenarios,
        expected_trip_frame,
    ):
        """Verify suites created inside tests still load and execute through the shared path."""
        ad_hoc_profile_path = local_batch_test_dir / f"negative_run_{uuid4().hex}.json"
        ad_hoc_profile_path.write_text(
            (
                '{'
                '"run_id": "negative-run", '
                '"run_label": "Negative Run", '
                '"selected_scenario_type": "negative"'
                '}'
            ),
            encoding="utf-8",
        )
        suite_path = local_batch_test_dir / f"suite_{uuid4().hex}.json"
        suite_path.write_text(
            (
                '{'
                '"suite_id": "negative-suite", '
                '"suite_label": "Negative Suite", '
                '"run_profiles": ['
                f'{{"profile_path": "{ad_hoc_profile_path.name}"}}'
                ']'
                '}'
            ),
            encoding="utf-8",
        )

        try:
            run_suite = TripSearchRunSuiteLoader().load_json(suite_path)
            suite_result = TripSearchRunSuiteExecutor(
                service_api=batch_trip_search_service_api,
                numeric_tolerance=config.numeric_tolerance,
                run_profile_loader=run_profile_loader,
            ).execute(run_suite, batch_scenarios, expected_trip_frame)
            build_suite_reporting_bundle(suite_result).attach_to_allure("test-defined-suite")

            with soft_assertions():
                assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"])).is_equal_to(1)
                assert_that(suite_result.suite_run_summary_frame["run_id"].tolist()).is_equal_to(["negative-run"])
                assert_that(int(suite_result.suite_run_summary_frame.loc[0, "total_scenarios"])).is_equal_to(1)
        finally:
            if ad_hoc_profile_path.exists():
                ad_hoc_profile_path.unlink()
            if suite_path.exists():
                suite_path.unlink()

    @allure.title("Run suite supports stop-on-first-failed-run policies")
    def test_run_suite_stops_early_on_first_failed_run(
        self,
        local_batch_test_dir: Path,
        config,
        run_profile_loader,
        batch_trip_search_service_api,
        batch_scenarios,
        expected_trip_frame,
    ):
        """Verify stop-on-first-failure policies halt suite execution early."""
        suite_path = local_batch_test_dir / f"stop_early_suite_{uuid4().hex}.json"
        suite_path.write_text(
            json.dumps(
                {
                    "suite_id": "stop-early-suite",
                    "suite_label": "Stop Early Suite",
                    "policy": {
                        "stop_on_first_failed_run": True,
                        "continue_on_failure": False,
                    },
                    "run_profiles": [
                        {"profile_path": str(config.run_profile_path)},
                        {"profile_path": str(config.run_profile_path.parent / "smoke_trip_search_run_profile.json")},
                    ],
                }
            ),
            encoding="utf-8",
        )

        try:
            run_suite = TripSearchRunSuiteLoader().load_json(suite_path)
            executor = TripSearchRunSuiteExecutor(
                service_api=FaultInjectingSuiteSearchServiceAPI(batch_trip_search_service_api),
                numeric_tolerance=config.numeric_tolerance,
                run_profile_loader=run_profile_loader,
            )
            suite_result = executor.execute(run_suite, batch_scenarios, expected_trip_frame)
            build_suite_reporting_bundle(suite_result).attach_to_allure("stop-early-suite")

            with soft_assertions():
                assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"])).is_equal_to(1)
                assert_that(bool(suite_result.suite_summary_frame.loc[0, "stopped_early"])).is_true()
                assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"]).is_equal_to("partial")
        finally:
            if suite_path.exists():
                suite_path.unlink()

    @allure.title("Run suite enforces minimum pass thresholds")
    def test_run_suite_enforces_minimum_pass_thresholds(
        self,
        local_batch_test_dir: Path,
        config,
        run_profile_loader,
        batch_trip_search_service_api,
        batch_scenarios,
        expected_trip_frame,
    ):
        """Verify suite thresholds can force a failed final suite status."""
        suite_path = local_batch_test_dir / f"threshold_suite_{uuid4().hex}.json"
        suite_path.write_text(
            json.dumps(
                {
                    "suite_id": "threshold-suite",
                    "suite_label": "Threshold Suite",
                    "policy": {
                        "minimum_pass_rate": 1.0,
                    },
                    "run_profiles": [
                        {"profile_path": str(config.run_profile_path.parent / "smoke_trip_search_run_profile.json")},
                        {"profile_path": str(config.run_profile_path)},
                    ],
                }
            ),
            encoding="utf-8",
        )

        try:
            run_suite = TripSearchRunSuiteLoader().load_json(suite_path)
            executor = TripSearchRunSuiteExecutor(
                service_api=FaultInjectingSuiteSearchServiceAPI(batch_trip_search_service_api),
                numeric_tolerance=config.numeric_tolerance,
                run_profile_loader=run_profile_loader,
            )
            suite_result = executor.execute(run_suite, batch_scenarios, expected_trip_frame)
            build_suite_reporting_bundle(suite_result).attach_to_allure("threshold-suite")

            with soft_assertions():
                assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"]).is_equal_to("failed")
                assert_that(bool(suite_result.suite_summary_frame.loc[0, "minimum_pass_rate_met"])).is_false()
        finally:
            if suite_path.exists():
                suite_path.unlink()

    @allure.title("Run suite can report blocked status from preflight-blocked runs")
    def test_run_suite_reports_blocked_status(
        self,
        config,
        default_run_suite,
        batch_trip_search_service_api,
        run_profile_loader,
    ):
        """Verify preflight-blocked runs surface a blocked suite status."""
        blocked_result = TripSearchBatchValidator.build_preflight_blocked_result(
            preflight_result=attachable_preflight_result(),
        )
        executor = TripSearchRunSuiteExecutor(
            service_api=batch_trip_search_service_api,
            numeric_tolerance=config.numeric_tolerance,
            run_profile_loader=run_profile_loader,
            batch_validator=PreflightBlockedBatchValidator(blocked_result),
        )
        suite_result = executor.execute(default_run_suite, scenarios=[], expected_trip_frame=attachable_frame([]))
        build_suite_reporting_bundle(suite_result).attach_to_allure("blocked-suite")

        with soft_assertions():
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"]).is_equal_to("blocked")
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_preflight_failed_runs"])).is_equal_to(2)


def attachable_frame(rows: list[dict[str, object]]):
    """Build a lightweight dataframe for suite reporting test payloads."""
    import pandas as pd

    return pd.DataFrame(rows)


def attachable_preflight_result():
    """Build a fixed preflight result used by blocked-suite tests."""
    from src.validators.quality.trip_search_scenario_preflight_validator import ScenarioPreflightResult

    return ScenarioPreflightResult(
        is_valid=False,
        summary_frame=attachable_frame([{"scenario_count": 1, "issue_count": 1, "is_valid": False}]),
        issues_frame=attachable_frame(
            [{"scenario_id": "blocked", "issue_code": "duplicate_scenario_id", "issue_message": "blocked"}]
        ),
    )


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Suite Reporting")
class TestTripSearchSuiteReportingBundle:
    """Suite reporting bundle tests for Allure/export packaging inputs."""

    @allure.title("Suite reporting bundle exposes policy and status summaries for Allure packaging")
    def test_suite_reporting_bundle_exposes_policy_and_status_summaries(
        self,
        default_run_suite,
        batch_scenarios,
        expected_trip_frame,
        run_suite_executor,
    ):
        """Verify the suite reporting bundle exposes the key policy and status fields."""
        suite_result = run_suite_executor.execute(default_run_suite, batch_scenarios, expected_trip_frame)
        reporting_bundle = build_suite_reporting_bundle(suite_result)

        with soft_assertions():
            assert_that(reporting_bundle.policy_summary["stop_on_first_failed_run"]).is_false()
            assert_that(reporting_bundle.policy_summary["continue_on_failure"]).is_true()
            assert_that(reporting_bundle.status_summary["dataset_profile"]).is_equal_to("small")
            assert_that(reporting_bundle.status_summary["scenario_dataset_asset"]).is_equal_to("batch_trip_search_scenarios.csv")
            assert_that(reporting_bundle.status_summary["suite_status"]).is_equal_to("passed")
            assert_that(reporting_bundle.status_summary["stopped_early"]).is_false()
