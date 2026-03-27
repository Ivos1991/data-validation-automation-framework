from pathlib import Path
from uuid import uuid4

import allure
import json
from tests.assertions import assert_that, soft_assertions

from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader
from src.framework.reporting.trip_search_reporting import build_suite_reporting_bundle
from src.validators.reconciliation.trip_batch_validator import TripSearchBatchValidator
from src.validators.reconciliation.trip_suite_executor import TripSearchRunSuiteExecutor
from tests.batch_tests.support import (
    FaultInjectingSuiteSearchServiceAPI,
    PreflightBlockedBatchValidator,
    attachable_frame,
    attachable_preflight_result,
)


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Run Suite Execution")
class TestTripSearchRunSuiteExecution:
    """Suite-execution tests for run sequencing, policy handling, and rollups."""

    @allure.title("Run suite executes multiple run profiles in sequence")
    def test_run_suite_expects_multiple_profiles_to_execute(
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
            assert_that(suite_result.run_results, "Expected assertion for suite_result.run_results to hold").is_length(2)
            assert_that(suite_result.suite_run_summary_frame["run_id"].tolist(), "Expected assertion for suite_result.suite_run_summary_frame['run_id'].tolist() to hold").is_equal_to(
                ["smoke-run", "filters-pack-run"]
            )
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_runs']) to hold").is_equal_to(2)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_scenarios_executed"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_scenarios_executed']) to hold").is_equal_to(3)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_passed_scenarios"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_passed_scenarios']) to hold").is_equal_to(3)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_failed_scenarios"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_failed_scenarios']) to hold").is_equal_to(0)
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'suite_status'] to hold").is_equal_to("passed")
            assert_that(bool(suite_result.suite_summary_frame.loc[0, "stopped_early"]), "Expected assertion for bool(suite_result.suite_summary_frame.loc[0, 'stopped_early']) to hold").is_false()

    @allure.title("Run suite summary rolls up mixed results across runs")
    def test_run_suite_expects_mixed_results_to_be_reported_in_summary(
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
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_runs']) to hold").is_equal_to(2)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_passed_scenarios"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_passed_scenarios']) to hold").is_equal_to(2)
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_failed_scenarios"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_failed_scenarios']) to hold").is_equal_to(1)
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'suite_status'] to hold").is_equal_to("partial")
            assert_that(int(issue_rollup.loc["row_reconciliation", "issue_count"]), "Expected assertion for int(issue_rollup.loc['row_reconciliation', 'issue_count']) to hold").is_equal_to(1)
            assert_that(int(issue_rollup.loc["aggregate_mismatch", "issue_count"]), "Expected assertion for int(issue_rollup.loc['aggregate_mismatch', 'issue_count']) to hold").is_equal_to(1)

    @allure.title("Run suite supports stop-on-first-failed-run policies")
    def test_run_suite_expects_stop_on_first_failed_run_to_stop_early(
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

        run_suite = TripSearchRunSuiteLoader().load_json(suite_path)
        executor = TripSearchRunSuiteExecutor(
            service_api=FaultInjectingSuiteSearchServiceAPI(batch_trip_search_service_api),
            numeric_tolerance=config.numeric_tolerance,
            run_profile_loader=run_profile_loader,
        )
        suite_result = executor.execute(run_suite, batch_scenarios, expected_trip_frame)
        build_suite_reporting_bundle(suite_result).attach_to_allure("stop-early-suite")

        with soft_assertions():
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_runs']) to hold").is_equal_to(1)
            assert_that(bool(suite_result.suite_summary_frame.loc[0, "stopped_early"]), "Expected assertion for bool(suite_result.suite_summary_frame.loc[0, 'stopped_early']) to hold").is_true()
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'suite_status'] to hold").is_equal_to("partial")

    @allure.title("Run suite enforces minimum pass thresholds")
    def test_run_suite_expects_minimum_pass_thresholds_to_be_enforced(
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

        run_suite = TripSearchRunSuiteLoader().load_json(suite_path)
        executor = TripSearchRunSuiteExecutor(
            service_api=FaultInjectingSuiteSearchServiceAPI(batch_trip_search_service_api),
            numeric_tolerance=config.numeric_tolerance,
            run_profile_loader=run_profile_loader,
        )
        suite_result = executor.execute(run_suite, batch_scenarios, expected_trip_frame)
        build_suite_reporting_bundle(suite_result).attach_to_allure("threshold-suite")

        with soft_assertions():
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'suite_status'] to hold").is_equal_to("failed")
            assert_that(bool(suite_result.suite_summary_frame.loc[0, "minimum_pass_rate_met"]), "Expected assertion for bool(suite_result.suite_summary_frame.loc[0, 'minimum_pass_rate_met']) to hold").is_false()

    @allure.title("Run suite can report blocked status from preflight-blocked runs")
    def test_run_suite_expects_blocked_status_to_be_reported(
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
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'suite_status'] to hold").is_equal_to("blocked")
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_preflight_failed_runs"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_preflight_failed_runs']) to hold").is_equal_to(2)


