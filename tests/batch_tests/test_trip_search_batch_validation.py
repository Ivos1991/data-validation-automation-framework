import allure
from assertpy import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe
from src.framework.reporting.trip_search_reporting import build_batch_reporting_bundle
from src.validators.quality.trip_search_scenario_preflight_validator import ScenarioPreflightResult
from src.validators.reconciliation.trip_batch_validator import TripSearchBatchValidator


class FaultInjectingSearchServiceAPI:
    def __init__(self, wrapped_service_api, failing_scenario_id: str) -> None:
        """Wrap the real API and force one scenario to fail deterministically."""
        self.wrapped_service_api = wrapped_service_api
        self.failing_scenario_id = failing_scenario_id

    def search_by_route_and_departure_date(self, request_params):
        """Return an empty result for the injected failing scenario."""
        payload = self.wrapped_service_api.search_by_route_and_departure_date(request_params)
        if request_params.get("carrier") == "AmRail" and request_params.get("stops_count") == 0:
            return {"trips": []}
        return payload


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Batch Validation")
class TestTripSearchBatchValidation:
    """Batch-validation tests for mixed scenario execution and reporting."""

    @allure.title("Batch validation executes multiple scenarios and produces a summary table")
    def test_batch_validation_executes_successfully(self, batch_scenario_dataset, batch_validation_result):
        """Verify a clean batch run produces consistent scenario and run summaries."""
        attach_dataframe("loaded-scenario-table", batch_scenario_dataset.scenario_frame)
        attach_dataframe("scenario-preflight-summary", batch_scenario_dataset.preflight_result.summary_frame)
        build_batch_reporting_bundle(batch_validation_result).attach_to_allure("batch")

        with soft_assertions():
            assert_that(batch_validation_result.summary_frame["scenario_id"].tolist()).is_equal_to(
                ["route-date-only", "carrier-filter", "combined-filter", "no-match"]
            )
            assert_that(batch_validation_result.summary_frame["is_pass"].tolist()).is_equal_to([True, True, True, True])
            assert_that(int(batch_validation_result.summary_frame.loc[0, "expected_rows"])).is_equal_to(2)
            assert_that(int(batch_validation_result.summary_frame.loc[3, "expected_rows"])).is_equal_to(0)
            assert_that(batch_validation_result.summary_frame["pack"].tolist()).is_equal_to(
                ["smoke", "filters", "filters", "regression"]
            )
            assert_that(int(batch_validation_result.run_summary_frame.loc[0, "total_scenarios"])).is_equal_to(4)
            assert_that(int(batch_validation_result.run_summary_frame.loc[0, "passed_scenarios"])).is_equal_to(4)
            assert_that(int(batch_validation_result.run_summary_frame.loc[0, "failed_scenarios"])).is_equal_to(0)
            assert_that(batch_validation_result.run_summary_frame.loc[0, "run_id"]).is_equal_to("adhoc")
            assert_that(batch_validation_result.run_summary_frame.loc[0, "run_label"]).is_equal_to("Ad Hoc Run")
            assert_that(batch_validation_result.run_summary_frame.loc[0, "dataset_profile"]).is_equal_to("small")
            assert_that(batch_validation_result.run_summary_frame.loc[0, "scenario_dataset_asset"]).is_equal_to("batch_trip_search_scenarios.csv")
            assert_that(batch_validation_result.run_summary_frame.loc[0, "selected_pack"]).is_equal_to("all")
            assert_that(batch_validation_result.run_summary_frame.loc[0, "selected_tag"]).is_equal_to("all")
            assert_that(batch_validation_result.run_summary_frame.loc[0, "selected_scenario_type"]).is_equal_to("all")
            assert_that(int(batch_validation_result.issue_category_frame.set_index("issue_category").loc["row_reconciliation", "issue_count"])).is_equal_to(0)

    @allure.title("Batch summary reports mixed results when one scenario fails")
    def test_batch_validation_reports_mixed_results(
        self,
        config,
        batch_trip_search_service_api,
        batch_scenario_dataset,
        expected_trip_frame,
        batch_scenarios,
    ):
        """Verify a single failing scenario is reflected in run-level rollups."""
        validator = TripSearchBatchValidator(
            FaultInjectingSearchServiceAPI(batch_trip_search_service_api, failing_scenario_id="combined-filter"),
            config.numeric_tolerance,
        )
        batch_result = validator.validate(batch_scenarios, expected_trip_frame)
        attach_dataframe("mixed-batch-loaded-scenarios", batch_scenario_dataset.scenario_frame)
        attach_dataframe("mixed-batch-preflight-summary", batch_scenario_dataset.preflight_result.summary_frame)
        build_batch_reporting_bundle(batch_result).attach_to_allure("mixed-batch")

        failing_result = next(result for result in batch_result.scenario_results if result.scenario.scenario_id == "combined-filter")
        attach_dataframe("mixed-batch-failing-missing-rows", failing_result.reconciliation_result.missing_rows)
        attach_dataframe("mixed-batch-failing-unexpected-rows", failing_result.reconciliation_result.unexpected_rows)
        attach_dataframe("mixed-batch-failing-summary-mismatches", failing_result.aggregate_result.summary_mismatches)

        summary_by_scenario = batch_result.summary_frame.set_index("scenario_id")
        issue_categories = batch_result.issue_category_frame.set_index("issue_category")
        run_summary = batch_result.run_summary_frame.iloc[0]

        with soft_assertions():
            assert_that(summary_by_scenario.loc["route-date-only", "is_pass"]).is_true()
            assert_that(summary_by_scenario.loc["carrier-filter", "is_pass"]).is_true()
            assert_that(summary_by_scenario.loc["combined-filter", "is_pass"]).is_false()
            assert_that(summary_by_scenario.loc["combined-filter", "missing_row_count"]).is_equal_to(1)
            assert_that(summary_by_scenario.loc["combined-filter", "aggregates_match"]).is_false()
            assert_that(summary_by_scenario.loc["no-match", "is_pass"]).is_true()
            assert_that(int(run_summary["total_scenarios"])).is_equal_to(4)
            assert_that(int(run_summary["passed_scenarios"])).is_equal_to(3)
            assert_that(int(run_summary["failed_scenarios"])).is_equal_to(1)
            assert_that(int(run_summary["execution_failed_scenarios"])).is_equal_to(1)
            assert_that(int(run_summary["aggregate_match_count"])).is_equal_to(3)
            assert_that(int(run_summary["row_reconciliation_failure_count"])).is_equal_to(1)
            assert_that(int(run_summary["filter_failure_count"])).is_equal_to(0)
            assert_that(int(issue_categories.loc["row_reconciliation", "issue_count"])).is_equal_to(1)
            assert_that(int(issue_categories.loc["aggregate_mismatch", "issue_count"])).is_equal_to(1)
            assert_that(int(issue_categories.loc["preflight", "issue_count"])).is_equal_to(0)

    @allure.title("Batch scenario result details stay aligned with summary output")
    def test_batch_validation_summary_matches_scenario_details(self, batch_validation_result):
        """Verify per-scenario summary rows stay aligned with detailed scenario results."""
        build_batch_reporting_bundle(batch_validation_result).attach_to_allure("batch-detail-check")

        summary_by_scenario = batch_validation_result.summary_frame.set_index("scenario_id")

        with soft_assertions():
            for scenario_result in batch_validation_result.scenario_results:
                summary_row = summary_by_scenario.loc[scenario_result.scenario.scenario_id]
                assert_that(summary_row["is_pass"]).is_equal_to(scenario_result.is_pass)
                assert_that(summary_row["expected_rows"]).is_equal_to(scenario_result.reconciliation_result.expected_rows_count)
                assert_that(summary_row["actual_rows"]).is_equal_to(scenario_result.reconciliation_result.actual_rows_count)
                assert_that(summary_row["missing_row_count"]).is_equal_to(len(scenario_result.reconciliation_result.missing_rows))
                assert_that(summary_row["unexpected_row_count"]).is_equal_to(len(scenario_result.reconciliation_result.unexpected_rows))
                assert_that(summary_row["aggregates_match"]).is_equal_to(scenario_result.aggregate_result.is_match)

    @allure.title("Batch run summary can represent preflight-blocked scenario packs")
    def test_batch_validation_builds_preflight_blocked_run_summary(self):
        """Verify preflight-blocked batches still produce consistent run summaries."""
        preflight_result = ScenarioPreflightResult(
            is_valid=False,
            summary_frame=attachable_frame(
                [{"scenario_count": 2, "issue_count": 3, "is_valid": False}]
            ),
            issues_frame=attachable_frame(
                [
                    {"scenario_id": "dup-a", "issue_code": "duplicate_scenario_id", "issue_message": "duplicate"},
                    {"scenario_id": "dup-b", "issue_code": "duplicate_logical_scenario", "issue_message": "duplicate"},
                    {"scenario_id": "bad-route", "issue_code": "contradictory_route", "issue_message": "contradictory"},
                ]
            ),
        )
        batch_result = TripSearchBatchValidator.build_preflight_blocked_result(preflight_result)
        build_batch_reporting_bundle(batch_result).attach_to_allure("preflight-blocked")

        with soft_assertions():
            assert_that(batch_result.summary_frame.empty).is_true()
            assert_that(batch_result.pack_summary_frame.empty).is_true()
            assert_that(int(batch_result.run_summary_frame.loc[0, "preflight_failed_scenarios"])).is_equal_to(2)
            assert_that(int(batch_result.run_summary_frame.loc[0, "execution_failed_scenarios"])).is_equal_to(0)
            assert_that(batch_result.run_summary_frame.loc[0, "run_id"]).is_equal_to("adhoc")
            assert_that(batch_result.run_summary_frame.loc[0, "run_label"]).is_equal_to("Ad Hoc Run")
            assert_that(int(batch_result.issue_category_frame.set_index("issue_category").loc["preflight", "issue_count"])).is_equal_to(3)


def attachable_frame(rows: list[dict[str, object]]):
    """Build a lightweight dataframe for test-only reporting payloads."""
    import pandas as pd

    return pd.DataFrame(rows)
