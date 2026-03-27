import allure
import pandas as pd
from tests.assertions import assert_that, soft_assertions

from src.domain.trip_search.search_service import search_by_route_and_departure_date
from src.domain.trip_search.search_service_request import SearchServiceRequest
from src.framework.reporting.allure_helpers import attach_dataframe
from src.framework.reporting.trip_search_reporting import build_batch_reporting_bundle, build_suite_reporting_bundle
from src.framework.utils.dataframe_utils import build_aggregate_summary, build_carrier_count_frame, filter_expected_trip_frame
from src.validators.aggregate.trip_aggregate_validator import TripAggregateValidator
from src.validators.reconciliation.trip_reconciliation_validator import TripReconciliationValidator


def _scenario_by_id(scenarios, scenario_id: str):
    return next(scenario for scenario in scenarios if scenario.scenario_id == scenario_id)


@allure.parent_suite("Trip Search Validation")
@allure.suite("Large Dataset Tests")
@allure.sub_suite("Synthetic Dataset")
class TestLargeSyntheticDataset:
    @allure.title("Deterministic larger synthetic dataset builds a stable high-variation trip set")
    def test_large_synthetic_dataset_builder_expects_expected_profile_counts(
        self,
        large_raw_trip_frame: pd.DataFrame,
        large_dataset_profile_frame: pd.DataFrame,
    ):
        attach_dataframe("large-dataset-profile", large_dataset_profile_frame)

        with soft_assertions():
            assert_that(len(large_raw_trip_frame), "Expected assertion for len(large_raw_trip_frame) to hold").is_equal_to(720)
            assert_that(int(large_dataset_profile_frame.loc[0, "row_count"]), "Expected assertion for int(large_dataset_profile_frame.loc[0, 'row_count']) to hold").is_equal_to(720)
            assert_that(int(large_dataset_profile_frame.loc[0, "unique_origins"]), "Expected assertion for int(large_dataset_profile_frame.loc[0, 'unique_origins']) to hold").is_equal_to(5)
            assert_that(int(large_dataset_profile_frame.loc[0, "unique_departure_dates"]), "Expected assertion for int(large_dataset_profile_frame.loc[0, 'unique_departure_dates']) to hold").is_equal_to(4)
            assert_that(sorted(large_raw_trip_frame["carrier"].unique().tolist()), "Expected assertion for sorted(large_raw_trip_frame['carrier'].unique().tolist()) to hold").is_equal_to(["AmRail", "BudgetBus", "SkyJet"])
            assert_that(sorted(large_raw_trip_frame["stops_count"].unique().tolist()), "Expected assertion for sorted(large_raw_trip_frame['stops_count'].unique().tolist()) to hold").is_equal_to([0, 1, 2])
            assert_that(large_raw_trip_frame["trip_id"].is_unique, "Expected assertion for large_raw_trip_frame['trip_id'].is_unique to hold").is_true()

    @allure.title("Row-level reconciliation stays correct on a selected larger-data combined-filter scenario")
    def test_large_synthetic_dataset_expects_row_level_reconciliation_to_match(
        self,
        large_batch_scenarios,
        large_expected_trip_frame: pd.DataFrame,
        large_trip_search_service_api,
        large_dataset_profile_frame: pd.DataFrame,
    ):
        scenario = _scenario_by_id(large_batch_scenarios, "large-combined-nyc-was-amrail-nonstop")
        expected_subset = filter_expected_trip_frame(large_expected_trip_frame, **scenario.to_search_filters())
        actual_trips = search_by_route_and_departure_date(
            large_trip_search_service_api,
            SearchServiceRequest.build(**scenario.to_search_filters()),
        )
        actual_trip_frame = pd.DataFrame([trip.to_canonical_dict() for trip in actual_trips], columns=expected_subset.columns)
        reconciliation_result = TripReconciliationValidator().reconcile(expected_subset, actual_trip_frame)

        attach_dataframe("large-row-level-dataset-profile", large_dataset_profile_frame)
        attach_dataframe("large-row-level-expected-trips", expected_subset)
        attach_dataframe("large-row-level-actual-trips", actual_trip_frame)

        with soft_assertions():
            assert_that(reconciliation_result.is_match, "Expected assertion for reconciliation_result.is_match to hold").is_true()
            assert_that(len(expected_subset), "Expected assertion for len(expected_subset) to hold").is_equal_to(1)
            assert_that(len(actual_trip_frame), "Expected assertion for len(actual_trip_frame) to hold").is_equal_to(1)
            assert_that(actual_trip_frame.loc[0, "carrier"], "Expected assertion for actual_trip_frame.loc[0, 'carrier'] to hold").is_equal_to("AmRail")
            assert_that(int(actual_trip_frame.loc[0, "stops_count"]), "Expected assertion for int(actual_trip_frame.loc[0, 'stops_count']) to hold").is_equal_to(0)

    @allure.title("Aggregate validation stays consistent on a selected larger-data route-date scenario")
    def test_large_synthetic_dataset_expects_aggregate_consistency_to_match(
        self,
        config,
        large_batch_scenarios,
        large_expected_trip_frame: pd.DataFrame,
        large_trip_search_service_api,
    ):
        scenario = _scenario_by_id(large_batch_scenarios, "large-route-date-nyc-bos")
        expected_subset = filter_expected_trip_frame(large_expected_trip_frame, **scenario.to_search_filters())
        actual_trips = search_by_route_and_departure_date(
            large_trip_search_service_api,
            SearchServiceRequest.build(**scenario.to_search_filters()),
        )
        actual_trip_frame = pd.DataFrame([trip.to_canonical_dict() for trip in actual_trips], columns=expected_subset.columns)
        aggregate_result = TripAggregateValidator(config.numeric_tolerance).validate(
            expected_summary=build_aggregate_summary(expected_subset),
            actual_summary=build_aggregate_summary(actual_trip_frame),
            expected_carrier_counts=build_carrier_count_frame(expected_subset),
            actual_carrier_counts=build_carrier_count_frame(actual_trip_frame),
        )

        attach_dataframe("large-aggregate-expected-trips", expected_subset)
        attach_dataframe("large-aggregate-actual-trips", actual_trip_frame)

        with soft_assertions():
            assert_that(len(expected_subset), "Expected assertion for len(expected_subset) to hold").is_equal_to(9)
            assert_that(aggregate_result.is_match, "Expected assertion for aggregate_result.is_match to hold").is_true()
            assert_that(sorted(actual_trip_frame["carrier"].unique().tolist()), "Expected assertion for sorted(actual_trip_frame['carrier'].unique().tolist()) to hold").is_equal_to(["AmRail", "BudgetBus", "SkyJet"])

    @allure.title("Batch validation executes a broader larger-data scenario pack successfully")
    def test_large_synthetic_dataset_expects_batch_execution_to_pass(
        self,
        large_batch_scenario_dataset,
        large_batch_scenarios,
        large_expected_trip_frame: pd.DataFrame,
        large_batch_validator,
        large_dataset_profile_frame: pd.DataFrame,
    ):
        batch_result = large_batch_validator.validate(large_batch_scenarios, large_expected_trip_frame, dataset_profile="large")

        attach_dataframe("large-batch-dataset-profile", large_dataset_profile_frame)
        attach_dataframe("large-batch-loaded-scenario-table", large_batch_scenario_dataset.scenario_frame)
        attach_dataframe("large-batch-preflight-summary", large_batch_scenario_dataset.preflight_result.summary_frame)
        build_batch_reporting_bundle(batch_result).attach_to_allure("large-batch")

        with soft_assertions():
            assert_that(len(batch_result.scenario_results), "Expected assertion for len(batch_result.scenario_results) to hold").is_equal_to(9)
            assert_that(batch_result.summary_frame["is_pass"].tolist(), "Expected assertion for batch_result.summary_frame['is_pass'].tolist() to hold").contains_only(*([True] * 9))
            assert_that(batch_result.run_summary_frame.loc[0, "dataset_profile"], "Expected assertion for batch_result.run_summary_frame.loc[0, 'dataset_profile'] to hold").is_equal_to("large")
            assert_that(batch_result.run_summary_frame.loc[0, "scenario_dataset_asset"], "Expected assertion for batch_result.run_summary_frame.loc[0, 'scenario_dataset_asset'] to hold").is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(int(batch_result.run_summary_frame.loc[0, "total_scenarios"]), "Expected assertion for int(batch_result.run_summary_frame.loc[0, 'total_scenarios']) to hold").is_equal_to(9)
            assert_that(int(batch_result.run_summary_frame.loc[0, "passed_scenarios"]), "Expected assertion for int(batch_result.run_summary_frame.loc[0, 'passed_scenarios']) to hold").is_equal_to(9)
            assert_that(int(batch_result.issue_category_frame.set_index("issue_category").loc["row_reconciliation", "issue_count"]), "Expected assertion for int(batch_result.issue_category_frame.set_index('issue_category').loc['row_reconciliati... to hold").is_equal_to(0)
            assert_that(batch_result.pack_summary_frame["pack"].tolist(), "Expected assertion for batch_result.pack_summary_frame['pack'].tolist() to hold").is_equal_to(["filters", "regression", "smoke"])

    @allure.title("Suite execution stays stable on a realistic larger-data run pack")
    def test_large_synthetic_dataset_expects_suite_execution_to_pass(
        self,
        large_run_suite,
        large_batch_scenarios,
        large_expected_trip_frame: pd.DataFrame,
        large_run_suite_executor,
        large_dataset_profile_frame: pd.DataFrame,
    ):
        suite_result = large_run_suite_executor.execute(
            large_run_suite,
            large_batch_scenarios,
            large_expected_trip_frame,
            dataset_profile="large",
        )

        attach_dataframe("large-suite-dataset-profile", large_dataset_profile_frame)
        build_suite_reporting_bundle(suite_result).attach_to_allure("large-suite")

        suite_summary = suite_result.suite_summary_frame.iloc[0]

        with soft_assertions():
            assert_that(suite_summary["suite_status"], "Expected assertion for suite_summary['suite_status'] to hold").is_equal_to("passed")
            assert_that(suite_summary["dataset_profile"], "Expected assertion for suite_summary['dataset_profile'] to hold").is_equal_to("large")
            assert_that(suite_summary["scenario_dataset_asset"], "Expected assertion for suite_summary['scenario_dataset_asset'] to hold").is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(int(suite_summary["total_runs"]), "Expected assertion for int(suite_summary['total_runs']) to hold").is_equal_to(3)
            assert_that(int(suite_summary["total_scenarios_executed"]), "Expected assertion for int(suite_summary['total_scenarios_executed']) to hold").is_equal_to(9)
            assert_that(int(suite_summary["total_passed_scenarios"]), "Expected assertion for int(suite_summary['total_passed_scenarios']) to hold").is_equal_to(9)
            assert_that(int(suite_summary["total_failed_scenarios"]), "Expected assertion for int(suite_summary['total_failed_scenarios']) to hold").is_equal_to(0)
            assert_that(suite_result.suite_run_summary_frame["run_id"].tolist(), "Expected assertion for suite_result.suite_run_summary_frame['run_id'].tolist() to hold").is_equal_to(
                ["large-smoke-run", "large-filters-run", "large-regression-run"]
            )
            assert_that(suite_result.suite_run_summary_frame["dataset_profile"].tolist(), "Expected assertion for suite_result.suite_run_summary_frame['dataset_profile'].tolist() to hold").is_equal_to(["large", "large", "large"])
            assert_that(suite_result.suite_run_summary_frame["scenario_dataset_asset"].tolist(), "Expected assertion for suite_result.suite_run_summary_frame['scenario_dataset_asset'].tolist() to hold").is_equal_to(
                ["large_batch_trip_search_scenarios.csv", "large_batch_trip_search_scenarios.csv", "large_batch_trip_search_scenarios.csv"]
            )
