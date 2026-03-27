import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Filter Tests")
@allure.sub_suite("Combined Filter Aggregates")
class TestTripSearchCombinedFilterAggregates:
    @allure.title("Carrier and stops filters together reconcile aggregate summaries")
    def test_combined_filter_expects_matching_aggregate_consistency(
        self,
        combined_filtered_expected_aggregate_summary,
        combined_filtered_actual_aggregate_summary,
        combined_filtered_expected_carrier_count_frame,
        combined_filtered_actual_carrier_count_frame,
        combined_filtered_aggregate_comparison_result,
    ):
        attach_dataframe("combined-filter-expected-aggregate-summary", combined_filtered_expected_aggregate_summary)
        attach_dataframe("combined-filter-actual-aggregate-summary", combined_filtered_actual_aggregate_summary)
        attach_dataframe("combined-filter-expected-carrier-counts", combined_filtered_expected_carrier_count_frame)
        attach_dataframe("combined-filter-actual-carrier-counts", combined_filtered_actual_carrier_count_frame)
        attach_dataframe("combined-filter-summary-mismatches", combined_filtered_aggregate_comparison_result.summary_mismatches)
        attach_dataframe("combined-filter-carriers-mismatches", combined_filtered_aggregate_comparison_result.carrier_count_mismatches)
        attach_text(
            "combined-filter-aggregate-summary",
            f"result_count={int(combined_filtered_expected_aggregate_summary.iloc[0]['result_count'])}",
        )

        with soft_assertions():
            assert_that(int(combined_filtered_expected_aggregate_summary.iloc[0]["result_count"]), "Expected assertion for int(combined_filtered_expected_aggregate_summary.iloc[0]['result_count']) to hold").is_equal_to(1)
            assert_that(combined_filtered_aggregate_comparison_result.is_match, "Expected assertion for combined_filtered_aggregate_comparison_result.is_match to hold").is_true()
            assert_that(combined_filtered_aggregate_comparison_result.summary_mismatches.empty, "Expected assertion for combined_filtered_aggregate_comparison_result.summary_mismatches.empty to hold").is_true()
            assert_that(combined_filtered_aggregate_comparison_result.carrier_count_mismatches.empty, "Expected assertion for combined_filtered_aggregate_comparison_result.carrier_count_mismatches.empty to hold").is_true()

    @allure.title("Carrier and stops filters together reconcile empty aggregate summaries")
    def test_combined_filter_expects_matching_aggregate_consistency_for_zero_match_case(
        self,
        combined_no_match_expected_aggregate_summary,
        combined_no_match_actual_aggregate_summary,
        combined_no_match_expected_carrier_count_frame,
        combined_no_match_actual_carrier_count_frame,
        combined_no_match_aggregate_comparison_result,
    ):
        attach_dataframe("combined-no-match-expected-aggregate-summary", combined_no_match_expected_aggregate_summary)
        attach_dataframe("combined-no-match-actual-aggregate-summary", combined_no_match_actual_aggregate_summary)
        attach_dataframe("combined-no-match-expected-carrier-counts", combined_no_match_expected_carrier_count_frame)
        attach_dataframe("combined-no-match-actual-carrier-counts", combined_no_match_actual_carrier_count_frame)
        attach_dataframe("combined-no-match-summary-mismatches", combined_no_match_aggregate_comparison_result.summary_mismatches)
        attach_dataframe("combined-no-match-carriers-mismatches", combined_no_match_aggregate_comparison_result.carrier_count_mismatches)
        attach_text(
            "combined-no-match-aggregate-summary",
            f"result_count={int(combined_no_match_expected_aggregate_summary.iloc[0]['result_count'])}",
        )

        with soft_assertions():
            assert_that(int(combined_no_match_expected_aggregate_summary.iloc[0]["result_count"]), "Expected assertion for int(combined_no_match_expected_aggregate_summary.iloc[0]['result_count']) to hold").is_equal_to(0)
            assert_that(int(combined_no_match_actual_aggregate_summary.iloc[0]["result_count"]), "Expected assertion for int(combined_no_match_actual_aggregate_summary.iloc[0]['result_count']) to hold").is_equal_to(0)
            assert_that(combined_no_match_aggregate_comparison_result.is_match, "Expected assertion for combined_no_match_aggregate_comparison_result.is_match to hold").is_true()
            assert_that(combined_no_match_aggregate_comparison_result.summary_mismatches.empty, "Expected assertion for combined_no_match_aggregate_comparison_result.summary_mismatches.empty to hold").is_true()
            assert_that(combined_no_match_aggregate_comparison_result.carrier_count_mismatches.empty, "Expected assertion for combined_no_match_aggregate_comparison_result.carrier_count_mismatches.empty to hold").is_true()
