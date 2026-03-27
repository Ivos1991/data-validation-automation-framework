import allure
from assertpy import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Filter Tests")
@allure.sub_suite("Carrier Filter Aggregates")
class TestTripSearchCarrierFilterAggregates:
    @allure.title("Carrier-filtered search aggregates reconcile with the pandas-derived subset")
    def test_carrier_filtered_aggregate_consistency(
        self,
        filtered_expected_aggregate_summary,
        filtered_actual_aggregate_summary,
        filtered_expected_carrier_count_frame,
        filtered_actual_carrier_count_frame,
        filtered_aggregate_comparison_result,
    ):
        attach_dataframe("carrier-filter-expected-aggregate-summary", filtered_expected_aggregate_summary)
        attach_dataframe("carrier-filter-actual-aggregate-summary", filtered_actual_aggregate_summary)
        attach_dataframe("carrier-filter-expected-carrier-counts", filtered_expected_carrier_count_frame)
        attach_dataframe("carrier-filter-actual-carrier-counts", filtered_actual_carrier_count_frame)
        attach_dataframe("carrier-filter-summary-mismatches", filtered_aggregate_comparison_result.summary_mismatches)
        attach_dataframe("carrier-filter-carriers-mismatches", filtered_aggregate_comparison_result.carrier_count_mismatches)
        attach_text(
            "carrier-filter-aggregate-summary",
            f"result_count={int(filtered_expected_aggregate_summary.iloc[0]['result_count'])}",
        )

        with soft_assertions():
            assert_that(int(filtered_expected_aggregate_summary.iloc[0]["result_count"])).is_equal_to(1)
            assert_that(filtered_aggregate_comparison_result.is_match).is_true()
            assert_that(filtered_aggregate_comparison_result.summary_mismatches.empty).is_true()
            assert_that(filtered_aggregate_comparison_result.carrier_count_mismatches.empty).is_true()
