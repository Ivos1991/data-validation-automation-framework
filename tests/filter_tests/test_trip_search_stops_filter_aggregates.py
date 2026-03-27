import allure
from assertpy import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Filter Tests")
@allure.sub_suite("Stops Filter Aggregates")
class TestTripSearchStopsFilterAggregates:
    @allure.title("Stops-filtered search aggregates reconcile with the pandas-derived subset")
    def test_stops_filtered_aggregate_consistency(
        self,
        stops_filtered_expected_aggregate_summary,
        stops_filtered_actual_aggregate_summary,
        stops_filtered_expected_carrier_count_frame,
        stops_filtered_actual_carrier_count_frame,
        stops_filtered_aggregate_comparison_result,
    ):
        attach_dataframe("stops-filter-expected-aggregate-summary", stops_filtered_expected_aggregate_summary)
        attach_dataframe("stops-filter-actual-aggregate-summary", stops_filtered_actual_aggregate_summary)
        attach_dataframe("stops-filter-expected-carrier-counts", stops_filtered_expected_carrier_count_frame)
        attach_dataframe("stops-filter-actual-carrier-counts", stops_filtered_actual_carrier_count_frame)
        attach_dataframe("stops-filter-summary-mismatches", stops_filtered_aggregate_comparison_result.summary_mismatches)
        attach_dataframe("stops-filter-carriers-mismatches", stops_filtered_aggregate_comparison_result.carrier_count_mismatches)
        attach_text(
            "stops-filter-aggregate-summary",
            f"result_count={int(stops_filtered_expected_aggregate_summary.iloc[0]['result_count'])}",
        )

        with soft_assertions():
            assert_that(int(stops_filtered_expected_aggregate_summary.iloc[0]["result_count"])).is_equal_to(1)
            assert_that(stops_filtered_aggregate_comparison_result.is_match).is_true()
            assert_that(stops_filtered_aggregate_comparison_result.summary_mismatches.empty).is_true()
            assert_that(stops_filtered_aggregate_comparison_result.carrier_count_mismatches.empty).is_true()
