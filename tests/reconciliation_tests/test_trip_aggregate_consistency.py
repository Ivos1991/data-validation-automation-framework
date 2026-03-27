import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe


@allure.parent_suite("Trip Search Validation")
@allure.suite("Reconciliation Tests")
@allure.sub_suite("Trip Aggregates")
class TestTripAggregateConsistency:
    """Aggregate-level consistency checks for deterministic trip-search results."""

    @allure.title("Aggregate consistency matches pandas-derived expected summaries with actual search results")
    def test_aggregate_consistency_expects_matching_expected_and_actual_result_sets(
        self,
        expected_aggregate_summary,
        actual_aggregate_summary,
        expected_carrier_count_frame,
        actual_carrier_count_frame,
        aggregate_comparison_result,
    ):
        """Verify summary metrics and grouped carrier counts reconcile."""
        attach_dataframe("expected-aggregate-summary", expected_aggregate_summary)
        attach_dataframe("actual-aggregate-summary", actual_aggregate_summary)
        attach_dataframe("expected-carrier-counts", expected_carrier_count_frame)
        attach_dataframe("actual-carrier-counts", actual_carrier_count_frame)
        attach_dataframe("aggregate-summary-mismatches", aggregate_comparison_result.summary_mismatches)
        attach_dataframe("aggregate-carriers-mismatches", aggregate_comparison_result.carrier_count_mismatches)

        with soft_assertions():
            assert_that(aggregate_comparison_result.is_match, "aggregate summary and carrier counts reconcile").is_true()
            assert_that(aggregate_comparison_result.summary_mismatches.empty, "aggregate summary has no mismatches").is_true()
            assert_that(aggregate_comparison_result.carrier_count_mismatches.empty, "carrier counts have no mismatches").is_true()
