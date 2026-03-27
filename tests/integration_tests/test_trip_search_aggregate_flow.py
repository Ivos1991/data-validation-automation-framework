import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe


@allure.parent_suite("Trip Search Validation")
@allure.suite("Integration Tests")
@allure.sub_suite("Aggregate Consistency")
class TestTripSearchAggregateIntegration:
    """End-to-end aggregate validation tests for the SQLite-backed search flow."""

    @allure.title("Dataset normalization, SQLite seeding, search, and aggregate comparison succeed end to end")
    def test_dataset_to_sqlite_to_search_expects_matching_aggregates(
        self,
        integration_expected_aggregate_summary,
        integration_actual_aggregate_summary,
        integration_expected_carrier_count_frame,
        integration_actual_carrier_count_frame,
        integration_aggregate_result,
    ):
        """Verify aggregate summaries stay aligned through the integration path."""
        attach_dataframe("integration-expected-aggregate-summary", integration_expected_aggregate_summary)
        attach_dataframe("integration-actual-aggregate-summary", integration_actual_aggregate_summary)
        attach_dataframe("integration-expected-carrier-counts", integration_expected_carrier_count_frame)
        attach_dataframe("integration-actual-carrier-counts", integration_actual_carrier_count_frame)

        with soft_assertions():
            assert_that(int(integration_expected_aggregate_summary.iloc[0]["result_count"]), "Expected assertion for int(integration_expected_aggregate_summary.iloc[0]['result_count']) to hold").is_equal_to(2)
            assert_that(integration_aggregate_result.is_match, "end-to-end aggregate comparison reconciles").is_true()
            assert_that(integration_aggregate_result.summary_mismatches.empty, "Expected assertion for integration_aggregate_result.summary_mismatches.empty to hold").is_true()
            assert_that(integration_aggregate_result.carrier_count_mismatches.empty, "Expected assertion for integration_aggregate_result.carrier_count_mismatches.empty to hold").is_true()
