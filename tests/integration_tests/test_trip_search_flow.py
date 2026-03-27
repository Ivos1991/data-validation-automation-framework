import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe


@allure.parent_suite("Trip Search Validation")
@allure.suite("Integration Tests")
@allure.sub_suite("Dataset To SQLite To Search")
class TestTripSearchIntegration:
    """End-to-end reconciliation tests across dataset, SQLite, and service layers."""

    @allure.title("Dataset normalization, SQLite seeding, search, and reconciliation succeed end to end")
    def test_dataset_to_sqlite_to_search_expects_reconciled_results(
        self,
        integration_expected_trip_frame,
        integration_actual_trip_frame,
        integration_reconciliation_result,
    ):
        """Verify the seeded SQLite search flow reconciles with the normalized dataset."""
        attach_dataframe("integration-expected-trips", integration_expected_trip_frame)
        attach_dataframe("integration-actual-trips", integration_actual_trip_frame)

        with soft_assertions():
            assert_that(len(integration_expected_trip_frame), "expected route/date slice contains two trips").is_equal_to(2)
            assert_that(len(integration_actual_trip_frame), "actual route/date slice contains two trips").is_equal_to(2)
            assert_that(integration_reconciliation_result.is_match, "end-to-end result set reconciles").is_true()
