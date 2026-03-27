import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Reconciliation Tests")
@allure.sub_suite("Trip Search")
class TestTripReconciliation:
    """Row-level reconciliation tests for expected versus actual trips."""

    @allure.title("Reconciliation matches pandas-derived expected results with actual search results")
    def test_reconciliation_expects_matching_expected_and_actual_result_sets(
        self,
        normalized_expected_trip_frame,
        actual_trip_frame,
        reconciliation_result,
    ):
        """Verify the baseline deterministic slice reconciles cleanly."""
        attach_dataframe("expected-trips", normalized_expected_trip_frame)
        attach_dataframe("actual-trips", actual_trip_frame)
        attach_dataframe("missing-trips", reconciliation_result.missing_rows)
        attach_dataframe("unexpected-trips", reconciliation_result.unexpected_rows)
        attach_dataframe("mismatched-fields", reconciliation_result.mismatched_fields)
        attach_text(
            "reconciliation-summary",
            f"expected_rows={reconciliation_result.expected_rows_count}, actual_rows={reconciliation_result.actual_rows_count}",
        )

        with soft_assertions():
            assert_that(reconciliation_result.is_match, "expected and actual result sets fully reconcile").is_true()
            assert_that(reconciliation_result.missing_rows.empty, "no expected trips are missing").is_true()
            assert_that(reconciliation_result.unexpected_rows.empty, "no unexpected trips are returned").is_true()
            assert_that(reconciliation_result.mismatched_fields.empty, "no field mismatches exist").is_true()
