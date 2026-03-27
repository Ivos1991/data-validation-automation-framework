import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Filter Tests")
@allure.sub_suite("Combined Filter")
class TestTripSearchCombinedFilter:
    @allure.title("Carrier and stops filters together return only the expected normalized subset")
    def test_combined_filter_expects_reconciled_results_and_excludes_invalid_rows(
        self,
        combined_filtered_expected_trip_frame,
        combined_filtered_actual_trip_frame,
        combined_filtered_reconciliation_result,
        combined_filter_violating_rows,
        search_criteria_nyc_to_bos_rail_nonstop,
    ):
        attach_dataframe("combined-filter-expected-trips", combined_filtered_expected_trip_frame)
        attach_dataframe("combined-filter-actual-trips", combined_filtered_actual_trip_frame)
        attach_dataframe("combined-filter-missing-trips", combined_filtered_reconciliation_result.missing_rows)
        attach_dataframe("combined-filter-unexpected-trips", combined_filtered_reconciliation_result.unexpected_rows)
        attach_dataframe("combined-filter-mismatched-fields", combined_filtered_reconciliation_result.mismatched_fields)
        attach_dataframe("combined-filter-violating-rows", combined_filter_violating_rows)
        attach_text(
            "combined-filter-summary",
            (
                f"carrier={search_criteria_nyc_to_bos_rail_nonstop['carrier']}, "
                f"stops_count={search_criteria_nyc_to_bos_rail_nonstop['stops_count']}, "
                f"expected_rows={combined_filtered_reconciliation_result.expected_rows_count}, "
                f"actual_rows={combined_filtered_reconciliation_result.actual_rows_count}"
            ),
        )

        with soft_assertions():
            assert_that(combined_filtered_reconciliation_result.is_match, "Expected assertion for combined_filtered_reconciliation_result.is_match to hold").is_true()
            assert_that(combined_filter_violating_rows.empty, "no rows violate carrier or stops filters").is_true()
            assert_that(combined_filtered_actual_trip_frame.iloc[0]["trip_id"], "Expected assertion for combined_filtered_actual_trip_frame.iloc[0]['trip_id'] to hold").is_equal_to("TRIP-001")

    @allure.title("Carrier and stops filters together handle zero-match result sets cleanly")
    def test_combined_filter_expects_zero_match_case_to_be_handled_cleanly(
        self,
        combined_no_match_expected_trip_frame,
        combined_no_match_actual_trip_frame,
        combined_no_match_reconciliation_result,
        search_criteria_nyc_to_bos_rail_one_stop,
    ):
        attach_dataframe("combined-no-match-expected-trips", combined_no_match_expected_trip_frame)
        attach_dataframe("combined-no-match-actual-trips", combined_no_match_actual_trip_frame)
        attach_dataframe("combined-no-match-missing-trips", combined_no_match_reconciliation_result.missing_rows)
        attach_dataframe("combined-no-match-unexpected-trips", combined_no_match_reconciliation_result.unexpected_rows)
        attach_dataframe("combined-no-match-mismatched-fields", combined_no_match_reconciliation_result.mismatched_fields)
        attach_text(
            "combined-no-match-summary",
            (
                f"carrier={search_criteria_nyc_to_bos_rail_one_stop['carrier']}, "
                f"stops_count={search_criteria_nyc_to_bos_rail_one_stop['stops_count']}, "
                f"expected_rows={combined_no_match_reconciliation_result.expected_rows_count}, "
                f"actual_rows={combined_no_match_reconciliation_result.actual_rows_count}"
            ),
        )

        with soft_assertions():
            assert_that(combined_no_match_expected_trip_frame.empty, "Expected assertion for combined_no_match_expected_trip_frame.empty to hold").is_true()
            assert_that(combined_no_match_actual_trip_frame.empty, "Expected assertion for combined_no_match_actual_trip_frame.empty to hold").is_true()
            assert_that(combined_no_match_reconciliation_result.is_match, "Expected assertion for combined_no_match_reconciliation_result.is_match to hold").is_true()
