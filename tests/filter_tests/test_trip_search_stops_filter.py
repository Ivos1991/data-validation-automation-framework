import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Filter Tests")
@allure.sub_suite("Stops Filter")
class TestTripSearchStopsFilter:
    @allure.title("Stops-filtered search returns only the expected normalized subset")
    def test_stops_filter_expects_reconciled_results_and_excludes_non_matching_stop_counts(
        self,
        stops_filtered_expected_trip_frame,
        stops_filtered_actual_trip_frame,
        stops_filtered_reconciliation_result,
        non_matching_stops_rows,
        search_criteria_nyc_to_bos_nonstop,
    ):
        attach_dataframe("stops-filter-expected-trips", stops_filtered_expected_trip_frame)
        attach_dataframe("stops-filter-actual-trips", stops_filtered_actual_trip_frame)
        attach_dataframe("stops-filter-missing-trips", stops_filtered_reconciliation_result.missing_rows)
        attach_dataframe("stops-filter-unexpected-trips", stops_filtered_reconciliation_result.unexpected_rows)
        attach_dataframe("stops-filter-mismatched-fields", stops_filtered_reconciliation_result.mismatched_fields)
        attach_dataframe("stops-filter-non-matching-rows", non_matching_stops_rows)
        attach_text(
            "stops-filter-summary",
            (
                f"stops_count={search_criteria_nyc_to_bos_nonstop['stops_count']}, "
                f"expected_rows={stops_filtered_reconciliation_result.expected_rows_count}, "
                f"actual_rows={stops_filtered_reconciliation_result.actual_rows_count}"
            ),
        )

        with soft_assertions():
            assert_that(stops_filtered_reconciliation_result.is_match, "Expected assertion for stops_filtered_reconciliation_result.is_match to hold").is_true()
            assert_that(stops_filtered_reconciliation_result.missing_rows.empty, "Expected assertion for stops_filtered_reconciliation_result.missing_rows.empty to hold").is_true()
            assert_that(stops_filtered_reconciliation_result.unexpected_rows.empty, "Expected assertion for stops_filtered_reconciliation_result.unexpected_rows.empty to hold").is_true()
            assert_that(stops_filtered_reconciliation_result.mismatched_fields.empty, "Expected assertion for stops_filtered_reconciliation_result.mismatched_fields.empty to hold").is_true()
            assert_that(non_matching_stops_rows.empty, "no rows with different stop counts are returned").is_true()
            assert_that(len(stops_filtered_actual_trip_frame), "Expected assertion for len(stops_filtered_actual_trip_frame) to hold").is_equal_to(1)
            assert_that(stops_filtered_actual_trip_frame.iloc[0]["trip_id"], "Expected assertion for stops_filtered_actual_trip_frame.iloc[0]['trip_id'] to hold").is_equal_to("TRIP-001")
            assert_that(int(stops_filtered_actual_trip_frame.iloc[0]["stops_count"]), "Expected assertion for int(stops_filtered_actual_trip_frame.iloc[0]['stops_count']) to hold").is_equal_to(
                search_criteria_nyc_to_bos_nonstop["stops_count"]
            )
