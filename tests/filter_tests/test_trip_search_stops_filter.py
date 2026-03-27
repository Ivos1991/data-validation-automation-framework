import allure
from assertpy import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Filter Tests")
@allure.sub_suite("Stops Filter")
class TestTripSearchStopsFilter:
    @allure.title("Stops-filtered search returns only the expected normalized subset")
    def test_stops_filtered_results_reconcile_and_exclude_non_matching_stop_counts(
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
            assert_that(stops_filtered_reconciliation_result.is_match).is_true()
            assert_that(stops_filtered_reconciliation_result.missing_rows.empty).is_true()
            assert_that(stops_filtered_reconciliation_result.unexpected_rows.empty).is_true()
            assert_that(stops_filtered_reconciliation_result.mismatched_fields.empty).is_true()
            assert_that(non_matching_stops_rows.empty, "no rows with different stop counts are returned").is_true()
            assert_that(len(stops_filtered_actual_trip_frame)).is_equal_to(1)
            assert_that(stops_filtered_actual_trip_frame.iloc[0]["trip_id"]).is_equal_to("TRIP-001")
            assert_that(int(stops_filtered_actual_trip_frame.iloc[0]["stops_count"])).is_equal_to(
                search_criteria_nyc_to_bos_nonstop["stops_count"]
            )
