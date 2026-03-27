import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe, attach_text


@allure.parent_suite("Trip Search Validation")
@allure.suite("Filter Tests")
@allure.sub_suite("Carrier Filter")
class TestTripSearchCarrierFilter:
    @allure.title("Carrier-filtered search returns only the expected normalized subset")
    def test_carrier_filter_expects_reconciled_results_and_excludes_non_matching_carriers(
        self,
        filtered_expected_trip_frame,
        filtered_actual_trip_frame,
        filtered_reconciliation_result,
        non_matching_carrier_rows,
        search_criteria_nyc_to_bos_rail,
    ):
        attach_dataframe("carrier-filter-expected-trips", filtered_expected_trip_frame)
        attach_dataframe("carrier-filter-actual-trips", filtered_actual_trip_frame)
        attach_dataframe("carrier-filter-missing-trips", filtered_reconciliation_result.missing_rows)
        attach_dataframe("carrier-filter-unexpected-trips", filtered_reconciliation_result.unexpected_rows)
        attach_dataframe("carrier-filter-mismatched-fields", filtered_reconciliation_result.mismatched_fields)
        attach_dataframe("carrier-filter-non-matching-rows", non_matching_carrier_rows)
        attach_text(
            "carrier-filter-summary",
            (
                f"carrier={search_criteria_nyc_to_bos_rail['carrier']}, "
                f"expected_rows={filtered_reconciliation_result.expected_rows_count}, "
                f"actual_rows={filtered_reconciliation_result.actual_rows_count}"
            ),
        )

        with soft_assertions():
            assert_that(filtered_reconciliation_result.is_match, "Expected assertion for filtered_reconciliation_result.is_match to hold").is_true()
            assert_that(non_matching_carrier_rows.empty, "no rows from other carriers are returned").is_true()
            assert_that(filtered_actual_trip_frame.iloc[0]["trip_id"], "Expected assertion for filtered_actual_trip_frame.iloc[0]['trip_id'] to hold").is_equal_to("TRIP-001")
            assert_that(filtered_actual_trip_frame.iloc[0]["carrier"], "Expected assertion for filtered_actual_trip_frame.iloc[0]['carrier'] to hold").is_equal_to(search_criteria_nyc_to_bos_rail["carrier"])
