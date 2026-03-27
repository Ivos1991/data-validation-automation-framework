import allure
from assertpy import assert_that, soft_assertions

from src.framework.reporting.allure_helpers import attach_dataframe


@allure.parent_suite("Trip Search Validation")
@allure.suite("Data Quality Tests")
@allure.sub_suite("Trip Dataset")
class TestTripDataQuality:
    """Dataset-quality checks for canonical trip inputs."""

    @allure.title("Duplicate trip IDs are reported as a data-quality violation")
    def test_duplicate_trip_ids_are_detected(self, trip_data_quality_validator, source_dataset_with_duplicate_trip_id):
        """Verify duplicate trip identifiers are surfaced as quality issues."""
        result = trip_data_quality_validator.validate(source_dataset_with_duplicate_trip_id)
        attach_dataframe("duplicate-trip-id-rows", result.duplicate_trip_ids)

        with soft_assertions():
            assert_that(result.is_valid, "dataset with duplicate trip IDs should be invalid").is_false()
            assert_that(result.duplicate_trip_ids.empty, "duplicate trip rows should be reported").is_false()
            assert_that(result.duplicate_trip_ids["trip_id"].tolist()).contains("TRIP-001")

    @allure.title("Invalid departure dates are reported as a data-quality violation")
    def test_invalid_departure_dates_are_detected(self, trip_data_quality_validator, source_dataset_with_invalid_departure_date):
        """Verify malformed departure dates are surfaced as quality issues."""
        result = trip_data_quality_validator.validate(source_dataset_with_invalid_departure_date)
        attach_dataframe("invalid-departure-date-rows", result.invalid_departure_dates)

        with soft_assertions():
            assert_that(result.is_valid, "dataset with invalid departure dates should be invalid").is_false()
            assert_that(result.invalid_departure_dates.empty, "invalid departure date rows should be reported").is_false()
            assert_that(result.invalid_departure_dates.iloc[0]["trip_id"]).is_equal_to("TRIP-001")
