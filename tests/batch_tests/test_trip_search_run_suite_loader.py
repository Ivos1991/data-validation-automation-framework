from pathlib import Path
from uuid import uuid4

import allure
import pytest
from tests.assertions import assert_that

from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Run Suite Loading")
class TestTripSearchRunSuiteLoader:
    @allure.title("Run suite loader reads external run-suite JSON")
    def test_run_suite_loader_expects_external_suite_to_load(self, config, run_suite_loader: TripSearchRunSuiteLoader):
        run_suite = run_suite_loader.load_json(config.run_suite_path)

        assert_that(run_suite.suite_id, "Expected assertion for run_suite.suite_id to hold").is_equal_to("core-trip-search-suite")
        assert_that(run_suite.suite_label, "Expected assertion for run_suite.suite_label to hold").is_equal_to("Core Trip Search Suite")
        assert_that(run_suite.run_profiles, "Expected assertion for run_suite.run_profiles to hold").is_length(2)
        assert_that(run_suite.policy.continue_on_failure, "Expected assertion for run_suite.policy.continue_on_failure to hold").is_true()
        assert_that(run_suite.policy.stop_on_first_failed_run, "Expected assertion for run_suite.policy.stop_on_first_failed_run to hold").is_false()
        assert_that(run_suite.run_profiles[0].profile_path.name, "Expected assertion for run_suite.run_profiles[0].profile_path.name to hold").is_equal_to("smoke_trip_search_run_profile.json")
        assert_that(run_suite.run_profiles[1].profile_path.name, "Expected assertion for run_suite.run_profiles[1].profile_path.name to hold").is_equal_to("default_trip_search_run_profile.json")

    @allure.title("Run suite loader rejects malformed schema")
    def test_run_suite_loader_expects_missing_required_fields_to_be_rejected(
        self,
        local_batch_test_dir: Path,
        run_suite_loader: TripSearchRunSuiteLoader,
    ):
        invalid_suite_path = local_batch_test_dir / f"missing_suite_label_{uuid4().hex}.json"
        invalid_suite_path.write_text('{"suite_id": "missing-label", "run_profiles": [{"profile_path": "x.json"}]}', encoding="utf-8")

        with pytest.raises(ValueError) as error:
            run_suite_loader.load_json(invalid_suite_path)

        assert_that(str(error.value)).described_as("Malformed suites should fail schema validation").contains(
            "Missing required run-suite fields"
        )

    @allure.title("Run suite loader rejects empty run profile lists")
    def test_run_suite_loader_expects_empty_run_profile_list_to_be_rejected(
        self,
        local_batch_test_dir: Path,
        run_suite_loader: TripSearchRunSuiteLoader,
    ):
        invalid_suite_path = local_batch_test_dir / f"empty_run_profiles_{uuid4().hex}.json"
        invalid_suite_path.write_text(
            '{"suite_id": "empty-suite", "suite_label": "Empty Suite", "run_profiles": []}',
            encoding="utf-8",
        )

        with pytest.raises(ValueError) as error:
            run_suite_loader.load_json(invalid_suite_path)

        assert_that(str(error.value)).described_as("Suites must contain at least one run-profile reference").contains(
            "run_profiles"
        )

    @allure.title("Run suite loader rejects invalid policy values")
    def test_run_suite_loader_expects_invalid_policy_values_to_be_rejected(
        self,
        local_batch_test_dir: Path,
        run_suite_loader: TripSearchRunSuiteLoader,
    ):
        invalid_suite_path = local_batch_test_dir / f"invalid_policy_{uuid4().hex}.json"
        invalid_suite_path.write_text(
            (
                '{'
                '"suite_id": "invalid-policy-suite", '
                '"suite_label": "Invalid Policy Suite", '
                '"policy": {"minimum_pass_rate": 1.5}, '
                '"run_profiles": [{"profile_path": "x.json"}]'
                '}'
            ),
            encoding="utf-8",
        )

        with pytest.raises(ValueError) as error:
            run_suite_loader.load_json(invalid_suite_path)

        assert_that(str(error.value)).described_as("Invalid suite policy values should fail validation").contains(
            "minimum_pass_rate"
        )
