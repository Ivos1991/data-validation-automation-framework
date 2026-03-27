from pathlib import Path
from uuid import uuid4

import allure
from assertpy import assert_that

from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Run Suite Loading")
class TestTripSearchRunSuiteLoader:
    @allure.title("Run suite loader reads external run-suite JSON")
    def test_run_suite_loader_reads_external_suite(self, config, run_suite_loader: TripSearchRunSuiteLoader):
        run_suite = run_suite_loader.load_json(config.run_suite_path)

        assert_that(run_suite.suite_id).is_equal_to("core-trip-search-suite")
        assert_that(run_suite.suite_label).is_equal_to("Core Trip Search Suite")
        assert_that(run_suite.run_profiles).is_length(2)
        assert_that(run_suite.policy.continue_on_failure).is_true()
        assert_that(run_suite.policy.stop_on_first_failed_run).is_false()
        assert_that(run_suite.run_profiles[0].profile_path.name).is_equal_to("smoke_trip_search_run_profile.json")
        assert_that(run_suite.run_profiles[1].profile_path.name).is_equal_to("default_trip_search_run_profile.json")

    @allure.title("Run suite loader rejects malformed schema")
    def test_run_suite_loader_rejects_missing_required_fields(self, local_batch_test_dir: Path, run_suite_loader: TripSearchRunSuiteLoader):
        invalid_suite_path = local_batch_test_dir / f"missing_suite_label_{uuid4().hex}.json"
        invalid_suite_path.write_text('{"suite_id": "missing-label", "run_profiles": [{"profile_path": "x.json"}]}', encoding="utf-8")

        try:
            run_suite_loader.load_json(invalid_suite_path)
        except ValueError as error:
            assert_that(str(error)).contains("Missing required run-suite fields")
        else:
            raise AssertionError("Expected malformed run suite to fail validation")
        finally:
            if invalid_suite_path.exists():
                invalid_suite_path.unlink()

    @allure.title("Run suite loader rejects empty run profile lists")
    def test_run_suite_loader_rejects_empty_run_profile_list(self, local_batch_test_dir: Path, run_suite_loader: TripSearchRunSuiteLoader):
        invalid_suite_path = local_batch_test_dir / f"empty_run_profiles_{uuid4().hex}.json"
        invalid_suite_path.write_text(
            '{"suite_id": "empty-suite", "suite_label": "Empty Suite", "run_profiles": []}',
            encoding="utf-8",
        )

        try:
            run_suite_loader.load_json(invalid_suite_path)
        except ValueError as error:
            assert_that(str(error)).contains("run_profiles")
        else:
            raise AssertionError("Expected empty run profile list to fail validation")
        finally:
            if invalid_suite_path.exists():
                invalid_suite_path.unlink()

    @allure.title("Run suite loader rejects invalid policy values")
    def test_run_suite_loader_rejects_invalid_policy_values(self, local_batch_test_dir: Path, run_suite_loader: TripSearchRunSuiteLoader):
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

        try:
            run_suite_loader.load_json(invalid_suite_path)
        except ValueError as error:
            assert_that(str(error)).contains("minimum_pass_rate")
        else:
            raise AssertionError("Expected invalid policy values to fail validation")
        finally:
            if invalid_suite_path.exists():
                invalid_suite_path.unlink()
