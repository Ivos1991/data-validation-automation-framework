from pathlib import Path
from uuid import uuid4

import allure
import pytest
from assertpy import assert_that

from src.framework.connectors.files.run_profile_loader import TripSearchRunProfileLoader


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Run Profile Loading")
class TestTripSearchRunProfileLoader:
    @allure.title("Run profile loader reads external run-profile JSON")
    def test_run_profile_loader_reads_external_profile(self, config, run_profile_loader: TripSearchRunProfileLoader):
        run_profile = run_profile_loader.load_json(config.run_profile_path)

        assert_that(run_profile.run_id).is_equal_to("filters-pack-run")
        assert_that(run_profile.run_label).is_equal_to("Filters Pack")
        assert_that(run_profile.selected_pack).is_equal_to("filters")
        assert_that(run_profile.selected_tag).is_none()
        assert_that(run_profile.selected_scenario_type).is_none()

    @allure.title("Run profile loader rejects malformed schema")
    def test_run_profile_loader_rejects_missing_required_fields(self, local_batch_test_dir: Path, run_profile_loader: TripSearchRunProfileLoader):
        invalid_profile_path = local_batch_test_dir / f"missing_run_label_{uuid4().hex}.json"
        invalid_profile_path.write_text('{"run_id": "missing-label"}', encoding="utf-8")

        try:
            run_profile_loader.load_json(invalid_profile_path)
        except ValueError as error:
            assert_that(str(error)).contains("Missing required run-profile fields")
        else:
            raise AssertionError("Expected malformed run profile to fail validation")
        finally:
            if invalid_profile_path.exists():
                invalid_profile_path.unlink()

    @allure.title("Run profile loader rejects blank required values")
    def test_run_profile_loader_rejects_blank_required_values(self, local_batch_test_dir: Path, run_profile_loader: TripSearchRunProfileLoader):
        invalid_profile_path = local_batch_test_dir / f"blank_run_id_{uuid4().hex}.json"
        invalid_profile_path.write_text('{"run_id": " ", "run_label": "Blank Run Id"}', encoding="utf-8")

        try:
            run_profile_loader.load_json(invalid_profile_path)
        except ValueError as error:
            assert_that(str(error)).contains("Run-profile field 'run_id' must not be blank")
        else:
            raise AssertionError("Expected blank run_id profile to fail validation")
        finally:
            if invalid_profile_path.exists():
                invalid_profile_path.unlink()
