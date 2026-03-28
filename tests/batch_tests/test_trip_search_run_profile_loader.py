from pathlib import Path
from uuid import uuid4

import allure
import pytest
from tests.assertions import assert_that

from src.framework.connectors.files.run_profile_loader import TripSearchRunProfileLoader


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Run Profile Loading")
class TestTripSearchRunProfileLoader:
    @allure.title("Run profile loader reads external run-profile JSON")
    def test_run_profile_loader_expects_external_profile_to_load(self, config, run_profile_loader: TripSearchRunProfileLoader):
        run_profile = run_profile_loader.load_json(config.run_profile_path)

        assert_that(run_profile.run_id, "Expected assertion for run_profile.run_id to hold").is_equal_to("filters-pack-run")
        assert_that(run_profile.run_label, "Expected assertion for run_profile.run_label to hold").is_equal_to("Filters Pack")
        assert_that(run_profile.selected_pack, "Expected assertion for run_profile.selected_pack to hold").is_equal_to("filters")
        assert_that(run_profile.selected_tag, "Expected assertion for run_profile.selected_tag to hold").is_none()
        assert_that(run_profile.selected_scenario_type, "Expected assertion for run_profile.selected_scenario_type to hold").is_none()

    @allure.title("Run profile loader rejects malformed schema")
    def test_run_profile_loader_expects_missing_required_fields_to_be_rejected(
        self,
        local_batch_test_dir: Path,
        run_profile_loader: TripSearchRunProfileLoader,
    ):
        invalid_profile_path = local_batch_test_dir / f"missing_run_label_{uuid4().hex}.json"
        invalid_profile_path.write_text('{"run_id": "missing-label"}', encoding="utf-8")

        with pytest.raises(ValueError) as error:
            run_profile_loader.load_json(invalid_profile_path)

        assert_that(str(error.value)).described_as("Malformed run profiles should fail schema validation").contains(
            "Missing required run-profile fields"
        )

