import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import allure
import pytest
from tests.assertions import assert_that


@allure.parent_suite("Trip Search Validation")
@allure.suite("Large Dataset Tests")
@allure.sub_suite("Dataset Profiles")
class TestDatasetProfileSelection:
    @allure.title("Default config keeps the small deterministic dataset profile")
    def test_dataset_profile_expects_small_profile_by_default(self, loaded_trip_dataset):
        assert_that(loaded_trip_dataset.dataset_profile, "Expected assertion for loaded_trip_dataset.dataset_profile to hold").is_equal_to("small")
        assert_that(len(loaded_trip_dataset.raw_trip_frame), "Expected assertion for len(loaded_trip_dataset.raw_trip_frame) to hold").is_equal_to(5)
        assert_that(loaded_trip_dataset.scenario_dataset_path.name, "Expected assertion for loaded_trip_dataset.scenario_dataset_path.name to hold").is_equal_to("batch_trip_search_scenarios.csv")
        assert_that(loaded_trip_dataset.default_run_profile_path.name, "Expected assertion for loaded_trip_dataset.default_run_profile_path.name to hold").is_equal_to("default_trip_search_run_profile.json")
        assert_that(loaded_trip_dataset.default_run_suite_path.name, "Expected assertion for loaded_trip_dataset.default_run_suite_path.name to hold").is_equal_to("default_trip_search_run_suite.json")

    @allure.title("Explicit large dataset profile resolves the larger synthetic dataset")
    def test_dataset_profile_expects_large_profile_to_resolve_explicitly(self, config, trip_dataset_context_loader):
        loaded_dataset = trip_dataset_context_loader.load(config, dataset_profile="large")

        assert_that(loaded_dataset.dataset_profile, "Expected assertion for loaded_dataset.dataset_profile to hold").is_equal_to("large")
        assert_that(len(loaded_dataset.raw_trip_frame), "Expected assertion for len(loaded_dataset.raw_trip_frame) to hold").is_equal_to(720)
        assert_that(loaded_dataset.scenario_dataset_path.name, "Expected assertion for loaded_dataset.scenario_dataset_path.name to hold").is_equal_to("large_batch_trip_search_scenarios.csv")
        assert_that(loaded_dataset.default_run_profile_path.name, "Expected assertion for loaded_dataset.default_run_profile_path.name to hold").is_equal_to("large_filters_trip_search_run_profile.json")
        assert_that(loaded_dataset.default_run_suite_path.name, "Expected assertion for loaded_dataset.default_run_suite_path.name to hold").is_equal_to("large_trip_search_run_suite.json")

    @allure.title("Run-profile metadata can drive dataset profile selection")
    def test_dataset_profile_expects_run_profile_to_select_dataset_profile(
        self,
        local_large_test_dir: Path,
        large_run_profile_loader,
        trip_dataset_context_loader,
        config,
    ):
        run_profile_path = local_large_test_dir / f"dataset_profile_run_{uuid4().hex}.json"
        run_profile_path.write_text(
            json.dumps(
                {
                    "run_id": "large-profile-run",
                    "run_label": "Large Profile Run",
                    "dataset_profile": "large",
                    "selected_pack": "filters",
                }
            ),
            encoding="utf-8",
        )

        run_profile = large_run_profile_loader.load_json(run_profile_path)
        loaded_dataset = trip_dataset_context_loader.load(config, run_profile=run_profile)

        assert_that(run_profile.dataset_profile, "Expected assertion for run_profile.dataset_profile to hold").is_equal_to("large")
        assert_that(loaded_dataset.dataset_profile, "Expected assertion for loaded_dataset.dataset_profile to hold").is_equal_to("large")
        assert_that(len(loaded_dataset.expected_trip_frame), "Expected assertion for len(loaded_dataset.expected_trip_frame) to hold").is_equal_to(720)
        assert_that(loaded_dataset.scenario_dataset_path.name, "Expected assertion for loaded_dataset.scenario_dataset_path.name to hold").is_equal_to("large_batch_trip_search_scenarios.csv")

    @allure.title("Suite metadata can drive dataset profile selection")
    def test_dataset_profile_expects_run_suite_to_select_dataset_profile(
        self,
        config,
        trip_dataset_context_loader,
        large_run_suite,
    ):
        loaded_dataset = trip_dataset_context_loader.load(config, run_suite=large_run_suite)

        assert_that(large_run_suite.dataset_profile, "Expected assertion for large_run_suite.dataset_profile to hold").is_equal_to("large")
        assert_that(loaded_dataset.dataset_profile, "Expected assertion for loaded_dataset.dataset_profile to hold").is_equal_to("large")
        assert_that(len(loaded_dataset.raw_trip_frame), "Expected assertion for len(loaded_dataset.raw_trip_frame) to hold").is_equal_to(720)
        assert_that(loaded_dataset.scenario_dataset_path.name, "Expected assertion for loaded_dataset.scenario_dataset_path.name to hold").is_equal_to("large_batch_trip_search_scenarios.csv")

    @allure.title("Unknown dataset profiles are rejected clearly")
    def test_dataset_profile_expects_unknown_values_to_be_rejected(self, config, trip_dataset_context_loader):
        invalid_config = replace(config, dataset_profile="unknown")

        with pytest.raises(ValueError) as error:
            trip_dataset_context_loader.load(invalid_config)

        assert_that(str(error.value)).described_as("Unknown dataset profiles should fail validation").contains(
            "must be one of"
        )

    @allure.title("Incomplete profile asset configuration fails clearly")
    def test_dataset_profile_expects_missing_profile_assets_to_be_rejected(
        self,
        local_large_test_dir: Path,
        config,
        trip_dataset_context_loader,
    ):
        invalid_config = replace(config, scenario_dataset_path=local_large_test_dir / "missing_scenarios.csv")

        with pytest.raises(FileNotFoundError) as error:
            trip_dataset_context_loader.load(invalid_config)

        assert_that(str(error.value)).described_as("Missing profile assets should fail clearly").contains(
            "scenario_dataset_path"
        )
