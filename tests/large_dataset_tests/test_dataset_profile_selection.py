import json
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import allure
from assertpy import assert_that


@allure.parent_suite("Trip Search Validation")
@allure.suite("Large Dataset Tests")
@allure.sub_suite("Dataset Profiles")
class TestDatasetProfileSelection:
    @allure.title("Default config keeps the small deterministic dataset profile")
    def test_dataset_profile_defaults_to_small(self, loaded_trip_dataset):
        assert_that(loaded_trip_dataset.dataset_profile).is_equal_to("small")
        assert_that(len(loaded_trip_dataset.raw_trip_frame)).is_equal_to(5)
        assert_that(loaded_trip_dataset.scenario_dataset_path.name).is_equal_to("batch_trip_search_scenarios.csv")
        assert_that(loaded_trip_dataset.default_run_profile_path.name).is_equal_to("default_trip_search_run_profile.json")
        assert_that(loaded_trip_dataset.default_run_suite_path.name).is_equal_to("default_trip_search_run_suite.json")

    @allure.title("Explicit large dataset profile resolves the larger synthetic dataset")
    def test_dataset_profile_can_resolve_large_dataset_explicitly(self, config, trip_dataset_context_loader):
        loaded_dataset = trip_dataset_context_loader.load(config, dataset_profile="large")

        assert_that(loaded_dataset.dataset_profile).is_equal_to("large")
        assert_that(len(loaded_dataset.raw_trip_frame)).is_equal_to(720)
        assert_that(loaded_dataset.scenario_dataset_path.name).is_equal_to("large_batch_trip_search_scenarios.csv")
        assert_that(loaded_dataset.default_run_profile_path.name).is_equal_to("large_filters_trip_search_run_profile.json")
        assert_that(loaded_dataset.default_run_suite_path.name).is_equal_to("large_trip_search_run_suite.json")

    @allure.title("Run-profile metadata can drive dataset profile selection")
    def test_dataset_profile_can_be_selected_from_run_profile(
        self,
        large_run_profile_loader,
        trip_dataset_context_loader,
        config,
    ):
        local_test_dir = Path(__file__).resolve().parent / ".local_test_data"
        local_test_dir.mkdir(exist_ok=True)
        run_profile_path = local_test_dir / f"dataset_profile_run_{uuid4().hex}.json"
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

        try:
            run_profile = large_run_profile_loader.load_json(run_profile_path)
            loaded_dataset = trip_dataset_context_loader.load(config, run_profile=run_profile)

            assert_that(run_profile.dataset_profile).is_equal_to("large")
            assert_that(loaded_dataset.dataset_profile).is_equal_to("large")
            assert_that(len(loaded_dataset.expected_trip_frame)).is_equal_to(720)
            assert_that(loaded_dataset.scenario_dataset_path.name).is_equal_to("large_batch_trip_search_scenarios.csv")
        finally:
            if run_profile_path.exists():
                run_profile_path.unlink()

    @allure.title("Suite metadata can drive dataset profile selection")
    def test_dataset_profile_can_be_selected_from_run_suite(
        self,
        config,
        trip_dataset_context_loader,
        large_run_suite,
    ):
        loaded_dataset = trip_dataset_context_loader.load(config, run_suite=large_run_suite)

        assert_that(large_run_suite.dataset_profile).is_equal_to("large")
        assert_that(loaded_dataset.dataset_profile).is_equal_to("large")
        assert_that(len(loaded_dataset.raw_trip_frame)).is_equal_to(720)
        assert_that(loaded_dataset.scenario_dataset_path.name).is_equal_to("large_batch_trip_search_scenarios.csv")

    @allure.title("Unknown dataset profiles are rejected clearly")
    def test_dataset_profile_rejects_unknown_values(self, config, trip_dataset_context_loader):
        invalid_config = replace(config, dataset_profile="unknown")

        try:
            trip_dataset_context_loader.load(invalid_config)
        except ValueError as error:
            assert_that(str(error)).contains("must be one of")
        else:
            raise AssertionError("Expected unknown dataset profile to fail")

    @allure.title("Incomplete profile asset configuration fails clearly")
    def test_dataset_profile_rejects_missing_profile_assets(self, config, trip_dataset_context_loader):
        local_test_dir = Path(__file__).resolve().parent / ".local_test_data"
        local_test_dir.mkdir(exist_ok=True)
        invalid_config = replace(config, scenario_dataset_path=local_test_dir / "missing_scenarios.csv")

        try:
            trip_dataset_context_loader.load(invalid_config)
        except FileNotFoundError as error:
            assert_that(str(error)).contains("scenario_dataset_path")
        else:
            raise AssertionError("Expected missing scenario dataset asset to fail")
