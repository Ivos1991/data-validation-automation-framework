from pathlib import Path

import allure
import pytest
from tests.assertions import assert_that, soft_assertions

from src.framework.execution.trip_search_entrypoint import (
    TripSearchEntrypoint,
    parse_execution_args,
)


@allure.parent_suite("Trip Search Validation")
@allure.suite("Entrypoint Tests")
@allure.sub_suite("CLI Execution")
class TestTripSearchEntrypoint:
    """CLI and entrypoint execution tests for profile-aware validation runs."""

    @allure.title("CLI argument parsing supports dataset profile and execution mode selection")
    def test_entrypoint_expects_cli_arguments_to_parse_correctly(self):
        """Verify CLI arguments are parsed into the typed execution model."""
        execution_args = parse_execution_args(
            [
                "--dataset-profile",
                "large",
                "--execution-mode",
                "run-profile",
                "--run-profile-path",
                "data/raw/large_smoke_trip_search_run_profile.json",
            ]
        )

        with soft_assertions():
            assert_that(execution_args.dataset_profile, "Expected assertion for execution_args.dataset_profile to hold").is_equal_to("large")
            assert_that(execution_args.execution_mode, "Expected assertion for execution_args.execution_mode to hold").is_equal_to("run-profile")
            assert_that(execution_args.run_profile_path, "Expected assertion for execution_args.run_profile_path to hold").is_equal_to(Path("data/raw/large_smoke_trip_search_run_profile.json"))

    @allure.title("CLI defaults to suite execution for a selected dataset profile")
    def test_entrypoint_expects_profile_default_suite_to_execute(self, trip_search_entrypoint: TripSearchEntrypoint):
        """Verify a dataset-profile-only invocation resolves the default suite."""
        execution_result = trip_search_entrypoint.execute(parse_execution_args(["--dataset-profile", "large"]))

        with soft_assertions():
            assert_that(execution_result.execution_mode, "Expected assertion for execution_result.execution_mode to hold").is_equal_to("suite")
            assert_that(execution_result.dataset_profile, "Expected assertion for execution_result.dataset_profile to hold").is_equal_to("large")
            assert_that(execution_result.scenario_dataset_path.name, "Expected assertion for execution_result.scenario_dataset_path.name to hold").is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(execution_result.run_suite_path.name, "Expected assertion for execution_result.run_suite_path.name to hold").is_equal_to("large_trip_search_run_suite.json")
            assert_that(execution_result.summary["suite_status"], "Expected assertion for execution_result.summary['suite_status'] to hold").is_equal_to("passed")
            assert_that(execution_result.summary["scenario_dataset_asset"], "Expected assertion for execution_result.summary['scenario_dataset_asset'] to hold").is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(execution_result.export_dir, "Expected assertion for execution_result.export_dir to hold").is_not_none()

    @allure.title("CLI supports explicit run-profile execution with profile-aware asset resolution")
    def test_entrypoint_expects_explicit_run_profile_to_execute(
        self,
        trip_search_entrypoint: TripSearchEntrypoint,
        entrypoint_config,
    ):
        """Verify explicit run-profile execution reuses profile-aware asset resolution."""
        execution_result = trip_search_entrypoint.execute(
            parse_execution_args(
                [
                    "--execution-mode",
                    "run-profile",
                    "--run-profile-path",
                    str(entrypoint_config.run_profile_path.parent / "large_smoke_trip_search_run_profile.json"),
                ]
            )
        )

        with soft_assertions():
            assert_that(execution_result.execution_mode, "Expected assertion for execution_result.execution_mode to hold").is_equal_to("run-profile")
            assert_that(execution_result.dataset_profile, "Expected assertion for execution_result.dataset_profile to hold").is_equal_to("large")
            assert_that(execution_result.run_profile_path.name, "Expected assertion for execution_result.run_profile_path.name to hold").is_equal_to("large_smoke_trip_search_run_profile.json")
            assert_that(execution_result.scenario_dataset_path.name, "Expected assertion for execution_result.scenario_dataset_path.name to hold").is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(execution_result.summary["run_id"], "Expected assertion for execution_result.summary['run_id'] to hold").is_equal_to("large-smoke-run")
            assert_that(execution_result.summary["dataset_profile"], "Expected assertion for execution_result.summary['dataset_profile'] to hold").is_equal_to("large")

    @allure.title("CLI rejects invalid dataset profile values at parse time")
    def test_entrypoint_expects_invalid_dataset_profile_to_be_rejected(self):
        """Verify invalid dataset profiles fail during argument parsing."""
        with pytest.raises(SystemExit) as error:
            parse_execution_args(["--dataset-profile", "unknown"])

        assert_that(error.value.code).described_as("Invalid dataset profiles should fail argument parsing").is_equal_to(2)

    @allure.title("CLI reports invalid explicit asset paths clearly")
    def test_entrypoint_expects_missing_explicit_run_profile_to_be_rejected(self, trip_search_entrypoint: TripSearchEntrypoint):
        """Verify missing explicit asset paths fail with a clear file error."""
        missing_path = Path("data/raw/does-not-exist-run-profile.json")

        with pytest.raises(FileNotFoundError) as error:
            trip_search_entrypoint.execute(
                parse_execution_args(
                    [
                        "--execution-mode",
                        "run-profile",
                        "--run-profile-path",
                        str(missing_path),
                    ]
                )
            )

        assert_that(str(error.value)).described_as("Missing run-profile paths should fail clearly").contains(
            "does-not-exist-run-profile.json"
        )

