from pathlib import Path

import allure
from assertpy import assert_that, soft_assertions

from src.framework.execution.trip_search_entrypoint import (
    TripSearchEntrypoint,
    main,
    parse_execution_args,
)


@allure.parent_suite("Trip Search Validation")
@allure.suite("Entrypoint Tests")
@allure.sub_suite("CLI Execution")
class TestTripSearchEntrypoint:
    """CLI and entrypoint execution tests for profile-aware validation runs."""

    @allure.title("CLI argument parsing supports dataset profile and execution mode selection")
    def test_cli_argument_parsing(self):
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
            assert_that(execution_args.dataset_profile).is_equal_to("large")
            assert_that(execution_args.execution_mode).is_equal_to("run-profile")
            assert_that(execution_args.run_profile_path).is_equal_to(Path("data/raw/large_smoke_trip_search_run_profile.json"))

    @allure.title("CLI defaults to suite execution for a selected dataset profile")
    def test_cli_executes_profile_default_suite(self, trip_search_entrypoint: TripSearchEntrypoint):
        """Verify a dataset-profile-only invocation resolves the default suite."""
        execution_result = trip_search_entrypoint.execute(parse_execution_args(["--dataset-profile", "large"]))

        with soft_assertions():
            assert_that(execution_result.execution_mode).is_equal_to("suite")
            assert_that(execution_result.dataset_profile).is_equal_to("large")
            assert_that(execution_result.scenario_dataset_path.name).is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(execution_result.run_suite_path.name).is_equal_to("large_trip_search_run_suite.json")
            assert_that(execution_result.summary["suite_status"]).is_equal_to("passed")
            assert_that(execution_result.summary["scenario_dataset_asset"]).is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(execution_result.export_dir).is_not_none()

    @allure.title("CLI supports explicit run-profile execution with profile-aware asset resolution")
    def test_cli_executes_explicit_run_profile(
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
            assert_that(execution_result.execution_mode).is_equal_to("run-profile")
            assert_that(execution_result.dataset_profile).is_equal_to("large")
            assert_that(execution_result.run_profile_path.name).is_equal_to("large_smoke_trip_search_run_profile.json")
            assert_that(execution_result.scenario_dataset_path.name).is_equal_to("large_batch_trip_search_scenarios.csv")
            assert_that(execution_result.summary["run_id"]).is_equal_to("large-smoke-run")
            assert_that(execution_result.summary["dataset_profile"]).is_equal_to("large")

    @allure.title("CLI supports explicit suite execution through the shared execution path")
    def test_cli_executes_explicit_suite(
        self,
        trip_search_entrypoint: TripSearchEntrypoint,
        entrypoint_config,
    ):
        """Verify explicit suite execution runs through the shared entrypoint path."""
        execution_result = trip_search_entrypoint.execute(
            parse_execution_args(
                [
                    "--execution-mode",
                    "suite",
                    "--run-suite-path",
                    str(entrypoint_config.run_suite_path),
                ]
            )
        )

        with soft_assertions():
            assert_that(execution_result.execution_mode).is_equal_to("suite")
            assert_that(execution_result.dataset_profile).is_equal_to("small")
            assert_that(execution_result.run_suite_path.name).is_equal_to("default_trip_search_run_suite.json")
            assert_that(execution_result.summary["suite_status"]).is_equal_to("passed")
            assert_that(execution_result.summary["dataset_profile"]).is_equal_to("small")

    @allure.title("CLI rejects invalid dataset profile values at parse time")
    def test_cli_rejects_invalid_dataset_profile(self):
        """Verify invalid dataset profiles fail during argument parsing."""
        try:
            parse_execution_args(["--dataset-profile", "unknown"])
        except SystemExit as error:
            assert_that(error.code).is_equal_to(2)
        else:
            raise AssertionError("Expected invalid dataset profile to fail argument parsing")

    @allure.title("CLI reports invalid explicit asset paths clearly")
    def test_cli_rejects_missing_explicit_run_profile(self, trip_search_entrypoint: TripSearchEntrypoint):
        """Verify missing explicit asset paths fail with a clear file error."""
        missing_path = Path("data/raw/does-not-exist-run-profile.json")

        try:
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
        except FileNotFoundError as error:
            assert_that(str(error)).contains("does-not-exist-run-profile.json")
        else:
            raise AssertionError("Expected missing run-profile path to fail")

    @allure.title("CLI main returns zero and emits structured JSON output")
    def test_cli_main_returns_zero(self, capsys):
        """Verify the CLI main function exits cleanly with JSON output."""
        exit_code = main(["--execution-mode", "batch"])
        captured = capsys.readouterr()

        with soft_assertions():
            assert_that(exit_code).is_equal_to(0)
            assert_that(captured.out).contains('"execution_mode": "batch"')
            assert_that(captured.out).contains('"dataset_profile": "small"')
