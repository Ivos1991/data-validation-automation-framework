import allure
import pytest
from assertpy import assert_that
from pathlib import Path
from uuid import uuid4

from src.framework.connectors.files.scenario_loader import TripSearchScenarioLoader
from src.framework.reporting.allure_helpers import attach_dataframe
from src.validators.quality.trip_search_scenario_preflight_validator import ScenarioPreflightValidationError


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Scenario Loading")
class TestTripSearchScenarioLoader:
    @allure.title("External batch scenario dataset loads into typed scenarios")
    def test_scenario_loader_reads_external_dataset(self, config, scenario_loader: TripSearchScenarioLoader):
        loaded_dataset = scenario_loader.load_csv(config.scenario_dataset_path)
        attach_dataframe("loaded-scenario-table", loaded_dataset.scenario_frame)
        attach_dataframe("scenario-preflight-summary", loaded_dataset.preflight_result.summary_frame)
        attach_dataframe("scenario-preflight-issues", loaded_dataset.preflight_result.issues_frame)

        assert_that(loaded_dataset.scenarios).is_length(4)
        assert_that(bool(loaded_dataset.preflight_result.is_valid)).is_true()
        assert_that(loaded_dataset.preflight_result.issues_frame.empty).is_true()
        assert_that([scenario.scenario_id for scenario in loaded_dataset.scenarios]).is_equal_to(
            ["route-date-only", "carrier-filter", "combined-filter", "no-match"]
        )
        assert_that(loaded_dataset.scenarios[2].carrier).is_equal_to("AmRail")
        assert_that(loaded_dataset.scenarios[2].stops_count).is_equal_to(0)
        assert_that(loaded_dataset.scenarios[0].pack).is_equal_to("smoke")
        assert_that(loaded_dataset.scenarios[2].tag).is_equal_to("combined")
        assert_that(loaded_dataset.scenarios[3].scenario_type).is_equal_to("negative")

    @allure.title("Scenario loader rejects malformed schema")
    def test_scenario_loader_rejects_missing_required_columns(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"missing_required_columns_{uuid4().hex}.csv"
        invalid_dataset_path.write_text("scenario_id,origin,destination\nmissing-date,NYC,BOS\n", encoding="utf-8")

        try:
            scenario_loader.load_csv(invalid_dataset_path)
        except ValueError as error:
            assert_that(str(error)).contains("Missing required scenario columns")
        else:
            raise AssertionError("Expected missing-column scenario dataset to fail validation")
        finally:
            if invalid_dataset_path.exists():
                invalid_dataset_path.unlink()

    @allure.title("Scenario loader rejects malformed stops_count values")
    def test_scenario_loader_rejects_invalid_stops_count(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"invalid_stops_count_{uuid4().hex}.csv"
        invalid_dataset_path.write_text(
            "scenario_id,origin,destination,departure_date,carrier,stops_count\nbad-stops,NYC,BOS,2026-04-01,AmRail,abc\n",
            encoding="utf-8",
        )

        try:
            scenario_loader.load_csv(invalid_dataset_path)
        except ValueError as error:
            assert_that(str(error)).contains("Scenario field 'stops_count' is invalid")
        else:
            raise AssertionError("Expected invalid stops_count scenario dataset to fail validation")
        finally:
            if invalid_dataset_path.exists():
                invalid_dataset_path.unlink()

    @allure.title("Scenario loader rejects duplicate logical scenarios during preflight")
    def test_scenario_loader_rejects_duplicate_logical_scenarios(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"duplicate_logical_scenarios_{uuid4().hex}.csv"
        invalid_dataset_path.write_text(
            (
                "scenario_id,origin,destination,departure_date,carrier,stops_count\n"
                "base-case,NYC,BOS,2026-04-01,AmRail,0\n"
                "duplicate-case,NYC,BOS,2026-04-01,AmRail,0\n"
            ),
            encoding="utf-8",
        )

        try:
            scenario_loader.load_csv(invalid_dataset_path)
        except ScenarioPreflightValidationError as error:
            attach_dataframe("duplicate-logical-preflight-summary", error.preflight_result.summary_frame)
            attach_dataframe("duplicate-logical-preflight-issues", error.preflight_result.issues_frame)
            assert_that(str(error)).contains("failed preflight")
            assert_that(error.preflight_result.issues_frame["issue_code"].tolist()).contains("duplicate_logical_scenario")
        else:
            raise AssertionError("Expected duplicate logical scenario dataset to fail preflight")
        finally:
            if invalid_dataset_path.exists():
                invalid_dataset_path.unlink()

    @allure.title("Scenario loader rejects duplicate scenario ids during preflight")
    def test_scenario_loader_rejects_duplicate_scenario_ids(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"duplicate_scenario_ids_{uuid4().hex}.csv"
        invalid_dataset_path.write_text(
            (
                "scenario_id,origin,destination,departure_date,carrier,stops_count\n"
                "duplicate-id,NYC,BOS,2026-04-01,AmRail,0\n"
                "duplicate-id,NYC,BOS,2026-04-02,,\n"
            ),
            encoding="utf-8",
        )

        try:
            scenario_loader.load_csv(invalid_dataset_path)
        except ScenarioPreflightValidationError as error:
            attach_dataframe("duplicate-id-preflight-summary", error.preflight_result.summary_frame)
            attach_dataframe("duplicate-id-preflight-issues", error.preflight_result.issues_frame)
            assert_that(error.preflight_result.issues_frame["issue_code"].tolist()).contains("duplicate_scenario_id")
        else:
            raise AssertionError("Expected duplicate scenario id dataset to fail preflight")
        finally:
            if invalid_dataset_path.exists():
                invalid_dataset_path.unlink()

    @allure.title("Scenario loader rejects contradictory scenarios during preflight")
    def test_scenario_loader_rejects_contradictory_scenarios(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"contradictory_scenarios_{uuid4().hex}.csv"
        invalid_dataset_path.write_text(
            (
                "scenario_id,origin,destination,departure_date,carrier,stops_count\n"
                "same-city,NYC,NYC,2026-04-01,AmRail,0\n"
            ),
            encoding="utf-8",
        )

        try:
            scenario_loader.load_csv(invalid_dataset_path)
        except ScenarioPreflightValidationError as error:
            attach_dataframe("contradictory-preflight-summary", error.preflight_result.summary_frame)
            attach_dataframe("contradictory-preflight-issues", error.preflight_result.issues_frame)
            assert_that(error.preflight_result.issues_frame["issue_code"].tolist()).contains("contradictory_route")
        else:
            raise AssertionError("Expected contradictory scenario dataset to fail preflight")
        finally:
            if invalid_dataset_path.exists():
                invalid_dataset_path.unlink()

    @allure.title("Scenario loader rejects non-normalized optional filter values during preflight")
    def test_scenario_loader_rejects_non_normalized_optional_filters(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"non_normalized_optional_filters_{uuid4().hex}.csv"
        invalid_dataset_path.write_text(
            (
                "scenario_id,origin,destination,departure_date,carrier,stops_count\n"
                "carrier-spacing,NYC,BOS,2026-04-01, AmRail ,0\n"
            ),
            encoding="utf-8",
        )

        try:
            scenario_loader.load_csv(invalid_dataset_path)
        except ScenarioPreflightValidationError as error:
            attach_dataframe("non-normalized-preflight-summary", error.preflight_result.summary_frame)
            attach_dataframe("non-normalized-preflight-issues", error.preflight_result.issues_frame)
            assert_that(error.preflight_result.issues_frame["issue_code"].tolist()).contains("non_normalized_carrier")
        else:
            raise AssertionError("Expected non-normalized optional filter dataset to fail preflight")
        finally:
            if invalid_dataset_path.exists():
                invalid_dataset_path.unlink()
