import allure
import pytest
from tests.assertions import assert_that
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
    def test_scenario_loader_expects_external_dataset_to_load(self, config, scenario_loader: TripSearchScenarioLoader):
        loaded_dataset = scenario_loader.load_csv(config.scenario_dataset_path)
        attach_dataframe("loaded-scenario-table", loaded_dataset.scenario_frame)
        attach_dataframe("scenario-preflight-summary", loaded_dataset.preflight_result.summary_frame)
        attach_dataframe("scenario-preflight-issues", loaded_dataset.preflight_result.issues_frame)

        assert_that(loaded_dataset.scenarios, "Expected assertion for loaded_dataset.scenarios to hold").is_length(4)
        assert_that(bool(loaded_dataset.preflight_result.is_valid), "Expected assertion for bool(loaded_dataset.preflight_result.is_valid) to hold").is_true()
        assert_that(loaded_dataset.preflight_result.issues_frame.empty, "Expected assertion for loaded_dataset.preflight_result.issues_frame.empty to hold").is_true()
        assert_that([scenario.scenario_id for scenario in loaded_dataset.scenarios], "Expected assertion for [scenario.scenario_id for scenario in loaded_dataset.scenarios] to hold").is_equal_to(
            ["route-date-only", "carrier-filter", "combined-filter", "no-match"]
        )
        assert_that(loaded_dataset.scenarios[2].carrier, "Expected assertion for loaded_dataset.scenarios[2].carrier to hold").is_equal_to("AmRail")
        assert_that(loaded_dataset.scenarios[2].stops_count, "Expected assertion for loaded_dataset.scenarios[2].stops_count to hold").is_equal_to(0)
        assert_that(loaded_dataset.scenarios[0].pack, "Expected assertion for loaded_dataset.scenarios[0].pack to hold").is_equal_to("smoke")
        assert_that(loaded_dataset.scenarios[2].tag, "Expected assertion for loaded_dataset.scenarios[2].tag to hold").is_equal_to("combined")
        assert_that(loaded_dataset.scenarios[3].scenario_type, "Expected assertion for loaded_dataset.scenarios[3].scenario_type to hold").is_equal_to("negative")

    @allure.title("Scenario loader rejects malformed schema")
    def test_scenario_loader_expects_missing_required_columns_to_be_rejected(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"missing_required_columns_{uuid4().hex}.csv"
        invalid_dataset_path.write_text("scenario_id,origin,destination\nmissing-date,NYC,BOS\n", encoding="utf-8")

        with pytest.raises(ValueError) as error:
            scenario_loader.load_csv(invalid_dataset_path)

        assert_that(str(error.value)).described_as("Missing required columns should fail scenario loading").contains(
            "Missing required scenario columns"
        )

    @allure.title("Scenario loader rejects malformed stops_count values")
    def test_scenario_loader_expects_invalid_stops_count_to_be_rejected(
        self,
        local_batch_test_dir: Path,
        scenario_loader: TripSearchScenarioLoader,
    ):
        invalid_dataset_path = local_batch_test_dir / f"invalid_stops_count_{uuid4().hex}.csv"
        invalid_dataset_path.write_text(
            "scenario_id,origin,destination,departure_date,carrier,stops_count\nbad-stops,NYC,BOS,2026-04-01,AmRail,abc\n",
            encoding="utf-8",
        )

        with pytest.raises(ValueError) as error:
            scenario_loader.load_csv(invalid_dataset_path)

        assert_that(str(error.value)).described_as("Invalid stops_count values should fail scenario loading").contains(
            "Scenario field 'stops_count' is invalid"
        )

    @allure.title("Scenario loader rejects duplicate logical scenarios during preflight")
    def test_scenario_loader_expects_duplicate_logical_scenarios_to_be_rejected(
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

        with pytest.raises(ScenarioPreflightValidationError) as error:
            scenario_loader.load_csv(invalid_dataset_path)

        attach_dataframe("duplicate-logical-preflight-summary", error.value.preflight_result.summary_frame)
        attach_dataframe("duplicate-logical-preflight-issues", error.value.preflight_result.issues_frame)
        assert_that(str(error.value)).described_as("Duplicate logical scenarios should fail preflight").contains(
            "failed preflight"
        )
        assert_that(error.value.preflight_result.issues_frame["issue_code"].tolist()).described_as(
            "Duplicate logical scenario issue codes should be reported"
        ).contains("duplicate_logical_scenario")

