import allure
from tests.assertions import assert_that, soft_assertions

from src.domain.trip_search.search_models import TripSearchRunProfile, TripSearchScenarioSelection
from src.framework.reporting.trip_search_reporting import build_batch_reporting_bundle
from src.validators.reconciliation.trip_batch_validator import TripSearchBatchValidator


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Scenario Selection")
class TestTripSearchScenarioSelection:
    @allure.title("Scenario selector filters scenarios by pack, tag, and scenario type")
    def test_scenario_selector_expects_metadata_subsets_to_be_filtered(
        self,
        batch_scenario_dataset,
        scenario_selector,
        filter_pack_selection,
        combined_tag_selection,
        negative_type_selection,
    ):
        filter_pack_scenarios = scenario_selector.select(batch_scenario_dataset.scenarios, filter_pack_selection)
        combined_tag_scenarios = scenario_selector.select(batch_scenario_dataset.scenarios, combined_tag_selection)
        negative_type_scenarios = scenario_selector.select(batch_scenario_dataset.scenarios, negative_type_selection)

        with soft_assertions():
            assert_that([scenario.scenario_id for scenario in filter_pack_scenarios], "Expected assertion for [scenario.scenario_id for scenario in filter_pack_scenarios] to hold").is_equal_to(
                ["carrier-filter", "combined-filter"]
            )
            assert_that([scenario.scenario_id for scenario in combined_tag_scenarios], "Expected assertion for [scenario.scenario_id for scenario in combined_tag_scenarios] to hold").is_equal_to(
                ["combined-filter", "no-match"]
            )
            assert_that([scenario.scenario_id for scenario in negative_type_scenarios], "Expected assertion for [scenario.scenario_id for scenario in negative_type_scenarios] to hold").is_equal_to(["no-match"])

    @allure.title("Batch validation executes a selected pack subset and reports the selection")
    def test_batch_validation_expects_selected_pack_subset_to_execute(
        self,
        config,
        batch_trip_search_service_api,
        batch_scenario_dataset,
        expected_trip_frame,
        filter_pack_selection,
    ):
        validator = TripSearchBatchValidator(batch_trip_search_service_api, config.numeric_tolerance)
        batch_result = validator.validate(
            batch_scenario_dataset.scenarios,
            expected_trip_frame,
            selection=filter_pack_selection,
        )
        build_batch_reporting_bundle(batch_result).attach_to_allure("selected-pack")

        run_summary = batch_result.run_summary_frame.iloc[0]
        with soft_assertions():
            assert_that(batch_result.summary_frame["scenario_id"].tolist(), "Expected assertion for batch_result.summary_frame['scenario_id'].tolist() to hold").is_equal_to(
                ["carrier-filter", "combined-filter"]
            )
            assert_that(batch_result.summary_frame["pack"].tolist(), "Expected assertion for batch_result.summary_frame['pack'].tolist() to hold").is_equal_to(["filters", "filters"])
            assert_that(int(run_summary["total_scenarios"]), "Expected assertion for int(run_summary['total_scenarios']) to hold").is_equal_to(2)
            assert_that(run_summary["selected_pack"], "Expected assertion for run_summary['selected_pack'] to hold").is_equal_to("filters")
            assert_that(run_summary["selected_tag"], "Expected assertion for run_summary['selected_tag'] to hold").is_equal_to("all")
            assert_that(run_summary["selected_scenario_type"], "Expected assertion for run_summary['selected_scenario_type'] to hold").is_equal_to("all")
            assert_that(int(batch_result.pack_summary_frame.iloc[0]["total_scenarios"]), "Expected assertion for int(batch_result.pack_summary_frame.iloc[0]['total_scenarios']) to hold").is_equal_to(2)

    @allure.title("Batch validation executes a selected tag subset and preserves run summary totals")
    def test_batch_validation_expects_selected_tag_subset_to_execute(
        self,
        config,
        batch_trip_search_service_api,
        batch_scenario_dataset,
        expected_trip_frame,
        combined_tag_selection,
    ):
        validator = TripSearchBatchValidator(batch_trip_search_service_api, config.numeric_tolerance)
        batch_result = validator.validate(
            batch_scenario_dataset.scenarios,
            expected_trip_frame,
            selection=combined_tag_selection,
        )
        build_batch_reporting_bundle(batch_result).attach_to_allure("selected-tag")

        run_summary = batch_result.run_summary_frame.iloc[0]
        with soft_assertions():
            assert_that(batch_result.summary_frame["scenario_id"].tolist(), "Expected assertion for batch_result.summary_frame['scenario_id'].tolist() to hold").is_equal_to(
                ["combined-filter", "no-match"]
            )
            assert_that(int(run_summary["total_scenarios"]), "Expected assertion for int(run_summary['total_scenarios']) to hold").is_equal_to(2)
            assert_that(run_summary["selected_pack"], "Expected assertion for run_summary['selected_pack'] to hold").is_equal_to("all")
            assert_that(run_summary["selected_tag"], "Expected assertion for run_summary['selected_tag'] to hold").is_equal_to("combined")
            assert_that(run_summary["selected_scenario_type"], "Expected assertion for run_summary['selected_scenario_type'] to hold").is_equal_to("all")

    @allure.title("Batch validation handles empty scenario selections cleanly")
    def test_batch_validation_expects_empty_selection_to_be_handled(
        self,
        config,
        batch_trip_search_service_api,
        batch_scenario_dataset,
        expected_trip_frame,
    ):
        validator = TripSearchBatchValidator(batch_trip_search_service_api, config.numeric_tolerance)
        batch_result = validator.validate(
            batch_scenario_dataset.scenarios,
            expected_trip_frame,
            selection=TripSearchScenarioSelection(pack="missing-pack"),
        )
        build_batch_reporting_bundle(batch_result).attach_to_allure("empty-selection")

        run_summary = batch_result.run_summary_frame.iloc[0]
        with soft_assertions():
            assert_that(batch_result.summary_frame.empty, "Expected assertion for batch_result.summary_frame.empty to hold").is_true()
            assert_that(batch_result.pack_summary_frame.empty, "Expected assertion for batch_result.pack_summary_frame.empty to hold").is_true()
            assert_that(int(run_summary["total_scenarios"]), "Expected assertion for int(run_summary['total_scenarios']) to hold").is_equal_to(0)
            assert_that(int(run_summary["passed_scenarios"]), "Expected assertion for int(run_summary['passed_scenarios']) to hold").is_equal_to(0)
            assert_that(int(run_summary["failed_scenarios"]), "Expected assertion for int(run_summary['failed_scenarios']) to hold").is_equal_to(0)
            assert_that(run_summary["selected_pack"], "Expected assertion for run_summary['selected_pack'] to hold").is_equal_to("missing-pack")

    @allure.title("Batch validation executes subset selection from an external run profile")
    def test_batch_validation_expects_run_profile_subset_to_execute(
        self,
        config,
        batch_trip_search_service_api,
        batch_scenario_dataset,
        expected_trip_frame,
        default_run_profile,
    ):
        validator = TripSearchBatchValidator(batch_trip_search_service_api, config.numeric_tolerance)
        batch_result = validator.validate(
            batch_scenario_dataset.scenarios,
            expected_trip_frame,
            run_profile=default_run_profile,
        )
        build_batch_reporting_bundle(batch_result).attach_to_allure("run-profile")

        run_summary = batch_result.run_summary_frame.iloc[0]
        with soft_assertions():
            assert_that(batch_result.summary_frame["scenario_id"].tolist(), "Expected assertion for batch_result.summary_frame['scenario_id'].tolist() to hold").is_equal_to(
                ["carrier-filter", "combined-filter"]
            )
            assert_that(run_summary["run_id"], "Expected assertion for run_summary['run_id'] to hold").is_equal_to("filters-pack-run")
            assert_that(run_summary["run_label"], "Expected assertion for run_summary['run_label'] to hold").is_equal_to("Filters Pack")
            assert_that(run_summary["selected_pack"], "Expected assertion for run_summary['selected_pack'] to hold").is_equal_to("filters")
            assert_that(run_summary["selected_tag"], "Expected assertion for run_summary['selected_tag'] to hold").is_equal_to("all")
            assert_that(run_summary["selected_scenario_type"], "Expected assertion for run_summary['selected_scenario_type'] to hold").is_equal_to("all")

    @allure.title("Batch validation handles run profiles that select no scenarios")
    def test_batch_validation_expects_empty_run_profile_selection_to_be_handled(
        self,
        config,
        batch_trip_search_service_api,
        batch_scenario_dataset,
        expected_trip_frame,
    ):
        run_profile = TripSearchRunProfile(
            run_id="empty-selection-run",
            run_label="Empty Selection Run",
            selected_pack="missing-pack",
            description="Profile that intentionally selects no scenarios.",
        )
        validator = TripSearchBatchValidator(batch_trip_search_service_api, config.numeric_tolerance)
        batch_result = validator.validate(
            batch_scenario_dataset.scenarios,
            expected_trip_frame,
            run_profile=run_profile,
        )
        build_batch_reporting_bundle(batch_result).attach_to_allure("empty-run-profile")

        run_summary = batch_result.run_summary_frame.iloc[0]
        with soft_assertions():
            assert_that(batch_result.summary_frame.empty, "Expected assertion for batch_result.summary_frame.empty to hold").is_true()
            assert_that(int(run_summary["total_scenarios"]), "Expected assertion for int(run_summary['total_scenarios']) to hold").is_equal_to(0)
            assert_that(run_summary["run_id"], "Expected assertion for run_summary['run_id'] to hold").is_equal_to("empty-selection-run")
            assert_that(run_summary["run_label"], "Expected assertion for run_summary['run_label'] to hold").is_equal_to("Empty Selection Run")
            assert_that(run_summary["selected_pack"], "Expected assertion for run_summary['selected_pack'] to hold").is_equal_to("missing-pack")
