import allure
import pandas as pd
import pytest
from tests.assertions import assert_that, soft_assertions
from pathlib import Path
from uuid import uuid4

from src.domain.trip_search.search_service import search_by_route_and_departure_date
from src.domain.trip_search.search_service_request import SearchServiceRequest
from src.framework.reporting.allure_helpers import attach_dataframe
from src.framework.reporting.trip_search_reporting import build_suite_reporting_bundle
from src.framework.utils.dataframe_utils import (
    build_aggregate_summary,
    build_carrier_count_frame,
    filter_expected_trip_frame,
)
from src.transformers.gtfs_trip_transformer import GtfsTripTransformer
from src.validators.aggregate.trip_aggregate_validator import TripAggregateValidator
from src.validators.reconciliation.trip_reconciliation_validator import TripReconciliationValidator


@allure.parent_suite("Trip Search Validation")
@allure.suite("GTFS Tests")
@allure.sub_suite("GTFS Integration")
class TestGtfsIntegration:
    """GTFS profile tests for ingestion, transformation, and shared execution flows."""

    @allure.title("GTFS dataset loader reads the supported GTFS subset")
    def test_gtfs_loader_expects_supported_files_to_load(self, loaded_gtfs_dataset):
        """Verify the narrow GTFS loader reads the supported file subset."""
        with soft_assertions():
            assert_that(len(loaded_gtfs_dataset.trips_frame), "Expected assertion for len(loaded_gtfs_dataset.trips_frame) to hold").is_equal_to(4)
            assert_that(len(loaded_gtfs_dataset.calendar_frame), "Expected assertion for len(loaded_gtfs_dataset.calendar_frame) to hold").is_equal_to(3)
            assert_that(len(loaded_gtfs_dataset.stop_times_frame), "Expected assertion for len(loaded_gtfs_dataset.stop_times_frame) to hold").is_equal_to(9)
            assert_that(len(loaded_gtfs_dataset.calendar_dates_frame), "Expected assertion for len(loaded_gtfs_dataset.calendar_dates_frame) to hold").is_equal_to(1)
            assert_that(sorted(loaded_gtfs_dataset.routes_frame["route_id"].tolist()), "Expected assertion for sorted(loaded_gtfs_dataset.routes_frame['route_id'].tolist()) to hold").is_equal_to(["R100", "R200", "R300"])

    @allure.title("GTFS transformer derives canonical trip rows across multiple active service dates")
    def test_gtfs_transformer_expects_multidate_canonical_trip_frame_to_be_built(self, gtfs_transformed_trip_frame: pd.DataFrame):
        """Verify one GTFS trip expands into multiple canonical rows across active dates."""
        attach_dataframe("gtfs-canonical-trip-frame", gtfs_transformed_trip_frame)

        route_date_slice_20260410 = gtfs_transformed_trip_frame[
            (gtfs_transformed_trip_frame["origin"] == "NYC")
            & (gtfs_transformed_trip_frame["destination"] == "BOS")
            & (gtfs_transformed_trip_frame["departure_date"] == "2026-04-10")
        ]
        route_date_slice_20260413 = gtfs_transformed_trip_frame[
            (gtfs_transformed_trip_frame["origin"] == "NYC")
            & (gtfs_transformed_trip_frame["destination"] == "BOS")
            & (gtfs_transformed_trip_frame["departure_date"] == "2026-04-13")
        ]
        trip_001_dates = sorted(
            gtfs_transformed_trip_frame[gtfs_transformed_trip_frame["trip_id"].str.startswith("GTFS-TRIP-001-")]["departure_date"].tolist()
        )

        with soft_assertions():
            assert_that(len(gtfs_transformed_trip_frame), "Expected assertion for len(gtfs_transformed_trip_frame) to hold").is_equal_to(8)
            assert_that(gtfs_transformed_trip_frame.columns.tolist(), "Expected assertion for gtfs_transformed_trip_frame.columns.tolist() to hold").is_equal_to(
                ["trip_id", "origin", "destination", "departure_date", "stops_count", "route_id", "carrier", "price_amount", "currency", "duration_minutes"]
            )
            assert_that(len(route_date_slice_20260410), "Expected assertion for len(route_date_slice_20260410) to hold").is_equal_to(2)
            assert_that(len(route_date_slice_20260413), "Expected assertion for len(route_date_slice_20260413) to hold").is_equal_to(2)
            assert_that(sorted(route_date_slice_20260410["carrier"].tolist()), "Expected assertion for sorted(route_date_slice_20260410['carrier'].tolist()) to hold").is_equal_to(["AmRail", "BudgetBus"])
            assert_that(sorted(route_date_slice_20260413["carrier"].tolist()), "Expected assertion for sorted(route_date_slice_20260413['carrier'].tolist()) to hold").is_equal_to(["AmRail", "BudgetBus"])
            assert_that(sorted(route_date_slice_20260410["stops_count"].tolist()), "Expected assertion for sorted(route_date_slice_20260410['stops_count'].tolist()) to hold").is_equal_to([0, 1])
            assert_that(trip_001_dates, "Expected assertion for trip_001_dates to hold").is_equal_to(["2026-04-10", "2026-04-11", "2026-04-13"])

    @allure.title("GTFS dataset profile resolves matching GTFS assets")
    def test_gtfs_dataset_profile_expects_matching_assets_to_resolve(self, gtfs_loaded_trip_dataset):
        """Verify the GTFS dataset profile resolves the expected asset set."""
        with soft_assertions():
            assert_that(gtfs_loaded_trip_dataset.dataset_profile, "Expected assertion for gtfs_loaded_trip_dataset.dataset_profile to hold").is_equal_to("gtfs")
            assert_that(gtfs_loaded_trip_dataset.trip_dataset_source, "Expected assertion for gtfs_loaded_trip_dataset.trip_dataset_source to hold").is_equal_to("gtfs:data/raw/gtfs_sample")
            assert_that(gtfs_loaded_trip_dataset.scenario_dataset_path.name, "Expected assertion for gtfs_loaded_trip_dataset.scenario_dataset_path.name to hold").is_equal_to("gtfs_batch_trip_search_scenarios.csv")
            assert_that(gtfs_loaded_trip_dataset.default_run_profile_path.name, "Expected assertion for gtfs_loaded_trip_dataset.default_run_profile_path.name to hold").is_equal_to("gtfs_default_trip_search_run_profile.json")
            assert_that(gtfs_loaded_trip_dataset.default_run_suite_path.name, "Expected assertion for gtfs_loaded_trip_dataset.default_run_suite_path.name to hold").is_equal_to("gtfs_trip_search_run_suite.json")

    @pytest.mark.parametrize("departure_date", ["2026-04-10", "2026-04-13"])
    @allure.title("GTFS profile supports deterministic reconciliation across multiple service dates")
    def test_gtfs_profile_expects_search_reconciliation_flow_to_match_across_service_dates(
        self,
        departure_date: str,
        config,
        gtfs_loaded_trip_dataset,
        gtfs_trip_search_service_api,
    ):
        """Verify deterministic reconciliation works across more than one GTFS service date."""
        search_filters = {
            "origin": "NYC",
            "destination": "BOS",
            "departure_date": departure_date,
        }
        expected_subset = filter_expected_trip_frame(gtfs_loaded_trip_dataset.expected_trip_frame, **search_filters)
        actual_trips = search_by_route_and_departure_date(
            gtfs_trip_search_service_api,
            SearchServiceRequest.build(**search_filters),
        )
        actual_trip_frame = pd.DataFrame([trip.to_canonical_dict() for trip in actual_trips], columns=expected_subset.columns)
        reconciliation_result = TripReconciliationValidator().reconcile(expected_subset, actual_trip_frame)
        aggregate_result = TripAggregateValidator(config.numeric_tolerance).validate(
            expected_summary=build_aggregate_summary(expected_subset),
            actual_summary=build_aggregate_summary(actual_trip_frame),
            expected_carrier_counts=build_carrier_count_frame(expected_subset),
            actual_carrier_counts=build_carrier_count_frame(actual_trip_frame),
        )

        attach_dataframe(f"gtfs-expected-trips-{departure_date}", expected_subset)
        attach_dataframe(f"gtfs-actual-trips-{departure_date}", actual_trip_frame)

        with soft_assertions():
            assert_that(reconciliation_result.is_match, "Expected assertion for reconciliation_result.is_match to hold").is_true()
            assert_that(aggregate_result.is_match, "Expected assertion for aggregate_result.is_match to hold").is_true()
            assert_that(len(expected_subset), "Expected assertion for len(expected_subset) to hold").is_equal_to(2)

    @allure.title("GTFS profile supports aggregate validation on the expanded multi-date dataset")
    def test_gtfs_profile_expects_aggregate_validation_to_match_for_multidate_service(
        self,
        config,
        gtfs_loaded_trip_dataset,
        gtfs_trip_search_service_api,
    ):
        """Verify aggregate validation remains correct on the expanded GTFS date coverage."""
        search_filters = {
            "origin": "NYC",
            "destination": "BOS",
            "departure_date": "2026-04-13",
        }
        expected_subset = filter_expected_trip_frame(gtfs_loaded_trip_dataset.expected_trip_frame, **search_filters)
        actual_trips = search_by_route_and_departure_date(
            gtfs_trip_search_service_api,
            SearchServiceRequest.build(**search_filters),
        )
        actual_trip_frame = pd.DataFrame([trip.to_canonical_dict() for trip in actual_trips], columns=expected_subset.columns)
        aggregate_result = TripAggregateValidator(config.numeric_tolerance).validate(
            expected_summary=build_aggregate_summary(expected_subset),
            actual_summary=build_aggregate_summary(actual_trip_frame),
            expected_carrier_counts=build_carrier_count_frame(expected_subset),
            actual_carrier_counts=build_carrier_count_frame(actual_trip_frame),
        )

        with soft_assertions():
            assert_that(aggregate_result.is_match, "Expected assertion for aggregate_result.is_match to hold").is_true()
            assert_that(float(build_aggregate_summary(expected_subset).loc[0, "average_price"]), "Expected assertion for float(build_aggregate_summary(expected_subset).loc[0, 'average_price']) to hold").is_equal_to(97.75)

    @allure.title("GTFS profile supports suite execution through the shared suite executor")
    def test_gtfs_profile_expects_suite_execution_to_pass(
        self,
        config,
        gtfs_loaded_trip_dataset,
        gtfs_scenario_dataset,
        gtfs_run_profile_loader,
        gtfs_suite_search_service_api,
    ):
        """Verify GTFS runs still execute through the shared suite executor."""
        from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader
        from src.validators.reconciliation.trip_suite_executor import TripSearchRunSuiteExecutor
        suite = TripSearchRunSuiteLoader().load_json(gtfs_loaded_trip_dataset.default_run_suite_path)
        suite_result = TripSearchRunSuiteExecutor(
            service_api=gtfs_suite_search_service_api,
            numeric_tolerance=config.numeric_tolerance,
            run_profile_loader=gtfs_run_profile_loader,
        ).execute(
            suite,
            gtfs_scenario_dataset.scenarios,
            gtfs_loaded_trip_dataset.expected_trip_frame,
            dataset_profile="gtfs",
            scenario_dataset_asset=gtfs_loaded_trip_dataset.scenario_dataset_path.name,
        )
        build_suite_reporting_bundle(suite_result).attach_to_allure("gtfs-suite")

        with soft_assertions():
            assert_that(suite_result.suite_summary_frame.loc[0, "dataset_profile"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'dataset_profile'] to hold").is_equal_to("gtfs")
            assert_that(suite_result.suite_summary_frame.loc[0, "suite_status"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'suite_status'] to hold").is_equal_to("passed")
            assert_that(suite_result.suite_summary_frame.loc[0, "scenario_dataset_asset"], "Expected assertion for suite_result.suite_summary_frame.loc[0, 'scenario_dataset_asset'] to hold").is_equal_to("gtfs_batch_trip_search_scenarios.csv")
            assert_that(int(suite_result.suite_summary_frame.loc[0, "total_runs"]), "Expected assertion for int(suite_result.suite_summary_frame.loc[0, 'total_runs']) to hold").is_equal_to(2)

    @allure.title("GTFS loader rejects missing required GTFS files")
    def test_gtfs_loader_expects_missing_required_file_to_be_rejected(self, local_gtfs_test_dir: Path, gtfs_dataset_loader):
        """Verify missing supported GTFS files fail fast during loading."""
        missing_file_dir = local_gtfs_test_dir / f"gtfs_missing_file_{uuid4().hex}"
        missing_file_dir.mkdir(exist_ok=True)
        required_files = {
            "agency.txt": "agency_id,agency_name\nA1,AmRail\n",
            "routes.txt": "route_id,agency_id\nR100,A1\n",
            "trips.txt": "route_id,service_id,trip_id\nR100,SVC_A,T1\n",
            "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nSVC_A,0,0,0,0,1,0,0,20260410,20260410\n",
            "stops.txt": "stop_id,stop_code\nS1,NYC\nS2,BOS\n",
            "calendar_dates.txt": "service_id,date,exception_type\nSVC_A,20260410,1\n",
            "fare_attributes.txt": "fare_id,price,currency_type\nF1,120.0,USD\n",
            "fare_rules.txt": "fare_id,route_id\nF1,R100\n",
        }
        for file_name, content in required_files.items():
            (missing_file_dir / file_name).write_text(content, encoding="utf-8")

        with pytest.raises(FileNotFoundError) as error:
            gtfs_dataset_loader.load_directory(missing_file_dir)

        assert_that(str(error.value)).described_as("Missing required GTFS files should fail loading").contains(
            "stop_times.txt"
        )

    @allure.title("GTFS transformer rejects incomplete trip derivation input")
    def test_gtfs_transformer_expects_incomplete_trip_input_to_be_rejected(self, loaded_gtfs_dataset, gtfs_trip_transformer: GtfsTripTransformer):
        """Verify incomplete GTFS trip derivation fails before canonical output is produced."""
        broken_dataset = type(loaded_gtfs_dataset)(
            agency_frame=loaded_gtfs_dataset.agency_frame,
            routes_frame=loaded_gtfs_dataset.routes_frame,
            trips_frame=loaded_gtfs_dataset.trips_frame,
            calendar_frame=loaded_gtfs_dataset.calendar_frame,
            stop_times_frame=loaded_gtfs_dataset.stop_times_frame[loaded_gtfs_dataset.stop_times_frame["trip_id"] != "GTFS-TRIP-002"],
            stops_frame=loaded_gtfs_dataset.stops_frame,
            calendar_dates_frame=loaded_gtfs_dataset.calendar_dates_frame,
            fare_attributes_frame=loaded_gtfs_dataset.fare_attributes_frame,
            fare_rules_frame=loaded_gtfs_dataset.fare_rules_frame,
        )

        with pytest.raises(ValueError) as error:
            gtfs_trip_transformer.transform(broken_dataset)

        assert_that(str(error.value)).described_as("Incomplete GTFS trip derivation should fail").contains("origin")
