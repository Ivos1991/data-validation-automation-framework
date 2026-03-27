import json

import allure
from assertpy import assert_that, soft_assertions

from src.framework.reporting.trip_search_reporting import (
    build_batch_reporting_bundle,
    build_suite_export_manifest,
    build_suite_reporting_bundle,
    export_batch_reporting_bundle,
    export_suite_reporting_bundle,
)


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Report Export")
class TestTripSearchReportingExport:
    @allure.title("Batch reporting bundle exports structured files to disk")
    def test_batch_reporting_bundle_exports_to_disk(
        self,
        local_batch_test_dir,
        batch_validation_result,
    ):
        export_dir = export_batch_reporting_bundle(
            build_batch_reporting_bundle(batch_validation_result),
            output_root=local_batch_test_dir,
            export_name="batch-export",
        )

        with soft_assertions():
            assert_that(export_dir).is_not_none()
            assert_that((export_dir / "scenario_summary.csv").exists()).is_true()
            assert_that((export_dir / "run_summary.csv").exists()).is_true()
            assert_that((export_dir / "issue_categories.csv").exists()).is_true()
            assert_that((export_dir / "pack_summary.csv").exists()).is_true()

    @allure.title("Suite reporting bundle exports structured files to disk")
    def test_suite_reporting_bundle_exports_to_disk(
        self,
        local_batch_test_dir,
        default_run_suite,
        batch_scenarios,
        expected_trip_frame,
        run_suite_executor,
    ):
        suite_result = run_suite_executor.execute(default_run_suite, batch_scenarios, expected_trip_frame)
        reporting_bundle = build_suite_reporting_bundle(suite_result)
        export_dir = export_suite_reporting_bundle(
            reporting_bundle,
            output_root=local_batch_test_dir,
            export_name="suite-export",
        )

        policy_payload = json.loads((export_dir / "policy_summary.json").read_text(encoding="utf-8"))
        status_payload = json.loads((export_dir / "status_summary.json").read_text(encoding="utf-8"))
        manifest_payload = json.loads((export_dir / "suite_export_manifest.json").read_text(encoding="utf-8"))
        exported_relative_paths = {artifact["relative_path"] for artifact in manifest_payload["exported_artifacts"]}

        with soft_assertions():
            assert_that(export_dir).is_not_none()
            assert_that((export_dir / "suite_summary.csv").exists()).is_true()
            assert_that((export_dir / "suite_run_summary.csv").exists()).is_true()
            assert_that((export_dir / "issue_category_rollup.csv").exists()).is_true()
            assert_that((export_dir / "suite_export_manifest.json").exists()).is_true()
            assert_that(policy_payload["continue_on_failure"]).is_true()
            assert_that(status_payload["dataset_profile"]).is_equal_to("small")
            assert_that(status_payload["scenario_dataset_asset"]).is_equal_to("batch_trip_search_scenarios.csv")
            assert_that(status_payload["suite_status"]).is_equal_to("passed")
            assert_that(manifest_payload["suite_id"]).is_equal_to(default_run_suite.suite_id)
            assert_that(manifest_payload["suite_label"]).is_equal_to(default_run_suite.suite_label)
            assert_that(manifest_payload["execution_id"]).is_equal_to("suite-export")
            assert_that(manifest_payload["dataset_profile"]).is_equal_to("small")
            assert_that(manifest_payload["scenario_dataset_asset"]).is_equal_to("batch_trip_search_scenarios.csv")
            assert_that(manifest_payload["suite_status"]).is_equal_to("passed")
            assert_that(manifest_payload["total_runs"]).is_equal_to(len(default_run_suite.run_profiles))
            assert_that(exported_relative_paths).contains(
                "suite_summary.csv",
                "suite_run_summary.csv",
                "issue_category_rollup.csv",
                "policy_summary.json",
                "status_summary.json",
            )
            assert_that([subset["run_id"] for subset in manifest_payload["selected_subsets"]]).is_equal_to(
                list(reporting_bundle.suite_run_summary_frame["run_id"])
            )

    @allure.title("Report export helpers are safe when no output root is configured")
    def test_reporting_export_helpers_return_none_when_output_root_is_not_configured(
        self,
        batch_validation_result,
        default_run_suite,
        batch_scenarios,
        expected_trip_frame,
        run_suite_executor,
    ):
        suite_result = run_suite_executor.execute(default_run_suite, batch_scenarios, expected_trip_frame)

        batch_export = export_batch_reporting_bundle(
            build_batch_reporting_bundle(batch_validation_result),
            output_root=None,
            export_name="unused-batch-export",
        )
        suite_export = export_suite_reporting_bundle(
            build_suite_reporting_bundle(suite_result),
            output_root=None,
            export_name="unused-suite-export",
        )

        with soft_assertions():
            assert_that(batch_export).is_none()
            assert_that(suite_export).is_none()

    @allure.title("Suite export manifest builder is stable for direct bundle inspection")
    def test_build_suite_export_manifest_returns_expected_metadata(
        self,
        default_run_suite,
        batch_scenarios,
        expected_trip_frame,
        run_suite_executor,
        local_batch_test_dir,
    ):
        suite_result = run_suite_executor.execute(default_run_suite, batch_scenarios, expected_trip_frame)
        reporting_bundle = build_suite_reporting_bundle(suite_result)

        manifest = build_suite_export_manifest(
            reporting_bundle,
            output_dir=local_batch_test_dir / "direct-manifest-suite-export",
            execution_id="manual-suite-export",
        )

        with soft_assertions():
            assert_that(manifest.schema_version).is_equal_to("1.0")
            assert_that(manifest.execution_id).is_equal_to("manual-suite-export")
            assert_that(manifest.suite_id).is_equal_to(default_run_suite.suite_id)
            assert_that(manifest.total_runs).is_equal_to(len(default_run_suite.run_profiles))
            assert_that(manifest.exported_artifacts).is_length(5)
