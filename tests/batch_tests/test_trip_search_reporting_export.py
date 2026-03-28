import json

import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.reporting.trip_search_reporting import (
    build_batch_reporting_bundle,
    build_suite_reporting_bundle,
    export_batch_reporting_bundle,
    export_suite_reporting_bundle,
)


@allure.parent_suite("Trip Search Validation")
@allure.suite("Batch Tests")
@allure.sub_suite("Report Export")
class TestTripSearchReportingExport:
    @allure.title("Batch reporting bundle exports structured files to disk")
    def test_batch_reporting_expects_bundle_to_export_to_disk(
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
            assert_that(export_dir, "Expected assertion for export_dir to hold").is_not_none()
            assert_that((export_dir / "scenario_summary.csv").exists(), "Expected assertion for (export_dir / 'scenario_summary.csv').exists() to hold").is_true()
            assert_that((export_dir / "run_summary.csv").exists(), "Expected assertion for (export_dir / 'run_summary.csv').exists() to hold").is_true()
            assert_that((export_dir / "issue_categories.csv").exists(), "Expected assertion for (export_dir / 'issue_categories.csv').exists() to hold").is_true()
            assert_that((export_dir / "pack_summary.csv").exists(), "Expected assertion for (export_dir / 'pack_summary.csv').exists() to hold").is_true()

    @allure.title("Suite reporting bundle exports structured files to disk")
    def test_suite_reporting_expects_bundle_to_export_to_disk(
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
            assert_that(export_dir, "Expected assertion for export_dir to hold").is_not_none()
            assert_that((export_dir / "suite_summary.csv").exists(), "Expected assertion for (export_dir / 'suite_summary.csv').exists() to hold").is_true()
            assert_that((export_dir / "suite_run_summary.csv").exists(), "Expected assertion for (export_dir / 'suite_run_summary.csv').exists() to hold").is_true()
            assert_that((export_dir / "issue_category_rollup.csv").exists(), "Expected assertion for (export_dir / 'issue_category_rollup.csv').exists() to hold").is_true()
            assert_that((export_dir / "suite_export_manifest.json").exists(), "Expected assertion for (export_dir / 'suite_export_manifest.json').exists() to hold").is_true()
            assert_that(policy_payload["continue_on_failure"], "Expected assertion for policy_payload['continue_on_failure'] to hold").is_true()
            assert_that(status_payload["dataset_profile"], "Expected assertion for status_payload['dataset_profile'] to hold").is_equal_to("small")
            assert_that(status_payload["scenario_dataset_asset"], "Expected assertion for status_payload['scenario_dataset_asset'] to hold").is_equal_to("batch_trip_search_scenarios.csv")
            assert_that(status_payload["suite_status"], "Expected assertion for status_payload['suite_status'] to hold").is_equal_to("passed")
            assert_that(manifest_payload["suite_id"], "Expected assertion for manifest_payload['suite_id'] to hold").is_equal_to(default_run_suite.suite_id)
            assert_that(manifest_payload["suite_label"], "Expected assertion for manifest_payload['suite_label'] to hold").is_equal_to(default_run_suite.suite_label)
            assert_that(manifest_payload["execution_id"], "Expected assertion for manifest_payload['execution_id'] to hold").is_equal_to("suite-export")
            assert_that(manifest_payload["dataset_profile"], "Expected assertion for manifest_payload['dataset_profile'] to hold").is_equal_to("small")
            assert_that(manifest_payload["scenario_dataset_asset"], "Expected assertion for manifest_payload['scenario_dataset_asset'] to hold").is_equal_to("batch_trip_search_scenarios.csv")
            assert_that(manifest_payload["suite_status"], "Expected assertion for manifest_payload['suite_status'] to hold").is_equal_to("passed")
            assert_that(manifest_payload["total_runs"], "Expected assertion for manifest_payload['total_runs'] to hold").is_equal_to(len(default_run_suite.run_profiles))
            assert_that(exported_relative_paths, "Expected assertion for exported_relative_paths to hold").contains(
                "suite_summary.csv",
                "suite_run_summary.csv",
                "issue_category_rollup.csv",
                "policy_summary.json",
                "status_summary.json",
            )
            assert_that([subset["run_id"] for subset in manifest_payload["selected_subsets"]], "Expected assertion for [subset['run_id'] for subset in manifest_payload['selected_subsets']] to hold").is_equal_to(
                list(reporting_bundle.suite_run_summary_frame["run_id"])
            )

