import allure
from tests.assertions import assert_that, soft_assertions

from src.framework.connectors.db.execution_job_queries import ExecutionJobQueries
from tests.async_tests.support import wait_for_job_status


@allure.parent_suite("Trip Search Validation")
@allure.suite("Async Tests")
@allure.sub_suite("Async Batch Execution")
class TestTripSearchAsyncExecution:
    """Async batch-execution tests for SQLite-backed job status tracking."""

    @allure.title("Async batch execution creates a running job before completion")
    def test_async_batch_execution_expects_running_job_status_before_completion(
        self,
        async_batch_executor,
        seeded_trip_db,
        async_job_queries,
        async_batch_scenarios,
        expected_trip_frame,
    ):
        """Verify async execution writes an initial running job row immediately."""
        job_id = async_batch_executor.start_job(
            sqlite_db_path=seeded_trip_db.db_path,
            scenarios=async_batch_scenarios,
            expected_trip_frame=expected_trip_frame,
            dataset_profile="small",
            scenario_dataset_asset="batch_trip_search_scenarios.csv",
            startup_delay_seconds=0.2,
        )

        job = async_job_queries.get_job(job_id)

        with soft_assertions():
            assert_that(job.execution_mode, "Async execution job should use batch mode").is_equal_to("batch")
            assert_that(job.status, "Async execution job should start in running status").is_equal_to("running")
            assert_that(job.result_summary, "Running async jobs should not have a result summary yet").is_none()

        final_status = wait_for_job_status(seeded_trip_db.db_path, job_id, "completed")
        assert_that(
            final_status,
            "Running-status async test should wait for the background job to finish before teardown",
        ).is_equal_to("completed")

    @allure.title("Async batch execution reaches completed status with batch summary output")
    def test_async_batch_execution_expects_completed_status_and_summary(
        self,
        async_batch_executor,
        seeded_trip_db,
        async_batch_scenarios,
        expected_trip_frame,
    ):
        """Verify async execution eventually completes and stores the batch summary payload."""
        job_id = async_batch_executor.start_job(
            sqlite_db_path=seeded_trip_db.db_path,
            scenarios=async_batch_scenarios,
            expected_trip_frame=expected_trip_frame,
            dataset_profile="small",
            scenario_dataset_asset="batch_trip_search_scenarios.csv",
        )

        final_status = wait_for_job_status(seeded_trip_db.db_path, job_id, "completed")
        job = ExecutionJobQueries(seeded_trip_db).get_job(job_id)

        with soft_assertions():
            assert_that(final_status, "Async execution job should reach completed status").is_equal_to("completed")
            assert_that(job.status, "Stored async job status should be completed").is_equal_to("completed")
            assert_that(job.result_summary, "Completed async jobs should store a summary payload").is_not_none()
            assert_that(job.result_summary["passed_scenarios"], "Completed async jobs should report passed scenarios").is_equal_to(4)
            assert_that(job.result_summary["failed_scenarios"], "Completed async jobs should report zero failed scenarios").is_equal_to(0)

    @allure.title("Async batch execution records failed status when background validation raises")
    def test_async_batch_execution_expects_failed_status_for_invalid_background_input(
        self,
        async_batch_executor,
        seeded_trip_db,
        async_batch_scenarios,
        expected_trip_frame,
    ):
        """Verify background failures are captured as failed execution jobs."""
        invalid_expected_trip_frame = expected_trip_frame[["trip_id"]].copy()
        job_id = async_batch_executor.start_job(
            sqlite_db_path=seeded_trip_db.db_path,
            scenarios=async_batch_scenarios,
            expected_trip_frame=invalid_expected_trip_frame,
            dataset_profile="small",
            scenario_dataset_asset="batch_trip_search_scenarios.csv",
        )

        final_status = wait_for_job_status(seeded_trip_db.db_path, job_id, "failed")
        job = ExecutionJobQueries(seeded_trip_db).get_job(job_id)

        with soft_assertions():
            assert_that(final_status, "Invalid async background input should produce failed status").is_equal_to("failed")
            assert_that(job.status, "Stored async job status should be failed").is_equal_to("failed")
            assert_that(job.error_message, "Failed async jobs should persist the failure message").is_not_none()
