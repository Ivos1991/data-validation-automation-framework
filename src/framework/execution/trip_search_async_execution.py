from __future__ import annotations

from threading import Thread
from time import sleep
from uuid import uuid4

from src.framework.connectors.db.execution_job_queries import ExecutionJobQueries
from src.framework.connectors.db.sqlite_client import SQLiteClient
from src.framework.connectors.db.trip_queries import TripQueries
from src.framework.logging.logger import get_logger
from src.validators.reconciliation.trip_batch_validator import TripSearchBatchValidator
from src.domain.trip_search.search_service_api import SearchServiceAPI

LOGGER = get_logger("trip_search.async_execution")


class TripSearchAsyncBatchExecutor:
    """Run batch validation in a background thread and persist job status in SQLite."""

    def __init__(self, numeric_tolerance: float) -> None:
        self.numeric_tolerance = numeric_tolerance

    def start_job(
        self,
        *,
        sqlite_db_path: str,
        scenarios,
        expected_trip_frame,
        dataset_profile: str = "small",
        scenario_dataset_asset: str = "unknown",
        run_profile=None,
        startup_delay_seconds: float = 0.0,
    ) -> str:
        """Create and start one background batch-validation job."""
        job_id = uuid4().hex
        seed_client = SQLiteClient(sqlite_db_path)
        try:
            seed_client.initialize_schema()
            ExecutionJobQueries(seed_client).create_job(
                job_id=job_id,
                execution_mode="batch",
                run_id=None if run_profile is None else run_profile.run_id,
            )
        finally:
            seed_client.close()

        worker = Thread(
            target=self._run_job,
            kwargs={
                "job_id": job_id,
                "sqlite_db_path": sqlite_db_path,
                "scenarios": scenarios,
                "expected_trip_frame": expected_trip_frame,
                "dataset_profile": dataset_profile,
                "scenario_dataset_asset": scenario_dataset_asset,
                "run_profile": run_profile,
                "startup_delay_seconds": startup_delay_seconds,
            },
            daemon=True,
        )
        worker.start()
        LOGGER.info("Started async batch execution job job_id=%s", job_id)
        return job_id

    def _run_job(
        self,
        *,
        job_id: str,
        sqlite_db_path: str,
        scenarios,
        expected_trip_frame,
        dataset_profile: str,
        scenario_dataset_asset: str,
        run_profile,
        startup_delay_seconds: float,
    ) -> None:
        if startup_delay_seconds > 0:
            sleep(startup_delay_seconds)

        client = SQLiteClient(sqlite_db_path)
        try:
            validator = TripSearchBatchValidator(
                service_api=SearchServiceAPI(TripQueries(client)),
                numeric_tolerance=self.numeric_tolerance,
            )
            batch_result = validator.validate(
                scenarios,
                expected_trip_frame,
                run_profile=run_profile,
                dataset_profile=dataset_profile,
                scenario_dataset_asset=scenario_dataset_asset,
            )
            ExecutionJobQueries(client).mark_completed(
                job_id,
                batch_result.run_summary_frame.iloc[0].to_dict(),
            )
        except Exception as error:
            LOGGER.exception("Async batch execution job failed job_id=%s", job_id)
            ExecutionJobQueries(client).mark_failed(job_id, str(error))
        finally:
            client.close()
