from __future__ import annotations

import json

from src.domain.trip_search.search_models import TripSearchExecutionJob
from src.framework.connectors.db.sqlite_client import SQLiteClient
from src.framework.logging.logger import get_logger
from src.framework.utils.date_utils import parse_iso_datetime, utc_now

LOGGER = get_logger("trip_search.execution_job_queries")


class ExecutionJobQueries:
    """Repository-style queries for async execution jobs stored in SQLite."""

    def __init__(self, sqlite_client: SQLiteClient) -> None:
        self.sqlite_client = sqlite_client

    def create_job(self, job_id: str, execution_mode: str, run_id: str | None = None) -> TripSearchExecutionJob:
        """Insert a new running execution job and return the stored model."""
        now_text = utc_now().isoformat()
        LOGGER.info("Creating execution job job_id=%s execution_mode=%s run_id=%s", job_id, execution_mode, run_id)
        self.sqlite_client.execute(
            """
            INSERT INTO execution_jobs (
                job_id,
                execution_mode,
                status,
                run_id,
                created_at,
                updated_at,
                completed_at,
                result_summary,
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (job_id, execution_mode, "running", run_id, now_text, now_text, None, None, None),
        )
        return self.get_job(job_id)

    def mark_completed(self, job_id: str, result_summary: dict[str, object]) -> TripSearchExecutionJob:
        """Mark an execution job as completed and persist its summary payload."""
        now_text = utc_now().isoformat()
        LOGGER.info("Marking execution job as completed job_id=%s", job_id)
        self.sqlite_client.execute(
            """
            UPDATE execution_jobs
            SET status = ?, updated_at = ?, completed_at = ?, result_summary = ?, error_message = NULL
            WHERE job_id = ?
            """,
            ("completed", now_text, now_text, json.dumps(result_summary, sort_keys=True, default=str), job_id),
        )
        return self.get_job(job_id)

    def mark_failed(self, job_id: str, error_message: str) -> TripSearchExecutionJob:
        """Mark an execution job as failed and persist the failure message."""
        now_text = utc_now().isoformat()
        LOGGER.info("Marking execution job as failed job_id=%s", job_id)
        self.sqlite_client.execute(
            """
            UPDATE execution_jobs
            SET status = ?, updated_at = ?, completed_at = ?, error_message = ?
            WHERE job_id = ?
            """,
            ("failed", now_text, now_text, error_message, job_id),
        )
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> TripSearchExecutionJob:
        """Load one execution job by its id."""
        raw_job = self.sqlite_client.fetch_one(
            """
            SELECT
                job_id,
                execution_mode,
                status,
                run_id,
                created_at,
                updated_at,
                completed_at,
                result_summary,
                error_message
            FROM execution_jobs
            WHERE job_id = ?
            """,
            (job_id,),
        )
        if raw_job is None:
            raise ValueError(f"Execution job '{job_id}' was not found")
        return self._map_job(raw_job)

    @staticmethod
    def _map_job(raw_job: dict[str, object]) -> TripSearchExecutionJob:
        return TripSearchExecutionJob(
            job_id=str(raw_job["job_id"]),
            execution_mode=str(raw_job["execution_mode"]),
            status=str(raw_job["status"]),
            run_id=None if raw_job["run_id"] is None else str(raw_job["run_id"]),
            created_at=parse_iso_datetime(str(raw_job["created_at"])),
            updated_at=parse_iso_datetime(str(raw_job["updated_at"])),
            completed_at=None if raw_job["completed_at"] is None else parse_iso_datetime(str(raw_job["completed_at"])),
            result_summary=None if raw_job["result_summary"] is None else json.loads(str(raw_job["result_summary"])),
            error_message=None if raw_job["error_message"] is None else str(raw_job["error_message"]),
        )
