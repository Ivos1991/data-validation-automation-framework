from __future__ import annotations

from src.framework.connectors.db.execution_job_queries import ExecutionJobQueries
from src.framework.connectors.db.sqlite_client import SQLiteClient
from src.framework.utils.retry_utils import retry_for_relation_to_expected_value


def wait_for_job_status(sqlite_db_path: str, job_id: str, expected_status: str, max_attempts: int = 20, delay_seconds: float = 0.05):
    """Poll a SQLite-backed execution job until it reaches the expected status."""

    def load_status():
        client = SQLiteClient(sqlite_db_path)
        try:
            client.initialize_schema()
            return ExecutionJobQueries(client).get_job(job_id).status
        finally:
            client.close()

    return retry_for_relation_to_expected_value(
        func=load_status,
        expected_value=expected_status,
        delay_time_in_sec=delay_seconds,
        max_attempts=max_attempts,
        relate=lambda returned_status, target_status: returned_status != target_status,
    )
