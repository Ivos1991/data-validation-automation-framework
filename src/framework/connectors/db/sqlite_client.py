from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from src.framework.logging.logger import get_logger

LOGGER = get_logger("trip_search.sqlite_client")


class SQLiteClient:
    """Minimal SQLite wrapper used by the deterministic validation flow."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        LOGGER.info("Opening SQLite connection at %s", self.db_path)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row

    def initialize_schema(self) -> None:
        """Create the canonical trips schema when it is not present."""
        LOGGER.info("Initializing canonical trips schema")
        self.execute_script(
            """
            CREATE TABLE IF NOT EXISTS trips (
                trip_id TEXT PRIMARY KEY,
                origin TEXT NOT NULL,
                destination TEXT NOT NULL,
                departure_date TEXT NOT NULL,
                stops_count INTEGER NOT NULL,
                route_id TEXT NOT NULL,
                carrier TEXT NOT NULL,
                price_amount REAL NOT NULL,
                currency TEXT NOT NULL,
                duration_minutes INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS execution_jobs (
                job_id TEXT PRIMARY KEY,
                execution_mode TEXT NOT NULL,
                status TEXT NOT NULL,
                run_id TEXT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT NULL,
                result_summary TEXT NULL,
                error_message TEXT NULL
            );
            """
        )

    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        """Execute a single SQL statement and commit immediately."""
        LOGGER.info("Executing SQL query=%s parameters=%s", self._query_preview(query), parameters)
        cursor = self.connection.cursor()
        cursor.execute(query, parameters)
        self.connection.commit()
        LOGGER.info("Executed SQL query successfully")
        return cursor

    def fetch_all(self, query: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Return all rows from a query as dictionaries."""
        LOGGER.info("Fetching rows from SQLite query=%s", self._query_preview(query))
        cursor = self.execute(query, parameters)
        rows = cursor.fetchall()
        cursor.close()
        LOGGER.info("Fetched %s rows from SQLite", len(rows))
        return [dict(row) for row in rows]

    def fetch_one(self, query: str, parameters: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        """Return the first row from a query as a dictionary when one exists."""
        LOGGER.info("Fetching single row from SQLite query=%s", self._query_preview(query))
        cursor = self.execute(query, parameters)
        row = cursor.fetchone()
        cursor.close()
        if row is None:
            LOGGER.info("No SQLite row matched the query")
            return None
        LOGGER.info("Fetched one SQLite row successfully")
        return dict(row)

    def fetch_first_value(self, query: str, parameters: tuple[Any, ...] = ()) -> Any | None:
        """Return the first value from the first matching row when one exists."""
        row = self.fetch_one(query, parameters)
        if row is None:
            return None
        return next(iter(row.values()))

    def fetch_first_column(self, query: str, parameters: tuple[Any, ...] = ()) -> list[Any]:
        """Return the first column from all matching rows as a flat list."""
        rows = self.fetch_all(query, parameters)
        if not rows:
            return []
        first_column = next(iter(rows[0]))
        return [row[first_column] for row in rows]

    def exists(self, query: str, parameters: tuple[Any, ...] = ()) -> bool:
        """Return whether the query yields at least one row."""
        return self.fetch_one(query, parameters) is not None

    def count(self, query: str, parameters: tuple[Any, ...] = ()) -> int:
        """Return a scalar count result from a COUNT(*) query."""
        count_value = self.fetch_first_value(query, parameters)
        if count_value is None:
            return 0
        return int(count_value)

    def execute_many(self, query: str, parameters: list[tuple[Any, ...]]) -> None:
        """Execute a parameterized statement for multiple rows."""
        LOGGER.info(
            "Executing bulk SQL query=%s row_count=%s",
            self._query_preview(query),
            len(parameters),
        )
        cursor = self.connection.cursor()
        cursor.executemany(query, parameters)
        self.connection.commit()
        cursor.close()
        LOGGER.info("Executed bulk SQL query successfully")

    def execute_script(self, script: str) -> None:
        """Execute a multi-statement SQL script."""
        LOGGER.info("Executing SQL script")
        cursor = self.connection.cursor()
        cursor.executescript(script)
        self.connection.commit()
        cursor.close()
        LOGGER.info("Executed SQL script successfully")

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        LOGGER.info("Closing SQLite connection at %s", self.db_path)
        self.connection.close()

    @staticmethod
    def _query_preview(query: str) -> str:
        """Return a compact one-line preview of a SQL statement for logs."""
        compact_query = " ".join(query.split())
        if len(compact_query) > 240:
            return f"{compact_query[:237]}..."
        return compact_query
