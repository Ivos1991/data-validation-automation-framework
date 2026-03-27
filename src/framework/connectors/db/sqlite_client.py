from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class SQLiteClient:
    """Minimal SQLite wrapper used by the deterministic validation flow."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row

    def initialize_schema(self) -> None:
        """Create the canonical trips schema when it is not present."""
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
            """
        )

    def execute(self, query: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        """Execute a single SQL statement and commit immediately."""
        cursor = self.connection.cursor()
        cursor.execute(query, parameters)
        self.connection.commit()
        return cursor

    def fetch_all(self, query: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Return all rows from a query as dictionaries."""
        cursor = self.execute(query, parameters)
        rows = cursor.fetchall()
        cursor.close()
        return [dict(row) for row in rows]

    def execute_many(self, query: str, parameters: list[tuple[Any, ...]]) -> None:
        """Execute a parameterized statement for multiple rows."""
        cursor = self.connection.cursor()
        cursor.executemany(query, parameters)
        self.connection.commit()
        cursor.close()

    def execute_script(self, script: str) -> None:
        """Execute a multi-statement SQL script."""
        cursor = self.connection.cursor()
        cursor.executescript(script)
        self.connection.commit()
        cursor.close()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self.connection.close()
