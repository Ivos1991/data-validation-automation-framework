from __future__ import annotations

from datetime import date, datetime


def parse_iso_date(raw_value: str | date) -> date:
    """Parse an ISO date string into a date object."""
    if isinstance(raw_value, date):
        return raw_value
    return datetime.strptime(raw_value.strip(), "%Y-%m-%d").date()


def to_iso_date(raw_value: str | date) -> str:
    """Normalize a date-like value into ISO date text."""
    return parse_iso_date(raw_value).isoformat()


def parse_gtfs_service_date(raw_value: str | int) -> date:
    """Parse a GTFS service date in YYYYMMDD format."""
    return datetime.strptime(str(raw_value).strip(), "%Y%m%d").date()


def gtfs_time_to_minutes(raw_value: str) -> int:
    """Convert a GTFS HH:MM:SS time value into rounded total minutes."""
    normalized_value = str(raw_value).strip()
    hours_text, minutes_text, seconds_text = normalized_value.split(":")
    total_minutes = (int(hours_text) * 60) + int(minutes_text)
    if int(seconds_text) >= 30:
        total_minutes += 1
    return total_minutes
