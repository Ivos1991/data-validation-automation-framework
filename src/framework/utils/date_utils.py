from __future__ import annotations

from datetime import date, datetime, time, timezone


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


def utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_epoch_millis() -> int:
    """Return the current UTC epoch timestamp in milliseconds."""
    return int(utc_now().timestamp() * 1000)


def date_to_epoch_millis(value: date) -> int:
    """Convert a date into a UTC epoch timestamp at midnight in milliseconds."""
    return int(datetime.combine(value, time.min, timezone.utc).timestamp() * 1000)


def epoch_millis_to_utc_string(epoch_time_ms: int, date_time_format: str = "%Y-%m-%d %H:%M:%S.%f") -> str:
    """Convert an epoch timestamp in milliseconds to a readable UTC string."""
    epoch_time_sec = epoch_time_ms / 1000
    return datetime.fromtimestamp(epoch_time_sec, timezone.utc).strftime(date_time_format)
