from __future__ import annotations

import numpy as np


def normalize_int(raw_value: object) -> int:
    """Normalize an integer-like value into an int."""
    return int(raw_value)


def normalize_float(raw_value: object, precision: int = 2) -> float:
    """Normalize a numeric value into a rounded float."""
    return round(float(raw_value), precision)


def values_match(left: float, right: float, tolerance: float) -> bool:
    """Return whether two numeric values match within tolerance."""
    return bool(np.isclose(left, right, atol=tolerance))


def normalize_mean(raw_values: object, precision: int = 2) -> float:
    """Return a rounded mean for a numeric series."""
    return round(float(np.mean(raw_values)), precision)
