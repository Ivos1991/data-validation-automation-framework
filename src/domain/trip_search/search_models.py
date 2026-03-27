from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class Trip:
    """Canonical trip entity used across datasets, SQLite, and validation."""

    trip_id: str
    origin: str
    destination: str
    departure_date: date
    stops_count: int
    route_id: str
    carrier: str
    price_amount: float
    currency: str
    duration_minutes: int

    def to_canonical_dict(self) -> dict:
        """Return the trip as a normalized, serialization-friendly dictionary."""
        payload = asdict(self)
        payload["departure_date"] = self.departure_date.isoformat()
        return payload


@dataclass(frozen=True)
class TripSearchRequest:
    """Normalized request model for the trip-search service flow."""

    origin: str
    destination: str
    departure_date: date
    carrier: str | None = None
    stops_count: int | None = None


@dataclass(frozen=True)
class TripSearchScenario:
    """External validation scenario definition loaded from scenario datasets."""

    scenario_id: str
    origin: str
    destination: str
    departure_date: str
    carrier: str | None = None
    stops_count: int | None = None
    pack: str | None = None
    tag: str | None = None
    scenario_type: str | None = None

    def to_search_filters(self) -> dict[str, str | int]:
        """Convert a scenario into the filter set used by validation flows."""
        filters: dict[str, str | int] = {
            "origin": self.origin,
            "destination": self.destination,
            "departure_date": self.departure_date,
        }
        if self.carrier is not None:
            filters["carrier"] = self.carrier
        if self.stops_count is not None:
            filters["stops_count"] = self.stops_count
        return filters


@dataclass(frozen=True)
class TripSearchScenarioSelection:
    """Subset selector for scenario packs, tags, and scenario types."""

    pack: str | None = None
    tag: str | None = None
    scenario_type: str | None = None

    def to_summary_dict(self) -> dict[str, str]:
        """Return reporting-friendly selection labels."""
        return {
            "selected_pack": self.pack or "all",
            "selected_tag": self.tag or "all",
            "selected_scenario_type": self.scenario_type or "all",
        }


@dataclass(frozen=True)
class TripSearchRunProfile:
    """External run configuration that selects a subset of scenarios."""

    run_id: str
    run_label: str
    selected_pack: str | None = None
    selected_tag: str | None = None
    selected_scenario_type: str | None = None
    dataset_profile: str | None = None
    description: str | None = None

    def to_selection(self) -> TripSearchScenarioSelection:
        """Translate the run profile into the scenario selector model."""
        return TripSearchScenarioSelection(
            pack=self.selected_pack,
            tag=self.selected_tag,
            scenario_type=self.selected_scenario_type,
        )

    def to_summary_dict(self) -> dict[str, str]:
        """Return reporting-friendly metadata for the configured run."""
        return {
            "run_id": self.run_id,
            "run_label": self.run_label,
            "selected_pack": self.selected_pack or "all",
            "selected_tag": self.selected_tag or "all",
            "selected_scenario_type": self.selected_scenario_type or "all",
            "dataset_profile": self.dataset_profile or "config-default",
        }


@dataclass(frozen=True)
class TripSearchRunProfileReference:
    """Reference to an external run-profile asset inside a suite definition."""

    profile_path: Path


@dataclass(frozen=True)
class TripSearchRunSuitePolicy:
    """Failure-policy and threshold controls for suite execution."""

    stop_on_first_failed_run: bool = False
    continue_on_failure: bool = True
    minimum_passed_scenarios: int | None = None
    minimum_pass_rate: float | None = None
    maximum_failed_runs: int | None = None


@dataclass(frozen=True)
class TripSearchRunSuite:
    """Ordered collection of run-profile references executed as one suite."""

    suite_id: str
    suite_label: str
    run_profiles: list[TripSearchRunProfileReference]
    description: str | None = None
    dataset_profile: str | None = None
    policy: TripSearchRunSuitePolicy = TripSearchRunSuitePolicy()
