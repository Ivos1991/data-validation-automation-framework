from __future__ import annotations

from src.domain.trip_search.search_models import TripSearchScenario, TripSearchScenarioSelection


class TripSearchScenarioSelector:
    """Filter scenarios by pack, tag, and scenario type metadata."""

    def select(
        self,
        scenarios: list[TripSearchScenario],
        selection: TripSearchScenarioSelection | None = None,
    ) -> list[TripSearchScenario]:
        """Return the subset of scenarios matching the requested selection."""
        if selection is None:
            return scenarios

        selected_scenarios = scenarios
        if selection.pack is not None:
            selected_scenarios = [scenario for scenario in selected_scenarios if scenario.pack == selection.pack.strip().lower()]
        if selection.tag is not None:
            selected_scenarios = [scenario for scenario in selected_scenarios if scenario.tag == selection.tag.strip().lower()]
        if selection.scenario_type is not None:
            selected_scenarios = [
                scenario for scenario in selected_scenarios if scenario.scenario_type == selection.scenario_type.strip().lower()
            ]
        return selected_scenarios
