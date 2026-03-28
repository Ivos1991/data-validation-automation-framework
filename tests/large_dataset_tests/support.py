from __future__ import annotations


def scenario_by_id(scenarios, scenario_id: str):
    """Return one scenario by id for deterministic larger-data tests."""
    return next(scenario for scenario in scenarios if scenario.scenario_id == scenario_id)

