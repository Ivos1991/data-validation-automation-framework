from __future__ import annotations

import pandas as pd

from src.validators.quality.trip_search_scenario_preflight_validator import ScenarioPreflightResult


class FaultInjectingSearchServiceAPI:
    """Wrap the real API and force one scenario to fail deterministically."""

    def __init__(self, wrapped_service_api, failing_scenario_id: str) -> None:
        self.wrapped_service_api = wrapped_service_api
        self.failing_scenario_id = failing_scenario_id

    def search_by_route_and_departure_date(self, request_params):
        """Return an empty result for the injected failing scenario."""
        payload = self.wrapped_service_api.search_by_route_and_departure_date(request_params)
        if request_params.get("carrier") == "AmRail" and request_params.get("stops_count") == 0:
            return {"trips": []}
        return payload


class FaultInjectingSuiteSearchServiceAPI:
    """Wrap the real API and force one suite run to fail deterministically."""

    def __init__(self, wrapped_service_api) -> None:
        self.wrapped_service_api = wrapped_service_api

    def search_by_route_and_departure_date(self, request_params):
        """Return an empty result for the injected failing scenario."""
        payload = self.wrapped_service_api.search_by_route_and_departure_date(request_params)
        if request_params.get("carrier") == "AmRail" and request_params.get("stops_count") == 0:
            return {"trips": []}
        return payload


class PreflightBlockedBatchValidator:
    """Return a fixed preflight-blocked batch result for suite tests."""

    def __init__(self, blocked_result) -> None:
        self.blocked_result = blocked_result

    def validate(
        self,
        scenarios,
        expected_trip_frame,
        selection=None,
        run_profile=None,
        dataset_profile="small",
        scenario_dataset_asset="unknown",
    ):
        """Return the injected blocked result without executing validation."""
        return self.blocked_result


def attachable_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    """Build a lightweight dataframe for test-only reporting payloads."""
    return pd.DataFrame(rows)


def attachable_preflight_result() -> ScenarioPreflightResult:
    """Build a fixed preflight result used by blocked-suite tests."""
    return ScenarioPreflightResult(
        is_valid=False,
        summary_frame=attachable_frame([{"scenario_count": 1, "issue_count": 1, "is_valid": False}]),
        issues_frame=attachable_frame(
            [{"scenario_id": "blocked", "issue_code": "duplicate_scenario_id", "issue_message": "blocked"}]
        ),
    )

