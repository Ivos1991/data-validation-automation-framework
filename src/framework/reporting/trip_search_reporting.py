from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.framework.reporting.allure_helpers import attach_dataframe, attach_json
from src.validators.reconciliation.trip_batch_validator import BatchValidationResult
from src.validators.reconciliation.trip_suite_executor import TripSearchRunSuiteResult


@dataclass(frozen=True)
class BatchReportingBundle:
    """Reusable reporting payload for a single batch or run-profile execution."""

    summary_frame: pd.DataFrame
    run_summary_frame: pd.DataFrame
    issue_category_frame: pd.DataFrame
    pack_summary_frame: pd.DataFrame

    def attach_to_allure(self, name_prefix: str) -> None:
        """Attach the batch reporting bundle to Allure."""
        attach_dataframe(f"{name_prefix}-scenario-summary", self.summary_frame)
        attach_dataframe(f"{name_prefix}-run-summary", self.run_summary_frame)
        attach_dataframe(f"{name_prefix}-issue-categories", self.issue_category_frame)
        attach_dataframe(f"{name_prefix}-pack-summary", self.pack_summary_frame)

    def export_to_directory(self, output_dir: Path) -> None:
        """Export the batch reporting bundle to disk."""
        export_dataframe(self.summary_frame, output_dir / "scenario_summary.csv")
        export_dataframe(self.run_summary_frame, output_dir / "run_summary.csv")
        export_dataframe(self.issue_category_frame, output_dir / "issue_categories.csv")
        export_dataframe(self.pack_summary_frame, output_dir / "pack_summary.csv")


@dataclass(frozen=True)
class SuiteReportingBundle:
    """Reusable reporting payload for one suite execution."""

    suite_summary_frame: pd.DataFrame
    suite_run_summary_frame: pd.DataFrame
    issue_category_rollup_frame: pd.DataFrame
    policy_summary: dict[str, object]
    status_summary: dict[str, object]

    def attach_to_allure(self, name_prefix: str) -> None:
        """Attach the suite reporting bundle to Allure."""
        attach_dataframe(f"{name_prefix}-suite-summary", self.suite_summary_frame)
        attach_dataframe(f"{name_prefix}-run-summary", self.suite_run_summary_frame)
        attach_dataframe(f"{name_prefix}-issue-rollup", self.issue_category_rollup_frame)
        attach_json(f"{name_prefix}-policy", self.policy_summary)
        attach_json(f"{name_prefix}-status", self.status_summary)

    def export_to_directory(self, output_dir: Path) -> None:
        """Export the suite reporting bundle to disk."""
        export_dataframe(self.suite_summary_frame, output_dir / "suite_summary.csv")
        export_dataframe(self.suite_run_summary_frame, output_dir / "suite_run_summary.csv")
        export_dataframe(self.issue_category_rollup_frame, output_dir / "issue_category_rollup.csv")
        export_json(self.policy_summary, output_dir / "policy_summary.json")
        export_json(self.status_summary, output_dir / "status_summary.json")


@dataclass(frozen=True)
class SuiteExportArtifact:
    """Manifest entry describing one exported suite artifact."""

    artifact_name: str
    file_name: str
    relative_path: str


@dataclass(frozen=True)
class SuiteExportManifest:
    """Machine-readable index for a suite export bundle."""

    schema_version: str
    generated_at: str
    execution_id: str
    suite_id: str
    suite_label: str
    dataset_profile: str
    scenario_dataset_asset: str
    suite_status: str
    run_ids: list[str]
    total_runs: int
    total_scenarios_executed: int
    total_passed_scenarios: int
    total_failed_scenarios: int
    selected_subsets: list[dict[str, object]]
    exported_artifacts: list[SuiteExportArtifact]

    def to_dict(self) -> dict[str, object]:
        """Return the manifest as a JSON-serializable dictionary."""
        payload = asdict(self)
        payload["exported_artifacts"] = [asdict(artifact) for artifact in self.exported_artifacts]
        return payload


def build_batch_reporting_bundle(batch_result: BatchValidationResult) -> BatchReportingBundle:
    """Build the standardized reporting bundle for batch validation output."""
    return BatchReportingBundle(
        summary_frame=batch_result.summary_frame,
        run_summary_frame=batch_result.run_summary_frame,
        issue_category_frame=batch_result.issue_category_frame,
        pack_summary_frame=batch_result.pack_summary_frame,
    )


def build_suite_reporting_bundle(suite_result: TripSearchRunSuiteResult) -> SuiteReportingBundle:
    """Build the standardized reporting bundle for suite execution output."""
    suite_summary_row = suite_result.suite_summary_frame.iloc[0].to_dict()
    policy_summary = {
        "stop_on_first_failed_run": suite_summary_row["stop_on_first_failed_run"],
        "continue_on_failure": suite_summary_row["continue_on_failure"],
        "minimum_passed_scenarios": suite_summary_row["minimum_passed_scenarios"],
        "minimum_pass_rate": suite_summary_row["minimum_pass_rate"],
        "maximum_failed_runs": suite_summary_row["maximum_failed_runs"],
    }
    status_summary = {
        "suite_id": suite_summary_row["suite_id"],
        "suite_label": suite_summary_row["suite_label"],
        "dataset_profile": suite_summary_row["dataset_profile"],
        "scenario_dataset_asset": suite_summary_row["scenario_dataset_asset"],
        "suite_status": suite_summary_row["suite_status"],
        "stopped_early": suite_summary_row["stopped_early"],
        "minimum_passed_scenarios_met": suite_summary_row["minimum_passed_scenarios_met"],
        "minimum_pass_rate_met": suite_summary_row["minimum_pass_rate_met"],
        "maximum_failed_runs_met": suite_summary_row["maximum_failed_runs_met"],
    }
    return SuiteReportingBundle(
        suite_summary_frame=suite_result.suite_summary_frame,
        suite_run_summary_frame=suite_result.suite_run_summary_frame,
        issue_category_rollup_frame=suite_result.issue_category_rollup_frame,
        policy_summary=policy_summary,
        status_summary=status_summary,
    )


def export_dataframe(dataframe: pd.DataFrame, output_path: Path) -> None:
    """Write a dataframe to CSV, creating parent directories as needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False)


def export_json(payload: dict[str, object], output_path: Path) -> None:
    """Write a JSON payload to disk, creating parent directories as needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def export_batch_reporting_bundle(
    batch_reporting_bundle: BatchReportingBundle,
    output_root: Path | None,
    export_name: str,
) -> Path | None:
    """Export a batch reporting bundle when an output root is configured."""
    if output_root is None:
        return None
    output_dir = output_root / export_name
    batch_reporting_bundle.export_to_directory(output_dir)
    return output_dir


def export_suite_reporting_bundle(
    suite_reporting_bundle: SuiteReportingBundle,
    output_root: Path | None,
    export_name: str,
) -> Path | None:
    """Export a suite reporting bundle and its manifest when configured."""
    if output_root is None:
        return None
    output_dir = output_root / export_name
    suite_reporting_bundle.export_to_directory(output_dir)
    manifest = build_suite_export_manifest(suite_reporting_bundle, output_dir, export_name)
    export_json(manifest.to_dict(), output_dir / "suite_export_manifest.json")
    return output_dir


def build_suite_export_manifest(
    suite_reporting_bundle: SuiteReportingBundle,
    output_dir: Path,
    execution_id: str | None = None,
) -> SuiteExportManifest:
    """Build the manifest that indexes the exported suite reporting bundle."""
    suite_summary_row = suite_reporting_bundle.suite_summary_frame.iloc[0].to_dict()
    selected_subsets = suite_reporting_bundle.suite_run_summary_frame[
        ["run_id", "run_label", "dataset_profile", "scenario_dataset_asset", "selected_pack", "selected_tag", "selected_scenario_type"]
    ].to_dict(orient="records")
    exported_artifacts = [
        SuiteExportArtifact(
            artifact_name="suite_summary",
            file_name="suite_summary.csv",
            relative_path=str(Path("suite_summary.csv")),
        ),
        SuiteExportArtifact(
            artifact_name="suite_run_summary",
            file_name="suite_run_summary.csv",
            relative_path=str(Path("suite_run_summary.csv")),
        ),
        SuiteExportArtifact(
            artifact_name="issue_category_rollup",
            file_name="issue_category_rollup.csv",
            relative_path=str(Path("issue_category_rollup.csv")),
        ),
        SuiteExportArtifact(
            artifact_name="policy_summary",
            file_name="policy_summary.json",
            relative_path=str(Path("policy_summary.json")),
        ),
        SuiteExportArtifact(
            artifact_name="status_summary",
            file_name="status_summary.json",
            relative_path=str(Path("status_summary.json")),
        ),
    ]
    return SuiteExportManifest(
        schema_version="1.0",
        generated_at=pd.Timestamp.now(tz="UTC").isoformat(),
        execution_id=execution_id or f"suite-export-{uuid4()}",
        suite_id=str(suite_summary_row["suite_id"]),
        suite_label=str(suite_summary_row["suite_label"]),
        dataset_profile=str(suite_summary_row["dataset_profile"]),
        scenario_dataset_asset=str(suite_summary_row["scenario_dataset_asset"]),
        suite_status=str(suite_summary_row["suite_status"]),
        run_ids=[str(run["run_id"]) for run in selected_subsets],
        total_runs=int(suite_summary_row["total_runs"]),
        total_scenarios_executed=int(suite_summary_row["total_scenarios_executed"]),
        total_passed_scenarios=int(suite_summary_row["total_passed_scenarios"]),
        total_failed_scenarios=int(suite_summary_row["total_failed_scenarios"]),
        selected_subsets=selected_subsets,
        exported_artifacts=exported_artifacts,
    )
