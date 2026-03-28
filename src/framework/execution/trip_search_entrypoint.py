from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

from src.domain.trip_search.search_service_api import SearchServiceAPI
from src.domain.trip_search.dataset_profiles import SUPPORTED_DATASET_PROFILES
from src.framework.config.config_manager import ConfigManager
from src.framework.connectors.db.sqlite_client import SQLiteClient
from src.framework.connectors.db.trip_queries import TripQueries
from src.framework.connectors.files.dataset_loader import DatasetLoader
from src.framework.connectors.files.run_profile_loader import TripSearchRunProfileLoader
from src.framework.connectors.files.run_suite_loader import TripSearchRunSuiteLoader
from src.framework.connectors.files.scenario_loader import TripSearchScenarioLoader
from src.framework.connectors.files.synthetic_trip_dataset_builder import SyntheticTripDatasetBuilder
from src.framework.connectors.files.trip_dataset_context_loader import (
    LoadedTripDatasetContext,
    TripDatasetContextLoader,
)
from src.framework.logging.logger import get_logger
from src.framework.reporting.trip_search_reporting import (
    build_batch_reporting_bundle,
    build_suite_reporting_bundle,
    export_batch_reporting_bundle,
    export_suite_reporting_bundle,
)
from src.validators.reconciliation.trip_batch_validator import BatchValidationResult, TripSearchBatchValidator
from src.validators.reconciliation.trip_suite_executor import TripSearchRunSuiteExecutor, TripSearchRunSuiteResult

LOGGER = get_logger("trip_search.entrypoint")


EXECUTION_MODES = ("batch", "run-profile", "suite")


@dataclass(frozen=True)
class TripSearchExecutionArgs:
    """Normalized CLI arguments for one validation invocation."""

    dataset_profile: str | None
    execution_mode: str
    scenario_dataset_path: Path | None
    run_profile_path: Path | None
    run_suite_path: Path | None


@dataclass(frozen=True)
class TripSearchExecutionResult:
    """Serializable summary of one CLI execution."""

    execution_mode: str
    dataset_profile: str
    trip_dataset_source: str
    scenario_dataset_path: Path
    run_profile_path: Path | None
    run_suite_path: Path | None
    export_dir: Path | None
    summary: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-friendly execution summary."""
        return {
            "execution_mode": self.execution_mode,
            "dataset_profile": self.dataset_profile,
            "trip_dataset_source": self.trip_dataset_source,
            "scenario_dataset_path": str(self.scenario_dataset_path),
            "run_profile_path": None if self.run_profile_path is None else str(self.run_profile_path),
            "run_suite_path": None if self.run_suite_path is None else str(self.run_suite_path),
            "export_dir": None if self.export_dir is None else str(self.export_dir),
            "summary": self.summary,
        }


class TripSearchEntrypoint:
    """Profile-aware execution entrypoint for batch, run-profile, and suite modes."""

    def __init__(
        self,
        config: ConfigManager | None = None,
        dataset_context_loader: TripDatasetContextLoader | None = None,
        scenario_loader: TripSearchScenarioLoader | None = None,
        run_profile_loader: TripSearchRunProfileLoader | None = None,
        run_suite_loader: TripSearchRunSuiteLoader | None = None,
    ) -> None:
        self.config = config or ConfigManager.from_env()
        self.dataset_context_loader = dataset_context_loader or TripDatasetContextLoader(
            dataset_loader=DatasetLoader(),
            synthetic_trip_dataset_builder=SyntheticTripDatasetBuilder(),
        )
        self.scenario_loader = scenario_loader or TripSearchScenarioLoader(DatasetLoader())
        self.run_profile_loader = run_profile_loader or TripSearchRunProfileLoader()
        self.run_suite_loader = run_suite_loader or TripSearchRunSuiteLoader()

    def execute(self, execution_args: TripSearchExecutionArgs) -> TripSearchExecutionResult:
        """Dispatch the requested execution mode and return a structured summary."""
        LOGGER.info(
            "Executing trip-search entrypoint mode=%s dataset_profile=%s",
            execution_args.execution_mode,
            execution_args.dataset_profile,
        )
        if execution_args.execution_mode == "batch":
            return self._execute_batch(execution_args)
        if execution_args.execution_mode == "run-profile":
            return self._execute_run_profile(execution_args)
        if execution_args.execution_mode == "suite":
            return self._execute_suite(execution_args)
        raise ValueError(f"Unsupported execution mode: {execution_args.execution_mode}")

    def _execute_batch(self, execution_args: TripSearchExecutionArgs) -> TripSearchExecutionResult:
        dataset_context = self.dataset_context_loader.load(self.config, dataset_profile=execution_args.dataset_profile)
        scenario_dataset_path = execution_args.scenario_dataset_path or dataset_context.scenario_dataset_path
        scenario_dataset = self.scenario_loader.load_csv(scenario_dataset_path)
        batch_result = self._run_batch_validation(dataset_context, scenario_dataset.scenarios)
        export_dir = export_batch_reporting_bundle(
            build_batch_reporting_bundle(batch_result),
            self.config.report_export_dir,
            export_name=f"{dataset_context.dataset_profile}-batch-run",
        )
        return TripSearchExecutionResult(
            execution_mode="batch",
            dataset_profile=dataset_context.dataset_profile,
            trip_dataset_source=dataset_context.trip_dataset_source,
            scenario_dataset_path=scenario_dataset_path,
            run_profile_path=None,
            run_suite_path=None,
            export_dir=export_dir,
            summary=batch_result.run_summary_frame.iloc[0].to_dict(),
        )

    def _execute_run_profile(self, execution_args: TripSearchExecutionArgs) -> TripSearchExecutionResult:
        preliminary_context = self.dataset_context_loader.load(self.config, dataset_profile=execution_args.dataset_profile)
        run_profile_path = execution_args.run_profile_path or preliminary_context.default_run_profile_path
        run_profile = self.run_profile_loader.load_json(run_profile_path)
        dataset_context = self.dataset_context_loader.load(
            self.config,
            dataset_profile=execution_args.dataset_profile,
            run_profile=run_profile,
        )
        scenario_dataset_path = execution_args.scenario_dataset_path or dataset_context.scenario_dataset_path
        scenario_dataset = self.scenario_loader.load_csv(scenario_dataset_path)
        batch_result = self._run_batch_validation(dataset_context, scenario_dataset.scenarios, run_profile=run_profile)
        export_dir = export_batch_reporting_bundle(
            build_batch_reporting_bundle(batch_result),
            self.config.report_export_dir,
            export_name=run_profile.run_id,
        )
        return TripSearchExecutionResult(
            execution_mode="run-profile",
            dataset_profile=dataset_context.dataset_profile,
            trip_dataset_source=dataset_context.trip_dataset_source,
            scenario_dataset_path=scenario_dataset_path,
            run_profile_path=run_profile_path,
            run_suite_path=None,
            export_dir=export_dir,
            summary=batch_result.run_summary_frame.iloc[0].to_dict(),
        )

    def _execute_suite(self, execution_args: TripSearchExecutionArgs) -> TripSearchExecutionResult:
        preliminary_context = self.dataset_context_loader.load(self.config, dataset_profile=execution_args.dataset_profile)
        run_suite_path = execution_args.run_suite_path or preliminary_context.default_run_suite_path
        run_suite = self.run_suite_loader.load_json(run_suite_path)
        dataset_context = self.dataset_context_loader.load(
            self.config,
            dataset_profile=execution_args.dataset_profile,
            run_suite=run_suite,
        )
        scenario_dataset_path = execution_args.scenario_dataset_path or dataset_context.scenario_dataset_path
        scenario_dataset = self.scenario_loader.load_csv(scenario_dataset_path)
        suite_result = self._run_suite_validation(dataset_context, scenario_dataset.scenarios, run_suite, scenario_dataset_path)
        export_dir = export_suite_reporting_bundle(
            build_suite_reporting_bundle(suite_result),
            self.config.report_export_dir,
            export_name=run_suite.suite_id,
        )
        return TripSearchExecutionResult(
            execution_mode="suite",
            dataset_profile=dataset_context.dataset_profile,
            trip_dataset_source=dataset_context.trip_dataset_source,
            scenario_dataset_path=scenario_dataset_path,
            run_profile_path=None,
            run_suite_path=run_suite_path,
            export_dir=export_dir,
            summary=suite_result.suite_summary_frame.iloc[0].to_dict(),
        )

    def _run_batch_validation(
        self,
        dataset_context: LoadedTripDatasetContext,
        scenarios,
        run_profile=None,
    ) -> BatchValidationResult:
        with self._build_service_api(dataset_context) as service_api:
            validator = TripSearchBatchValidator(service_api, self.config.numeric_tolerance)
            return validator.validate(
                scenarios,
                dataset_context.expected_trip_frame,
                run_profile=run_profile,
                dataset_profile=dataset_context.dataset_profile,
                scenario_dataset_asset=dataset_context.scenario_dataset_path.name,
            )

    def _run_suite_validation(
        self,
        dataset_context: LoadedTripDatasetContext,
        scenarios,
        run_suite,
        scenario_dataset_path: Path,
    ) -> TripSearchRunSuiteResult:
        with self._build_service_api(dataset_context) as service_api:
            executor = TripSearchRunSuiteExecutor(
                service_api=service_api,
                numeric_tolerance=self.config.numeric_tolerance,
                run_profile_loader=self.run_profile_loader,
            )
            return executor.execute(
                run_suite,
                scenarios,
                dataset_context.expected_trip_frame,
                dataset_profile=dataset_context.dataset_profile,
                scenario_dataset_asset=scenario_dataset_path.name,
            )

    def _build_service_api(self, dataset_context: LoadedTripDatasetContext):
        return SeededSearchServiceApiContext(dataset_context, self.config.sqlite_db_path)


class SeededSearchServiceApiContext:
    """Context manager that seeds SQLite and exposes the API-style search adapter."""

    def __init__(self, dataset_context: LoadedTripDatasetContext, sqlite_db_path: str) -> None:
        self.dataset_context = dataset_context
        self.sqlite_db_path = sqlite_db_path
        self.sqlite_client: SQLiteClient | None = None
        self.resolved_db_path: str | Path | None = None

    def __enter__(self) -> SearchServiceAPI:
        """Seed the resolved dataset into SQLite and return the service API adapter."""
        self.resolved_db_path = self._resolve_db_path()
        LOGGER.info(
            "Building seeded service API for dataset_profile=%s db_path=%s",
            self.dataset_context.dataset_profile,
            self.resolved_db_path,
        )
        self.sqlite_client = SQLiteClient(self.resolved_db_path)
        self.sqlite_client.initialize_schema()
        TripQueries(self.sqlite_client).seed_trips(self.dataset_context.normalized_trips)
        return SearchServiceAPI(TripQueries(self.sqlite_client))

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        """Close SQLite resources and remove temporary database files."""
        if self.sqlite_client is not None:
            db_path = Path(self.sqlite_client.db_path)
            self.sqlite_client.close()
            if db_path.exists():
                LOGGER.info("Removing temporary SQLite database at %s", db_path)
                db_path.unlink()

    def _resolve_db_path(self) -> str | Path:
        if self.sqlite_db_path != ":memory:":
            return self.sqlite_db_path
        # CLI execution uses a temporary file-backed database so the seeded API can be reopened safely.
        temporary_file = NamedTemporaryFile(prefix="trip_search_cli_", suffix=".sqlite", delete=False)
        temporary_file.close()
        return Path(temporary_file.name)


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for profile-aware execution."""
    parser = argparse.ArgumentParser(description="Execute profile-aware trip-search validation.")
    parser.add_argument(
        "--dataset-profile",
        dest="dataset_profile",
        choices=SUPPORTED_DATASET_PROFILES,
        default=None,
        help="Dataset profile to resolve before execution.",
    )
    parser.add_argument(
        "--execution-mode",
        dest="execution_mode",
        choices=EXECUTION_MODES,
        default="suite",
        help="Validation execution mode. Defaults to 'suite'.",
    )
    parser.add_argument(
        "--scenario-dataset-path",
        dest="scenario_dataset_path",
        type=Path,
        default=None,
        help="Optional override for the external scenario dataset path.",
    )
    parser.add_argument(
        "--run-profile-path",
        dest="run_profile_path",
        type=Path,
        default=None,
        help="Optional override for the run-profile JSON path.",
    )
    parser.add_argument(
        "--run-suite-path",
        dest="run_suite_path",
        type=Path,
        default=None,
        help="Optional override for the run-suite JSON path.",
    )
    return parser


def parse_execution_args(argv: list[str] | None = None) -> TripSearchExecutionArgs:
    """Parse raw CLI arguments into the typed execution model."""
    parsed_args = build_argument_parser().parse_args(argv)
    return TripSearchExecutionArgs(
        dataset_profile=parsed_args.dataset_profile,
        execution_mode=parsed_args.execution_mode,
        scenario_dataset_path=parsed_args.scenario_dataset_path,
        run_profile_path=parsed_args.run_profile_path,
        run_suite_path=parsed_args.run_suite_path,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the CLI entrypoint and print a JSON execution summary."""
    execution_args = parse_execution_args(argv)
    execution_result = TripSearchEntrypoint().execute(execution_args)
    print(json.dumps(execution_result.to_dict(), indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
