# Data Validation Automation Framework

Data-driven QA automation framework for validating trip-search results against normalized source datasets. The project demonstrates a senior-level validation platform that starts with deterministic CSV data, scales to larger synthetic packs, and extends into a narrow GTFS-derived input path without changing the canonical validation architecture.

## What This Project Covers

- Canonical trip validation through a normalized `Trip` model
- SQLite-backed execution path that mirrors service-style API modules
- Row-level reconciliation and aggregate validation
- External scenario datasets, run profiles, and multi-run suites
- Profile-aware execution for `small`, `large`, and `gtfs` datasets
- Allure reporting plus structured export bundles for CI and offline review
- CLI-driven execution for local runs and pipeline use

## Architecture Overview

The platform keeps one validation shape across all dataset profiles:

1. Source data is loaded from a managed CSV, deterministic synthetic builder, or supported GTFS subset.
2. Source records are normalized into the canonical trip model.
3. Canonical trips are seeded into SQLite.
4. The service flow executes through:
   `search_service_request -> search_service_api -> search_service`
5. Expected results are derived from normalized pandas frames.
6. Validation compares expected versus actual through:
   - row-level reconciliation
   - aggregate validation
   - optional filter-correctness checks
7. Batch, run-profile, and suite layers package those results for Allure and export.

Key implementation areas:

- [src/domain/trip_search](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/src/domain/trip_search)
- [src/framework/connectors](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/src/framework/connectors)
- [src/framework/reporting](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/src/framework/reporting)
- [src/validators](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/src/validators)

## Dataset Profiles

The framework supports three dataset profiles through one centralized asset-resolution mechanism.

### `small`

- Trip source: [sample_trips.csv](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/sample_trips.csv)
- Scenario dataset: [batch_trip_search_scenarios.csv](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/batch_trip_search_scenarios.csv)
- Purpose: fast local smoke and baseline validation

### `large`

- Trip source: deterministic synthetic dataset builder
- Scenario dataset: [large_batch_trip_search_scenarios.csv](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/large_batch_trip_search_scenarios.csv)
- Purpose: broader regression coverage without unstable runtime cost

### `gtfs`

- Trip source: narrow GTFS-style sample input under [gtfs_sample](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/gtfs_sample)
- Scenario dataset: [gtfs_batch_trip_search_scenarios.csv](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/gtfs_batch_trip_search_scenarios.csv)
- Purpose: realistic source transformation while preserving the same canonical validation flow

Supported GTFS subset in this portfolio slice:

- `agency.txt`
- `routes.txt`
- `trips.txt`
- `stop_times.txt`
- `stops.txt`
- `calendar.txt`
- `calendar_dates.txt`
- `fare_attributes.txt`
- `fare_rules.txt`

## Scenario, Run, and Suite Model

### Scenario datasets

Scenarios live in external CSV files and define deterministic search requests:

- `scenario_id`
- `origin`
- `destination`
- `departure_date`
- optional `carrier`
- optional `stops_count`
- metadata such as `pack`, `tag`, and `scenario_type`

### Run profiles

Run profiles are external JSON files that select scenario subsets and label one validation run.

Examples:

- [default_trip_search_run_profile.json](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/default_trip_search_run_profile.json)
- [large_filters_trip_search_run_profile.json](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/large_filters_trip_search_run_profile.json)
- [gtfs_multidate_trip_search_run_profile.json](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/gtfs_multidate_trip_search_run_profile.json)

### Run suites

Suites are external JSON definitions that execute ordered run profiles with suite-level policies and rollups.

Examples:

- [default_trip_search_run_suite.json](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/default_trip_search_run_suite.json)
- [large_trip_search_run_suite.json](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/large_trip_search_run_suite.json)
- [gtfs_trip_search_run_suite.json](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/data/raw/gtfs_trip_search_run_suite.json)

## CLI Usage

The framework exposes one profile-aware entrypoint:

- [trip_search_entrypoint.py](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/src/framework/execution/trip_search_entrypoint.py)

Example commands:

```powershell
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --dataset-profile small
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --dataset-profile large --execution-mode batch
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --execution-mode run-profile --run-profile-path data/raw/gtfs_multidate_trip_search_run_profile.json
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --execution-mode suite --run-suite-path data/raw/large_trip_search_run_suite.json
```

Default behavior:

- If only `--dataset-profile` is provided, execution defaults to `suite`.
- The selected profile resolves its default scenario dataset, run profile, and suite assets through the centralized profile loader.

## Allure Reporting

Allure is the primary reporting layer.

The framework attaches:

- scenario-level expected/actual/mismatch artifacts
- batch summaries and issue-category rollups
- suite summaries, policy results, and final status views

Run Allure-enabled pytest locally:

```powershell
.\.venv\Scripts\python -m pytest tests --alluredir=artifacts/allure-results
```

## Exported Artifacts

When `TRIP_REPORT_EXPORT_DIR` is set, the same reporting bundles used for Allure are exported to disk.

Batch exports include:

- `scenario_summary.csv`
- `run_summary.csv`
- `issue_categories.csv`
- `pack_summary.csv`

Suite exports include:

- `suite_summary.csv`
- `suite_run_summary.csv`
- `issue_category_rollup.csv`
- `policy_summary.json`
- `status_summary.json`
- `suite_export_manifest.json`

## CI/CD Overview

GitHub Actions is set up in the same reusable-workflow style used in the reference API automation repo:

- reusable test workflow
- PR validation workflow
- manual workflow dispatch
- nightly regression workflow

The workflows:

- install dependencies
- run pytest with Allure output enabled
- export report bundles to disk
- upload artifacts for CI review

See [run-validation.yml](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/.github/workflows/run-validation.yml), [pr-validation.yml](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/.github/workflows/pr-validation.yml), [manual-run.yml](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/.github/workflows/manual-run.yml), and [nightly-regression.yml](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/.github/workflows/nightly-regression.yml).

## Why This Is A Strong QA Automation Portfolio Project

This repository demonstrates more than basic API or UI test scripting:

- deterministic data-first validation design
- reusable service/API/request separation
- normalized expected-result derivation with pandas
- row-level and aggregate reconciliation
- profile-aware execution and asset resolution
- batch, run, and suite orchestration
- reporting and export packaging for CI consumers
- realistic source transformation through GTFS-derived inputs

It shows how to design a maintainable validation platform, not just isolated tests.

## Local Setup

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m pytest tests -q -rs -p no:cacheprovider
```

Environment defaults are documented in [.env.example](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/.env.example).

## Supporting Docs

- [implementation-plan.md](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/docs/implementation-plan.md)
- [target-architecture.md](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/docs/target-architecture.md)
- [validation-strategy.md](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/docs/validation-strategy.md)
- [reporting-and-ci.md](C:/Users/Seguras/Downloads/cosas/ivo_personal/data-validation-automation-framework/docs/reporting-and-ci.md)
