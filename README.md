# Data Validation Automation Framework

Data-driven QA automation framework for validating trip-search results against normalized source datasets. The project demonstrates a validation platform that starts with deterministic CSV data, scales to larger synthetic packs, and extends into a narrow GTFS-derived input path without changing the canonical validation architecture.

## Overview

This repository validates trip-search behavior through one consistent platform model:

1. Load source data from a managed CSV, deterministic synthetic dataset, or supported GTFS subset.
2. Normalize the source into the canonical `Trip` model.
3. Seed canonical trips into SQLite.
4. Execute the search flow through:
   `search_service_request -> search_service_api -> search_service`
5. Derive expected results from normalized pandas data.
6. Compare expected versus actual results through row-level reconciliation, aggregate validation, and filter checks.
7. Package outcomes for Allure, exported report bundles, batch runs, run profiles, and suites.

## Repository Structure

- `src/domain/trip_search/`: canonical trip models, dataset profiles, selectors, and API-style service flow
- `src/framework/connectors/`: SQLite, CSV, GTFS, scenario, run-profile, and suite loaders
- `src/framework/execution/`: profile-aware CLI entrypoint
- `src/framework/reporting/`: Allure and export bundle packaging
- `src/transformers/`: raw-to-canonical mapping and GTFS transformation
- `src/validators/`: reconciliation, aggregate, quality, batch, and suite validators
- `tests/`: deterministic validation coverage across service, integration, filters, batch, suite, entrypoint, large dataset, and GTFS flows
- `data/raw/`: sample datasets, scenario CSVs, run profiles, suites, and GTFS sample input
- `docs/`: implementation and architecture notes

## Dataset Profiles

The framework supports three dataset profiles through one centralized asset-resolution layer.

### `small`

- Trip source: `data/raw/sample_trips.csv`
- Scenario dataset: `data/raw/batch_trip_search_scenarios.csv`
- Purpose: fast local smoke and baseline validation

### `large`

- Trip source: deterministic synthetic dataset builder
- Scenario dataset: `data/raw/large_batch_trip_search_scenarios.csv`
- Purpose: broader regression coverage without unstable runtime cost

### `gtfs`

- Trip source: `data/raw/gtfs_sample/`
- Scenario dataset: `data/raw/gtfs_batch_trip_search_scenarios.csv`
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

External CSV scenario packs define deterministic search requests:

- `scenario_id`
- `origin`
- `destination`
- `departure_date`
- optional `carrier`
- optional `stops_count`
- optional metadata: `pack`, `tag`, `scenario_type`

### Run profiles

External JSON run profiles select scenario subsets and label one validation run.

Examples:

- `data/raw/default_trip_search_run_profile.json`
- `data/raw/large_filters_trip_search_run_profile.json`
- `data/raw/gtfs_multidate_trip_search_run_profile.json`

### Run suites

External JSON suites execute ordered run profiles with suite-level policies and rollups.

Examples:

- `data/raw/default_trip_search_run_suite.json`
- `data/raw/large_trip_search_run_suite.json`
- `data/raw/gtfs_trip_search_run_suite.json`

## CLI Usage

The framework exposes one profile-aware entrypoint at `src/framework/execution/trip_search_entrypoint.py`.

Examples:

```powershell
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --dataset-profile small
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --dataset-profile large --execution-mode batch
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --execution-mode run-profile --run-profile-path data/raw/gtfs_multidate_trip_search_run_profile.json
.\.venv\Scripts\python -m src.framework.execution.trip_search_entrypoint --execution-mode suite --run-suite-path data/raw/large_trip_search_run_suite.json
```

Default behavior:

- If only `--dataset-profile` is provided, execution defaults to `suite`.
- The selected profile resolves its default scenario dataset, run profile, and suite assets automatically.

## Reporting

### Allure

Allure is the primary reporting layer. The framework attaches:

- scenario-level expected, actual, and mismatch artifacts
- aggregate comparison artifacts
- batch summaries and issue-category rollups
- suite summaries, policy results, and final status views

Example:

```powershell
.\.venv\Scripts\python -m pytest tests --alluredir=artifacts/allure-results
```

### Exported artifacts

When `TRIP_REPORT_EXPORT_DIR` is set, the same bundle data used for Allure is exported to disk.

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

## CI/CD

GitHub Actions is set up with reusable validation workflows:

- `.github/workflows/run-validation.yml`
- `.github/workflows/pr-validation.yml`
- `.github/workflows/manual-run.yml`
- `.github/workflows/nightly-regression.yml`

The workflows:

- install dependencies
- run pytest with Allure output enabled
- export report bundles to disk
- upload `artifacts/` for CI review

## This repository demonstrates

- deterministic data-first validation design
- reusable service/API/request separation
- normalized expected-result derivation with pandas
- row-level and aggregate reconciliation
- profile-aware execution and asset resolution
- batch, run, and suite orchestration
- Allure plus machine-readable export packaging
- realistic GTFS-derived source transformation

It shows how to build and maintain a validation platform, not just a collection of tests.

## Local Setup

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m pytest tests -q -rs -p no:cacheprovider
```

Environment defaults are documented in `.env.example`.

## Supporting Docs

- `docs/implementation-plan.md`
- `docs/target-architecture.md`
- `docs/validation-strategy.md`
- `docs/reporting-and-ci.md`
