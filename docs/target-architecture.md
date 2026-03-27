# Target Architecture

## Goal

Build a portfolio-ready, data-driven validation framework for a generic trip search system with clear data boundaries and credible reporting.

## Repository Shape

```text
data-validation-automation-framework/
|-- docs/
|-- data/
|   |-- raw/
|   `-- curated/
|-- src/
|   |-- framework/
|   |   |-- config/
|   |   |-- db/
|   |   |-- io/
|   |   |-- normalization/
|   |   |-- models/
|   |   |-- mapping/
|   |   `-- reporting/
|   |-- services/
|   |   `-- trip_search_service/
|   `-- validators/
|       |-- row_level/
|       |-- aggregate/
|       |-- reconciliation/
|       `-- data_quality/
|-- tests/
|   |-- services_tests/
|   |-- reconciliation_tests/
|   |-- integration_tests/
|   `-- data_quality_tests/
|-- conftest.py
`-- README.md
```

## Layer Ownership

### `src/framework/config`

- environment-driven settings
- file paths
- SQLite location
- API base URL and timeouts
- reporting toggles
- tolerance configuration for numeric comparisons

### `src/framework/db`

- SQLite connector management
- schema bootstrap
- query execution
- repository helpers
- no business assertions

### `src/framework/io`

- file and dataset loading
- CSV, JSON, or parquet entry points as needed later
- no normalization beyond basic file reading

### `src/framework/normalization`

- centralized date parsing
- duration normalization
- numeric normalization and tolerance helpers
- string and enum canonicalization
- null/default handling

### `src/framework/mapping`

- DB row mapper from raw SQLite rows to normalized trip models
- API response mapper from raw JSON payloads to normalized trip models
- optional dataset-to-model mapper during seeding

### `src/services/trip_search_service`

- `trip_search_service.py` exposes test-facing operations
- `trip_search_service_api.py` owns transport execution
- `trip_search_service_request.py` builds search requests from typed criteria

### `src/validators`

- row-level validators compare individual normalized trips
- aggregate validators compare counts, min/max, averages, and grouped summaries
- reconciliation validators compare SQLite-backed expected results to API results
- data-quality validators check source completeness, duplicates, invalid ranges, and rule violations

## Core Data Flow

1. load structured source dataset
2. normalize records into the canonical trip model
3. seed normalized records into SQLite
4. query SQLite for expected trip sets
5. call the trip search API
6. map API results into the same canonical model
7. validate row-level, aggregate, reconciliation, and quality rules
8. attach diagnostics to Allure

## Architectural Rules

- tests never consume raw DB rows
- validators never parse raw source-specific formats
- all cross-source comparisons happen on normalized data
- SQLite is the local persisted truth store, not a shortcut around normalization
- pandas is used for expected-set shaping and aggregate comparison, not as a replacement for mapper boundaries

## First Scope

The first implementation slice should support one search flow with one dataset and enough plumbing to prove the architecture end to end.
