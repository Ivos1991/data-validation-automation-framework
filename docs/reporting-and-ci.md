# Reporting And CI

## Reporting

Allure is the reporting solution for this repository.

### Reporting Principles

- Allure annotations belong in tests
- reusable attachment formatting belongs in the framework reporting layer
- validators should return structured results that tests can attach cleanly
- repository and transport layers must not assemble scenario-level narrative output

### Planned Reporting Helpers

- attach normalized expected vs actual trip snapshots
- attach reconciliation mismatch tables
- attach aggregate comparison summaries
- attach data-quality violation tables
- attach search request and response summaries when explicitly enabled

## CI Direction

The first CI target should remain small and deterministic:

- install Python dependencies
- run linting and unit-style validation tests
- run the seeded SQLite service and reconciliation slices
- publish Allure results as artifacts

## Initial Workflow Shape

- PR validation workflow for deterministic local-data slices
- manual workflow for broader datasets or heavier validation suites

Do not depend on an external live system in the first CI phase.

## Later CI Expansion

After the initial slice is stable, CI can expand to:

- scheduled data-quality regressions
- larger reconciliation suites
- optional environment-backed API execution when a stable target is available

## Publishing Note

Allure results should be treated as the primary test evidence for the portfolio:

- concise scenario names
- visible expected vs actual artifacts
- clear failure diagnostics for mismatched trips, counts, and aggregates
