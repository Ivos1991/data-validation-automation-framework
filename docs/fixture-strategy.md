# Fixture Strategy

## Objective

Keep fixtures explicit, composable, and aligned to validation intent.

## Fixture Levels

### Root `conftest.py`

Keep only broad infrastructure fixtures:

- `config`
- SQLite database bootstrap path or temporary DB location
- shared dataset path resolution
- common pandas display/debug options if useful
- shared Allure environment hooks when justified

### Local `conftest.py`

Keep scenario-specific setup close to the tests that use it:

- dataset slices for a specific trip-search scenario
- search criteria fixtures
- expected-result DataFrame fixtures
- seeded DB fixtures for one validation slice
- specialized tolerance or edge-case fixtures

Local fixture modules may also contain small scenario helpers, following the offline extractor pattern:

- helper functions to build before/after snapshots
- helper functions to wait for a condition through a shared retry utility
- helper functions to shape expected records for one test family

## Ownership Rules

- the fixture that creates mutable state owns cleanup
- dataset preparation should stay visible in the test signature
- avoid hidden fixture chains that obscure which data slice a test is validating
- if a fixture exists for one test class or one validation mode, keep it local
- if setup depends on a sequence, document the sequence at the top of the local `conftest.py`

## Recommended Fixture Examples

### Service Tests

- `search_criteria_any_nyc_to_bos`
- `seeded_trip_db`
- `expected_source_rows_for_route`

### Reconciliation Tests

- `normalized_expected_trip_frame`
- `api_search_response_payload`
- `reconciliation_result`
- `db_snapshot_before_search`
- `db_snapshot_after_search` when a scenario needs before/after comparisons

### Data Quality Tests

- `source_dataset_with_duplicate_trip_id`
- `source_dataset_with_invalid_departure_date`

## Teardown Philosophy

Prefer short-lived SQLite databases for deterministic tests:

- create schema for the slice
- seed only the data needed
- discard the DB after the test or test module

That keeps state simple and makes diagnostics easier.
