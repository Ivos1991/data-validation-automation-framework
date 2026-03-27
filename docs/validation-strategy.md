# Validation Strategy

## Objective

Validate a trip search system from multiple angles while keeping all comparisons grounded in normalized data.

## Validation Categories

### Row-Level Checks

Confirm each returned trip is correct at the record level:

- expected fields are present after mapping
- route and date fields match the requested criteria
- prices, durations, and carriers match the expected source record
- individual mismatches produce targeted diagnostics

### Aggregate Checks

Confirm grouped and summary behavior is consistent:

- result count
- min and max price
- average price
- grouped counts by carrier, route, or date
- optional percentile or distribution checks when useful

### Reconciliation Checks

Compare SQLite-backed expected data with API results:

- missing trips
- unexpected extra trips
- field-level mismatches on overlapping trip IDs
- stable ordering checks only when ordering is part of the API contract

### Data Quality Checks

Validate the source dataset and seeded DB quality:

- nulls in required fields
- duplicate trip IDs
- impossible date ranges
- negative prices or durations
- inconsistent route or currency values

## Tool Usage

### pandas

Use pandas to:

- filter source datasets into expected result sets
- project canonical comparison tables
- compute grouped summaries and aggregates
- render mismatch tables for reporting

### NumPy

Use NumPy to:

- support tolerance-aware numeric comparison
- calculate efficient aggregate statistics
- handle vectorized null and range checks

### SQLite

Use SQLite to:

- persist normalized trip records locally
- represent the framework's queryable expected-data store
- support deterministic DB-to-API reconciliation in tests

## Assertion Philosophy

- compare business intent first
- prefer canonical key-based matching over raw payload shape checks
- keep tolerances centralized and explicit
- attach enough evidence to explain failures without reading source code
- when several related fields should be evaluated together, prefer soft assertions so one failure report shows the full mismatch surface
