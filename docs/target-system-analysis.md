# Target System Analysis

## Chosen Target

The target is a generic trip search system exposed through an API. The framework should assume the system accepts search criteria and returns trip options, while keeping the project generic enough for portfolio use.

## Expected Search Behavior

The first architecture pass assumes the API can support some subset of:

- origin and destination filtering
- departure date filtering
- optional return date filtering
- price and carrier filtering
- sorting or pagination

The initial implementation should choose the narrowest stable subset, not the full matrix.

## Source Of Truth Model

The framework source of truth is a structured trip dataset that is:

1. loaded from local files
2. normalized into a canonical trip model
3. seeded into SQLite

SQLite becomes the local persisted representation of expected data, while the original dataset remains the upstream source artifact.

## Validation Opportunities

This target supports several credible validation modes:

- source-data-to-DB correctness
- DB-to-API row reconciliation
- search-filter correctness
- result aggregate consistency
- source and DB data-quality verification

## Risks To Design Around

- source datasets may express dates in inconsistent formats
- APIs may return timestamps, prices, or enum-like values in different shapes than the source dataset
- ordering may be unstable unless explicitly documented
- float comparisons can create noise unless tolerance rules are centralized

## Best First Slice

The best first slice is one-way trip search for a single route and departure date because it proves:

- deterministic source filtering
- normalized date handling
- SQLite seeding and queryability
- request building for a realistic search flow
- clean reconciliation between expected and actual results

## Later Expansion Candidates

- multi-city or return-trip cases
- pagination validation
- sorting validation
- richer aggregate and quality dashboards
