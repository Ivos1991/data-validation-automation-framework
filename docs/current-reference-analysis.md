# Current Reference Analysis

## Reference Workflow To Preserve

The previous portfolio repositories established a documentation-first workflow before implementation:

1. audit the current reference repository
2. extract reusable patterns
3. define the target architecture
4. define fixture, reporting, and coding conventions
5. define the first implementation slice
6. implement only after the planning documents agree

This repository should keep that order.

## Additional Legacy References Reviewed

The following old-repo artifacts add useful style guidance for this project:

- `core/core_utils/mysql_handler.py`
- `core/core_utils/date_and_time_utils.py`
- `core/core_utils/retry_utils.py`
- `testing_environment/tests/end_to_end_tests/oms/account/fie/offline_extractor/test_offline_extractor.py`
- `testing_environment/tests/end_to_end_tests/oms/account/fie/offline_extractor/conftest.py`

## Structural Patterns Worth Reusing

- a `docs/` set that explains architecture before code exists
- a root `config` fixture pattern so tests consume one explicit configuration object
- thin service modules used by tests instead of direct transport calls
- clear `service -> service_api -> service_request` separation where the target is an API
- Allure annotations in tests, with reusable attachment helpers in the framework layer
- small root fixtures and local `conftest.py` files for scenario-specific data
- fixture-driven setup chains that make before/after state explicit
- business-readable assertions using `assertpy`, including soft assertions where several related checks should report together
- focused retry helpers for eventual-consistency polling in fixtures instead of ad hoc sleep loops

## What Must Change For This Project

The business model is different from the earlier API portfolio projects. This repository is not primarily a request/response microservice harness. It is a data-driven validation framework centered on:

- dataset ingestion
- normalization into a canonical trip model
- SQLite as the local truth store
- reconciliation between SQLite-backed expected data and search API results
- aggregate and data-quality validation using pandas and NumPy

That shifts the core architecture from transport-first to data-first.

It also means the old MySQL utility should be treated as a style reference, not copied literally:

- keep the thin handler idea
- replace MySQL with SQLite-native connector and query abstractions
- avoid passing raw DB dictionaries straight into tests
- parameterize queries where practical instead of preserving string-built SQL as a default pattern

## Layer Responsibilities Confirmed For The New Repo

### `*_service.py`

- exposes test-facing search operations
- accepts the shared `config` object
- orchestrates request building, API execution, and response mapping
- does not contain heavy validation rules

### `*_service_api.py`

- owns endpoint paths and HTTP execution
- maps one operation to one API contract
- remains thin and transport-focused

### `*_service_request.py`

- shapes query parameters and request payloads
- keeps search-criteria construction separate from HTTP execution

### Data And Mapping Layers

New mandatory layers for this repository:

- dataset loader for source files
- normalization helpers for dates, money, durations, and categorical values
- SQLite connector and query layer
- DB row mapper that converts raw SQLite rows into normalized trip models
- API response mapper that converts raw search responses into the same normalized trip models

## Test Style Confirmed From The Offline Extractor Example

The offline extractor test and its local `conftest.py` confirm a test style worth preserving:

- test classes annotated with Allure suite hierarchy
- tests remain compact and readable because setup lives in local fixtures
- fixtures capture before and after snapshots explicitly
- helper functions inside `conftest.py` support the fixture flow without becoming global utilities too early
- retries for asynchronous state changes belong in fixtures or focused helpers, not in test bodies
- test assertions describe business expectations rather than transport mechanics

## Key Architectural Constraint

Raw DB outputs must not flow into tests or validators. The earlier repos already preferred thin boundaries; this repo must make them stricter:

- raw SQL rows stay inside repository/query modules
- parsing stays in focused helper utilities
- mappers emit normalized internal structures
- validators and tests consume only normalized data

## New Repo Direction

The new framework should preserve the previous repos' explicit layering style while changing the center of gravity:

- `src/framework/` owns config, SQLite access, dataset IO, normalization, and reporting helpers
- `src/services/` owns the search API client pattern
- `src/validators/` owns row-level, aggregate, reconciliation, and data-quality validation
- `tests/` stays split by test intent rather than by technical utility

Implementation should begin only after these boundaries are documented and agreed.
