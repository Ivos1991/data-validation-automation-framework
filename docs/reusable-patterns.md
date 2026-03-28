# Reusable Patterns

## Structural Patterns To Preserve

### 1. Documentation First

Create planning documents before implementation so architecture decisions are visible and stable.

### 2. Explicit Config Passing

Preserve the visible `config` fixture pattern:

- root `conftest.py` exposes a shared config object
- services, repositories, and validators receive config explicitly
- avoid hidden global environment reads during test execution

### 3. Service / API / Request Split

For the search system under test, keep the earlier service structure where it adds clarity:

- `trip_search_service.py`
- `trip_search_service_api.py`
- `trip_search_service_request.py`

Tests call service functions. Service functions call the request builder and API layer.

### 4. Root And Local Fixture Ownership

- root `conftest.py` owns only shared infrastructure
- local `conftest.py` files own dataset slices, search criteria fixtures, and scenario setup
- fixtures should reflect business meaning rather than technical shortcuts
- local fixture files may include small helper functions that support one scenario family, similar to the old offline extractor flow

### 5. Allure Boundaries

- Allure suite annotations stay in tests
- reusable attachments live in `src/framework/reporting/`
- validators can return rich result objects, but they should not format scenario-level reports themselves

### 6. Retry-As-Utility

Preserve the retry/polling pattern:

- polling behavior should be centralized
- retries should be explicit in fixture or helper usage
- eventual-consistency waiting should not be hand-written repeatedly in tests

For this repo, retries are only justified for environment-backed scenarios or delayed persistence checks, not for deterministic local SQLite assertions.

### 7. Assertion Style

Preserve the old repo's readable assertion style:

- use `assertpy` for descriptive assertions
- use `soft_assertions()` when a validation should report several related failures together
- keep comparison helpers narrow and business-focused

## Patterns To Introduce For This Repository

### Canonical Normalized Model

All source systems must converge on one internal trip representation before validation.

### Mapper-Owned Parsing

All source-format interpretation belongs in focused normalization helpers and mapper modules:

- date parsing
- numeric coercion
- currency/value rounding
- null/default handling
- list normalization

The old `date_and_time_utils.py` is a reminder to centralize time handling. In this repo, date and time utilities should be narrower and domain-focused:

- parsing source dataset timestamps
- parsing SQLite text fields
- parsing API timestamps
- formatting canonical values for comparison and reporting

### Validation By Intent

Separate validators by business purpose:

- row-level correctness
- aggregate consistency
- DB-to-API reconciliation
- data-quality rules

### DataFrame-Assisted Expected Results

Use pandas for expected-result shaping and aggregate comparison instead of embedding ad hoc loops in tests.

## Patterns Not To Carry Forward

- tests reading raw SQL tuples directly
- date parsing spread across test files
- response-format assumptions duplicated in many assertions
- oversized root fixture registries
- reporting logic mixed into transport or repository modules
