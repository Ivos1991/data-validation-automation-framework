# Data-Layer Conventions

## Objective

Keep all tests and validators insulated from raw source-specific and DB-specific field formats.

## Core Rules

- raw DB outputs are never used directly in tests
- parsing and normalization are isolated in focused helpers and mapper modules
- date parsing is centralized
- numeric normalization and tolerances are centralized
- mappers convert raw DB rows into standardized internal structures
- tests and validators operate only on normalized data

## Canonical Internal Model

Every comparison path should converge on one normalized trip structure. The exact implementation can be a dataclass, typed dict, or model object, but it should standardize fields such as:

- trip ID
- origin
- destination
- departure datetime
- arrival datetime
- duration minutes
- price amount
- currency
- carrier
- stops count
- cabin or service class

## Parsing Ownership

### Date Parsing

Own all source-to-datetime conversion in one date helper module:

- SQLite text dates
- source dataset date strings
- API response timestamps

Tests should never call `datetime.strptime` for business comparisons.

### Numeric Normalization

Own all numeric coercion and tolerance decisions in one numeric helper module:

- decimals or floats to normalized numeric form
- duration rounding
- price rounding strategy
- tolerance-aware comparison helpers for aggregates

### Enum And String Normalization

Keep casing, whitespace, and label harmonization centralized so tests compare canonical values instead of source-specific variants.

## Mapper Responsibilities

### Dataset Mapper

- converts raw source rows into the canonical trip model for seeding

### DB Row Mapper

- converts raw SQLite rows into canonical trip models
- hides SQLite column order and text-format details

### API Response Mapper

- converts raw response payloads into canonical trip models
- hides field naming and optional-field inconsistencies

## Query Layer Rules

- query modules may return raw SQLite rows internally
- raw rows must be mapped before crossing the repository boundary
- query modules should expose business-focused methods, such as `find_trips_by_route_and_date`
- the thin-handler idea from the old `mysql_handler.py` is acceptable, but the SQLite layer must add stricter mapping boundaries than the old repo used

## Validator Input Rules

Validators may accept:

- normalized trip objects
- canonical dictionaries
- canonical pandas DataFrames

Validators must not accept:

- raw cursor tuples
- unparsed API payload fragments
- source-specific date strings

## Change-Isolation Goal

If the DB field format changes, only the mapper and focused normalization helpers should need updates. Test intent and validator logic should remain stable.
