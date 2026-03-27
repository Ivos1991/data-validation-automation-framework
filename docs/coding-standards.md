# Coding Standards

## General

- prefer explicit behavior over hidden setup
- keep modules small and responsibility-focused
- use typed models where they clarify contracts
- keep deterministic local execution as the default
- prefer descriptive test assertions and fixture names over compact but opaque helpers

## Naming

- use business names that reflect trip-search behavior
- keep service modules named `<domain>_service.py`, `<domain>_service_api.py`, and `<domain>_service_request.py` where applicable
- name mapper modules by source and target, such as `db_row_to_trip_mapper.py`
- name normalization helpers by concern, such as `date_normalizer.py` or `numeric_normalizer.py`

## Layer Boundaries

- tests call service functions, repositories, or validators intentionally, never raw transport helpers by accident
- service modules orchestrate request builders and APIs and perform only thin transport-success validation
- repository modules return raw rows only to mapper modules, not to tests
- validators operate on normalized models, dictionaries, or DataFrames that already follow the canonical schema
- retry and polling helpers stay utility-owned; business tests should not embed custom retry loops

## Fixtures

- keep root fixtures infrastructure-oriented
- prefer local `conftest.py` when a fixture belongs to one validation slice
- the fixture that creates mutable state owns cleanup or reset

## Config

- expose configuration through a root `config` fixture
- prefer explicit env vars and simple defaults over sprawling config files
- keep tolerance and parsing configuration centralized

## Reporting

- Allure suite annotations belong in tests
- reusable attachments belong in `src/framework/reporting/`
- framework helpers may attach technical artifacts, but scenario interpretation belongs in tests

## Assertions

- assert business meaning, not incidental formatting
- avoid duplicating normalization logic inside assertions
- when comparing collections, compare normalized canonical structures
- centralize tolerances so assertion behavior does not drift across files
- prefer `assertpy` for readable assertions and grouped soft checks in larger validations
