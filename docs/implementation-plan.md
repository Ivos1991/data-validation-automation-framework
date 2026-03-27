# Implementation Plan

## Current Implementation Goal

Deliver a narrow but complete first slice that proves the data-validation architecture end to end without overbuilding the repository.

## Recommended First Slice

Validate one deterministic trip-search scenario:

- load a small structured trip dataset
- normalize it into a canonical trip model
- seed the normalized rows into SQLite
- implement one search service operation, such as route + departure-date filtering
- compute expected results from source data with pandas
- call the search service
- reconcile API results against expected normalized trips
- publish diagnostics through Allure

## Why This Slice First

It proves the core repository promises with minimal surface area:

- dataset ingestion
- normalization
- SQLite persistence
- service/API/request layering
- normalized mapper boundaries
- pandas-assisted expectation building
- Allure evidence

## Suggested Delivery Order

1. repository scaffolding and dependency setup
2. config manager and root `config` fixture
3. canonical trip model and normalization helpers
4. dataset loader and seed pipeline
5. SQLite schema, connector, and row mapper
6. trip search request, API, and service modules
7. row-level and reconciliation validators
8. first deterministic service and reconciliation tests
9. Allure helper attachments

## Deferred Until Later

- multi-criteria aggregate benchmarking
- large dataset performance checks
- fuzzy matching rules across inconsistent source systems
- optional real-environment runtime variants
