# Changelog

All notable productization changes are documented here.

## Unreleased

### Added
- Pull-request CI with compile, Ruff, pytest, dependency audit, Docker build, and non-root image verification.
- Non-root CLI container runtime and Docker context exclusions.
- Explicit security boundaries and SKYCOIN4444 integration guidance.

### Changed
- Product status is stated as engineering beta; no distributed, scheduled, streaming, or production-deployment claim is made.

## 0.1.0 - baseline
- Deterministic CSV-to-Parquet ETL with schema validation, invalid-row rejection, path containment, optional caller-supplied USD conversion, atomic output replacement, and tests.
