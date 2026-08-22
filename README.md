# Python ETL Pipeline

A focused, deterministic batch pipeline that reads transaction records from CSV, validates and normalizes them, and writes clean records to Parquet. The repository is an implementation component, not an enterprise-scale distributed data platform.

## Implemented behavior

The pipeline validates required columns (`user_id`, `transaction_amount`, and `status`), coerces numeric fields, normalizes status text, rejects malformed or negative transaction rows, reports accepted and rejected row counts, and writes output atomically. An optional USD conversion is supported only when the caller supplies an authoritative rate; no exchange rate is fabricated.

Input and output paths are constrained to their configured directories to reduce traversal risk. Missing files and schema errors fail fast with explicit exceptions. The CLI prints the output path and row accounting only after the output file has been written successfully.

## Usage

```bash
python3 -m pip install -r requirements.txt
python3 main.py ./input ./output transactions.csv clean.parquet
python3 main.py ./input ./output transactions.csv rated.parquet --usd-rate 1.25
```

## Validation

```bash
python3 -m pytest -q
```

The current test suite covers invalid-row rejection, explicit conversion rates, missing-column failures, path traversal protection, and invalid-rate handling.

## Scope and limitations

This implementation is local batch ETL. It does not provide distributed execution, scheduling, lineage, streaming, warehouse connectors, retries across workers, secrets management, or production deployment. Apache Beam, Dagster, and Prefect were reviewed as architectural references; no source code was copied from those projects. Any future integration must preserve their applicable license and attribution requirements.

The repository’s former “enterprise-grade” and “cloud-native” language was removed because the current implementation does not substantiate those claims.

## Attribution

Copyright and authorship remain with the repository owner. The implementation changes are original repository work and use the existing project dependencies under their respective licenses.
