# Sky ETL — Python ETL Pipeline

**Status: engineering beta.** CI validates the current code, tests, dependency audit, container build, and non-root image user. Production infrastructure and distributed execution are not verified by this repository.

Sky ETL is a focused, deterministic batch product that reads transaction records from CSV, validates and normalizes them, and writes clean records to Parquet. It is intentionally a small standalone engineering product rather than a fabricated enterprise data platform.

## Implemented behavior

The pipeline validates required columns (`user_id`, `transaction_amount`, and `status`), coerces numeric fields, normalizes status text, rejects malformed or negative transaction rows, reports accepted and rejected row counts, and writes output atomically. An optional USD conversion is supported only when the caller supplies an authoritative positive rate; no exchange rate is fetched or fabricated.

Input and output paths are constrained to their configured directories to reduce traversal risk. Missing files and schema errors fail fast with explicit exceptions.

## Local usage

```bash
python3 -m pip install -r requirements.txt
python3 main.py ./input ./output transactions.csv clean.parquet
python3 main.py ./input ./output transactions.csv rated.parquet --usd-rate 1.25
```

## Container usage

```bash
docker build -t sky-etl .
docker run --rm \
  -v "$PWD/input:/data/input:ro" \
  -v "$PWD/output:/data/output" \
  sky-etl /data/input /data/output transactions.csv clean.parquet
```

The image runs as a non-root application user.

## Verification

```bash
python -m compileall -q main.py src tests
ruff check main.py src tests
pytest -q
pip-audit -r requirements.txt
docker build -t sky-etl:ci .
test "$(docker run --rm --entrypoint id sky-etl:ci -u)" != "0"
```

GitHub Actions runs these gates on pushes and pull requests.

## Architecture

`main.py` is the CLI boundary. `src/pipeline.py` owns path validation, extraction, deterministic transformation, row-quality filtering, and atomic Parquet loading. The caller owns scheduling, secrets, cloud storage, retries across jobs, and production infrastructure.

For ecosystem consumption, see [`docs/SKYCOIN4444_INTEGRATION.md`](docs/SKYCOIN4444_INTEGRATION.md). Sky ETL should be integrated through its stable job/CLI boundary rather than copied into the flagship codebase.

## Scope and limitations

This implementation is local batch ETL. It does not provide distributed execution, scheduling, lineage, streaming, warehouse connectors, worker coordination, secrets management, multi-tenant authorization, or a verified production deployment.

See [`SECURITY.md`](SECURITY.md) for security boundaries and [`CHANGELOG.md`](CHANGELOG.md) for release history.

## License and attribution

The repository retains its existing license and project history. External dependencies remain subject to their own licenses.
