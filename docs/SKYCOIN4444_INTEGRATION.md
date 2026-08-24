# SKYCOIN4444 integration

Sky ETL remains independently runnable. SKYCOIN4444 should consume it through a job boundary rather than copying pipeline code into the flagship monorepo.

## Recommended contract

Provide an isolated input directory containing a CSV with `user_id`, `transaction_amount`, and `status`, invoke the CLI with an isolated output directory and `.parquet` target, and consume the emitted Parquet file only after a zero exit status.

Example:

```bash
python main.py /jobs/42/input /jobs/42/output transactions.csv clean.parquet
```

If currency conversion is required, SKYCOIN4444 must obtain and audit the authoritative rate itself and pass it explicitly with `--usd-rate`. Sky ETL deliberately does not fetch or fabricate market data.

## Integration ownership

The caller owns job scheduling, retries, object storage transfer, secrets, access control, lineage metadata, and retention policy. This repository owns deterministic local transformation and row-accounting behavior only.

## Future adapter

A future ecosystem adapter may wrap this CLI in Sky Queue or Sky Workflow. That adapter should preserve idempotent job identifiers, isolate per-job directories, capture stdout/stderr and exit status, and publish output metadata without weakening this repository's standalone interface.
