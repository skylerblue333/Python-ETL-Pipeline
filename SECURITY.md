# Security Policy

## Status

Sky ETL is an **engineering beta**. CI validates the current implementation, but production deployment, multi-tenant isolation, secrets management, and infrastructure controls are not verified by this repository.

## Supported versions

Security fixes target the current `main` branch and the latest tagged release when one exists.

## Reporting

Do not open public issues containing credentials, customer data, private datasets, or exploitable vulnerability details. Use GitHub private vulnerability reporting when enabled for this repository.

## Security boundaries

The pipeline constrains input/output paths to configured directories and writes Parquet output through a temporary file before replacement. It does not fetch exchange rates, execute user-provided code, manage cloud credentials, encrypt source datasets, or provide authorization between tenants.

Operators remain responsible for filesystem permissions, sensitive-data classification, encryption at rest/in transit, dependency update policy, backup/restore, and runtime/container hardening outside this image.
