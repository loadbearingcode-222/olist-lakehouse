# 0001: Lake core stack and image pins
Date: 2026-09-24 · Story: M0-S3 · Status: accepted

## Context
The lakehouse needs S3-compatible object storage, an Iceberg catalog that every engine (Trino
now, Spark in M2, dbt through Trino in M3) can share, and a SQL engine, all running locally in
Docker with no cloud account. Pins must be exact and checked against the live registries, not
remembered.

## Options
- **Catalog:** Lakekeeper (Rust, Iceberg REST, Postgres-backed); Apache Polaris (better known,
  heavier bootstrap with realms and principals); Project Nessie (git-like branches, Quarkus);
  a JDBC or Hive catalog (not REST, so each engine configures storage differently).
- **Object storage:** MinIO (the de-facto local S3; the community edition stopped publishing
  new feature images in late 2025); SeaweedFS (actively released); Garage.
- **SQL engine:** Trino (the engine behind Athena and Starburst); DuckDB (embedded, not a
  shared service).

## Decision
Lakekeeper + MinIO + Trino, with Postgres as Lakekeeper's store. The pins below were checked
live on **2026-09-24** (Docker Hub, quay.io and GitHub release APIs) and pulled.

| Component | Image | Why this tag |
|---|---|---|
| MinIO | `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z.hotfix.7aa24e772` | the newest official build (a 2026-04 security hotfix of the last feature release) |
| MinIO client | `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z` | the newest `mc` |
| Postgres | `postgres:17.11-bookworm` | latest 17.x; 17 is also Debezium's well-trodden major (M1) |
| Lakekeeper | `quay.io/lakekeeper/catalog:v0.13.6` | latest release, 2026-09-22 |
| Trino | `trinodb/trino:483` | latest release, 2026-07-18 |
| curl (bootstrap) | `curlimages/curl:8.16.0` | one-shot HTTP client |

Warehouse `lakehouse`: bucket `lakehouse`, prefix `warehouse`, `flavor: s3-compat`,
path-style, **STS and remote signing off**, hard deletes. Every engine brings its own S3
credentials instead of receiving vended ones.

## Cost (honest)
- MinIO's community line gets security hotfixes but no new features. If an engine needs an S3
  feature it lacks, the swap is SeaweedFS behind the same endpoint and credentials, since only
  `compose.yaml` names MinIO.
- Lakekeeper is younger than Polaris and less known to recruiters. The REST protocol is the
  portable part: Trino and Spark talk Iceberg REST, not Lakekeeper.
- Lakekeeper and Trino both use the MinIO root credentials. A dedicated least-privilege access
  key per engine is the production shape; it is deferred, and this line is the reminder.
- No authentication on Lakekeeper or Trino. This is acceptable only because every port is bound
  to 127.0.0.1.

## Revisit when
- A MinIO CVE has no hotfix image, or an engine needs an S3 API MinIO lacks → SeaweedFS.
- Spark (M2-S1) cannot write through the REST catalog with static credentials → re-evaluate
  vended credentials (STS) or remote signing.
- Lakekeeper's management API changes shape. `config/lakekeeper/bootstrap.sh` follows the
  v0.13.6 OpenAPI; field names changed between versions (`aws-access-key-id` →
  `access-key-id`, `minio` → `s3-compat`).
