# 0003: Spark Structured Streaming into Iceberg bronze
Date: 2026-09-24 · Story: M2-S1 · Status: accepted

## Context
Every Kafka message (the CDC envelopes and the clickstream) must land in Iceberg bronze on
MinIO, exactly once, surviving a hard kill of the job. The engine, its version set, and the
mechanism for exactly-once all had to be chosen together.

## Options
- **Engine:** Spark Structured Streaming (the Databricks/EMR/Glue skill); Flink (lower latency,
  a second JVM platform to learn); Kafka Connect's Iceberg sink (no code, but no place for
  the M2-S2 quality gate).
- **Version set:** Spark 4.2.0 (newest), or Spark 4.1.3 + Iceberg 1.11.0 (newest supported
  pair).
- **Exactly-once:** (a) the Iceberg streaming sink's own epoch tracking; (b) `foreachBatch` +
  a batch-id marker in the snapshot summary; (c) `foreachBatch` + an anti-join on the Kafka
  coordinates (topic, partition, offset).
- **Deployment:** a standalone master and worker, or `local[4]` in one container.

## Decision
- **Engine:** Spark Structured Streaming, `local[4]` in one container (`bronze-stream`, 3 GB
  limit). The Spark UI is at http://localhost:24040.
- **Version set:** checked live on 2026-09-24. Iceberg ships no Spark 4.2 runtime yet, so:
  - `apache/spark:4.1.3-scala2.13-java21-python3-ubuntu`
  - `iceberg-spark-runtime-4.1_2.13:1.11.0` and `iceberg-aws-bundle:1.11.0`
  - `spark-sql-kafka-0-10_2.13:4.1.3` with `kafka-clients:3.9.1` and `commons-pool2:2.12.1`,
    exactly as its POM declares
  - `hadoop-aws:3.4.2`, which must equal the image's Hadoop, with the AWS SDK `bundle:2.29.52`
    that hadoop-aws 3.4.2 declares

  Every jar is SHA-1 verified against Maven Central at build time.
- **Exactly-once:** option (c). Spark's checkpoint (`s3a://lakehouse/checkpoints/bronze/...`)
  makes a restart re-run the unfinished batch with the same offsets. The write then appends
  only the rows whose (topic, partition, offset) the table does not already hold in that
  offset range. It is chosen over (b) because it needs no query id and stays correct even if
  a checkpoint is reset: the job re-reads and skips what is already there. It is chosen over
  (a) because M2-S2 needs `foreachBatch` for the quality gate.
- **Python:** the job runs on the project's own Python 3.12, a uv venv from `uv.lock` inside
  the Spark image, not the image's 3.10.

## Cost (honest)
- **Image size:** the AWS SDK bundle is 641 MB and is needed only for S3A checkpoints (Iceberg's
  own bundle lacks S3A's transfer manager). The image is about 2 GB. Hand-picking SDK modules
  would shrink it but make it fragile to version skew.
- **Local mode** is one JVM. It shows Structured Streaming faithfully but not cluster
  scheduling or shuffle across executors.
- **The anti-join** reads a small offset range of the target on every batch. File pruning on
  `kafka_offset` min/max keeps it cheap, but it grows with small files until compaction runs
  (M4-S2).
- **Delivery is at-least-once upstream:** Connect and the clickstream producer can re-send.
  Bronze keeps distinct offsets, not distinct events; event-level dedup is silver's job (M3).

## Revisit when
- Iceberg publishes a Spark 4.2 runtime (bump both together).
- Micro-batch latency or the anti-join cost becomes visible in the Spark UI.
- A second streaming job needs its own resources (then a standalone cluster).

## Evidence
`tests/integration/test_bronze.py` SIGKILLs the job while producers write, restarts it, and
waits for catch-up: 1,139,457 bronze rows across 30 partitions equal the Kafka offsets, all
distinct. Red team: deleting checkpoint `commits/8` made Spark re-run batch 8, which logged
`+0 rows, 1,194 already present (replayed batch)`.
