"""Kafka → Iceberg bronze: land every message, exactly once, raw payload untouched.

Two streaming queries in one Spark application:

    olist.olist.*        → lake.bronze.cdc_events           (Debezium envelopes + tombstones)
    clickstream.events   → lake.bronze.clickstream_events

Bronze keeps the Kafka coordinates (topic, partition, offset, timestamp), the raw key and payload
string, and a few envelope fields pulled out for pruning and debugging. Parsing into typed
columns happens in silver (M3), where a bad row can be dealt with, never here where it would
be lost.

Exactly-once has two halves:
- Spark's checkpoint (in MinIO, s3a://lakehouse/checkpoints/...) records which offsets each
  micro-batch covered, so a restart re-runs the last unfinished batch with the same offsets;
- the write is idempotent by natural key: before appending, a batch anti-joins its
  (topic, partition, offset) against what the table already holds in that offset range (file
  pruning on the offset column's min/max keeps this cheap). A batch that committed to Iceberg
  just before a crash therefore re-runs as a no-op.
"""

from __future__ import annotations

import os
import signal
import sys
from types import FrameType
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINTS = os.environ.get("BRONZE_CHECKPOINTS", "s3a://lakehouse/checkpoints/bronze")
TRIGGER = os.environ.get("BRONZE_TRIGGER", "20 seconds")
MAX_OFFSETS = os.environ.get("BRONZE_MAX_OFFSETS_PER_TRIGGER", "200000")

CDC_TABLE = "lake.bronze.cdc_events"
CLICK_TABLE = "lake.bronze.clickstream_events"

KAFKA_COLUMNS = """
    kafka_topic      STRING    NOT NULL,
    kafka_partition  INT       NOT NULL,
    kafka_offset     BIGINT    NOT NULL,
    kafka_timestamp  TIMESTAMP NOT NULL,
    record_key       STRING,
    payload          STRING
"""

DDL = {
    CDC_TABLE: f"""
        CREATE TABLE IF NOT EXISTS {CDC_TABLE} (
            {KAFKA_COLUMNS},
            source_table     STRING NOT NULL,
            op               STRING,
            source_lsn       BIGINT,
            source_ts_ms     BIGINT,
            ingested_at      TIMESTAMP NOT NULL,
            batch_id         BIGINT NOT NULL
        ) USING iceberg
        PARTITIONED BY (source_table, days(ingested_at))
        TBLPROPERTIES ('format-version' = '2')
    """,
    CLICK_TABLE: f"""
        CREATE TABLE IF NOT EXISTS {CLICK_TABLE} (
            {KAFKA_COLUMNS},
            event_id         STRING,
            event_type       STRING,
            event_ts         TIMESTAMP,
            schema_version   INT,
            ingested_at      TIMESTAMP NOT NULL,
            batch_id         BIGINT NOT NULL
        ) USING iceberg
        PARTITIONED BY (days(ingested_at))
        TBLPROPERTIES ('format-version' = '2')
    """,
}


def kafka_frame(spark: SparkSession, **subscription: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .options(**subscription)
        .load()
        .select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("record_key"),
            F.col("value").cast("string").alias("payload"),
        )
    )


def cdc_columns(frame: DataFrame) -> DataFrame:
    # The table comes from the topic name, so tombstones (null payload) still have one.
    return frame.select(
        "*",
        F.element_at(F.split("kafka_topic", r"\."), -1).alias("source_table"),
        F.get_json_object("payload", "$.op").alias("op"),
        F.get_json_object("payload", "$.source.lsn").cast("bigint").alias("source_lsn"),
        F.get_json_object("payload", "$.source.ts_ms").cast("bigint").alias("source_ts_ms"),
    )


def click_columns(frame: DataFrame) -> DataFrame:
    # schema_drift events rename event_type to eventType; bronze records both spellings as one.
    return frame.select(
        "*",
        F.get_json_object("payload", "$.event_id").alias("event_id"),
        F.coalesce(
            F.get_json_object("payload", "$.event_type"),
            F.get_json_object("payload", "$.eventType"),
        ).alias("event_type"),
        F.to_timestamp(F.get_json_object("payload", "$.event_ts")).alias("event_ts"),
        F.get_json_object("payload", "$.schema_version").cast("int").alias("schema_version"),
    )


def append_new(batch: DataFrame, table: str, batch_id: int) -> int:
    """Append the rows of `batch` whose Kafka coordinates `table` does not hold yet."""
    spark = batch.sparkSession
    batch = batch.withColumn("ingested_at", F.current_timestamp()).withColumn(
        "batch_id", F.lit(batch_id).cast("bigint")
    )
    batch.persist()
    try:
        ranges = (
            batch.groupBy("kafka_topic", "kafka_partition")
            .agg(F.min("kafka_offset").alias("lo"), F.max("kafka_offset").alias("hi"))
            .collect()
        )
        if not ranges:
            return 0
        condition = " OR ".join(
            f"(kafka_topic = '{r.kafka_topic}' AND kafka_partition = {r.kafka_partition}"
            f" AND kafka_offset BETWEEN {r.lo} AND {r.hi})"
            for r in ranges
        )
        existing = (
            spark.table(table)
            .where(condition)
            .select("kafka_topic", "kafka_partition", "kafka_offset")
        )
        fresh = batch.join(
            existing, ["kafka_topic", "kafka_partition", "kafka_offset"], "left_anti"
        )
        count = int(fresh.count())
        if count:
            fresh.writeTo(table).append()
        skipped = batch.count() - count
        print(
            f"bronze: {table} batch {batch_id}: +{count:,} rows"
            + (f", {skipped:,} already present (replayed batch)" if skipped else ""),
            flush=True,
        )
        return count
    finally:
        batch.unpersist()


def writer(table: str) -> Any:
    def write(batch: DataFrame, batch_id: int) -> None:
        append_new(batch, table, batch_id)

    return write


def main() -> int:
    spark = SparkSession.builder.appName("olist-bronze-stream").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    for ddl in DDL.values():
        spark.sql(ddl)

    cdc = (
        cdc_columns(kafka_frame(spark, subscribePattern=r"olist\.olist\..*"))
        .writeStream.queryName("bronze_cdc")
        .option("checkpointLocation", f"{CHECKPOINTS}/cdc")
        .trigger(processingTime=TRIGGER)
        .foreachBatch(writer(CDC_TABLE))
        .start()
    )
    click = (
        click_columns(kafka_frame(spark, subscribe="clickstream.events"))
        .writeStream.queryName("bronze_clickstream")
        .option("checkpointLocation", f"{CHECKPOINTS}/clickstream")
        .trigger(processingTime=TRIGGER)
        .foreachBatch(writer(CLICK_TABLE))
        .start()
    )
    queries = [cdc, click]

    def stop(signum: int, frame: FrameType | None) -> None:
        print("bronze: stop requested; stopping queries after their current batch", flush=True)
        for query in queries:
            query.stop()

    signal.signal(signal.SIGTERM, stop)
    print(f"bronze: streaming from {KAFKA}, checkpoints under {CHECKPOINTS}", flush=True)
    spark.streams.awaitAnyTermination()
    failed = [q for q in queries if q.exception() is not None]
    for query in failed:
        print(f"bronze: query {query.name} failed: {query.exception()}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
