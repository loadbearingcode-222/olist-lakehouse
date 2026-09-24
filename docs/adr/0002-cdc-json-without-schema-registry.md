# 0002: CDC events as plain JSON, without a schema registry
Date: 2026-09-24 · Story: M1-S2 · Status: accepted

## Context
Debezium turns every row change in `postgres-source` into a Kafka message. The wire format
decides how Spark (M2-S1) reads it, how schema changes travel, and how many services the
laptop runs. The usual production answers are Avro or Protobuf with a schema registry.

## Options
1. **Avro + Confluent Schema Registry** (or Apicurio): compact, schemas versioned and
   compatibility-checked centrally. Spark needs the registry-aware deserialiser (ABRiS, or
   stripping the 5-byte magic header by hand). One more service, one more JVM.
2. **JSON with embedded schemas** (`schemas.enable=true`): self-describing and no registry,
   but every message carries its full schema, often more bytes than the data.
3. **JSON without schemas** (`schemas.enable=false`): the Debezium envelope only
   (`before`, `after`, `source`, `op`, `ts_ms`); the schema lives in the consumer.

## Decision
Option 3. The consumer-side schema is not a gap here, because the contract already lives in
two committed places: the app's DDL (`docker/postgres-source/10-schema.sql`) and the quality
gate's expectations (M2-S2). Bronze stores the raw envelope as-is, so no event is lost to a
parse failure, and parsing happens once in a place that can quarantine. Wire details:

- keys are the primary key as JSON; values are the full envelope (REPLICA IDENTITY FULL, so
  `before` is populated on updates and deletes);
- `timestamp` columns arrive as epoch **microseconds** (Debezium adaptive mode), naive, and
  silver converts them back to local business time;
- `numeric` columns arrive as **strings** (`decimal.handling.mode=string`), which is exact
  (no binary float and no base64 `bytes` decimal);
- deletes are followed by a tombstone (null value) so a compacted topic could forget the key.

## Cost (honest)
- No central compatibility check: a producer-side schema change reaches consumers
  unannounced. Detection moves to the GX gate (unknown or missing fields → quarantine), which
  catches it after it happens rather than refusing it before.
- JSON is roughly 3–5× larger than Avro. At about 870k events this is megabytes, not a concern.
- Recruiters often expect "Schema Registry" on a CDC diagram. This ADR is the answer to
  "why not?".

## Revisit when
- A second producer team appears (a schema registry earns its keep with several producers);
- or event volume makes JSON's size matter;
- or M2-S1 finds Spark's JSON parsing of the envelope too costly. Then Avro + Apicurio, with
  `schemas.enable` kept false for keys.
