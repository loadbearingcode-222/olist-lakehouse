-- Clickstream events from bronze, parsed. Deduplication by event_id happens in
-- silver.click_events, on the incremental window only: ranking all 1.2M events here on every
-- run exceeded Trino's per-node memory (M3-S1).
select
    event_id,
    event_type,                     -- bronze already coalesced event_type / eventType
    event_ts,
    json_extract_scalar(payload, '$.session_id') as session_id,
    json_extract_scalar(payload, '$.customer_unique_id') as customer_unique_id,
    json_extract_scalar(payload, '$.device') as device,
    json_extract_scalar(payload, '$.user_agent') as user_agent,
    json_extract_scalar(payload, '$.page') as page,
    json_extract_scalar(payload, '$.product_id') as product_id,
    json_extract_scalar(payload, '$.order_id') as order_id,
    schema_version,
    kafka_partition as _kafka_partition,
    kafka_offset as _kafka_offset,
    kafka_timestamp as _kafka_timestamp,
    ingested_at as _ingested_at
from {{ source('bronze', 'clickstream_events') }}
where event_id is not null
