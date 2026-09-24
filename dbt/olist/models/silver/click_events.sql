-- Every clickstream event once, in event time. The `duplicate` chaos kind and producer re-sends
-- repeat an event byte for byte; the first landing is kept and later copies are skipped, both
-- within this run's window and against what silver already holds.
{{ config(unique_key='event_id') }}
with fresh as (
    select * from {{ ref('stg_clickstream_events') }}
    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), timestamp '1970-01-01 00:00:00')
               - interval '{{ var("lookback_minutes") }}' minute
        from {{ this }}
    )
    {% endif %}
),
first_landing as (
    select *, row_number() over (
        partition by event_id order by _kafka_timestamp, _kafka_partition, _kafka_offset
    ) as landing
    from fresh
)
select
    f.event_id, f.event_type, f.event_ts, f.session_id, f.customer_unique_id, f.device,
    f.user_agent, f.page, f.product_id, f.order_id, f.schema_version, f._kafka_timestamp,
    f._ingested_at
from first_landing as f
{% if is_incremental() %}
left join {{ this }} as seen on seen.event_id = f.event_id
{% endif %}
where f.landing = 1
{% if is_incremental() %}
  and seen.event_id is null
{% endif %}
