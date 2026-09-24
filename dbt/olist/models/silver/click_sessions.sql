-- One row per web session. An event more than `late_after_hours` before its session's latest
-- event is late (it arrived with an event time far in the past): it is counted, but it does
-- not move the session's start. Rebuilt in full on each run (about 1.2M events).
{{ config(materialized='table') }}
with events as (
    select *, max(event_ts) over (partition by session_id) as session_latest
    from {{ ref('click_events') }}
),
flagged as (
    select *,
        event_ts < session_latest - interval '{{ var("late_after_hours") }}' hour as is_late
    from events
)
select
    session_id,
    max(customer_unique_id) as customer_unique_id,
    max(device) as device,
    min(case when not is_late then event_ts end) as started_at,
    max(event_ts) as ended_at,
    count(*) as events,
    count_if(event_type = 'page_view') as page_views,
    count_if(event_type = 'add_to_cart') as add_to_carts,
    bool_or(event_type = 'checkout') as reached_checkout,
    bool_or(event_type = 'purchase') as converted,
    max(order_id) as order_id,
    count_if(is_late) as late_events
from flagged
group by session_id
