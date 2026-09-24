-- One row per person (customer_unique_id). Olist issues a new customer_id per order, and the
-- app writes the customer row at purchase time (updated_at), so a person's latest row is
-- their latest location. Built from customers alone: an order whose customer_id was lost
-- (a flagged order, ADR-0005) must not make its customer vanish from the dimension.
with ranked as (
    select *,
        row_number() over (
            partition by customer_unique_id order by updated_at desc, customer_id
        ) as recency,
        count(*) over (partition by customer_unique_id) as orders,
        min(updated_at) over (partition by customer_unique_id) as first_purchase_ts
    from {{ ref('customers') }}
    where not _is_deleted
)
select
    ranked.customer_unique_id,
    ranked.zip_code_prefix,
    ranked.city,
    ranked.state,
    geo.lat,
    geo.lng,
    ranked.first_purchase_ts,
    cast(ranked.orders as integer) as orders
from ranked
left join {{ ref('zip_geolocation') }} as geo
    on geo.zip_code_prefix = ranked.zip_code_prefix and not geo._is_deleted
where ranked.recency = 1
