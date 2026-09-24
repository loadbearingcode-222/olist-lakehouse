-- One row per order: lifecycle, delivery performance, value, and review, in BRL and USD.
-- `is_trusted` is false when silver flagged the order (_dq_issues, ADR-0005): the row stays,
-- so totals reconcile, and metrics choose whether to include it.
-- Sums are cast back to decimal(14,2) BEFORE any multiplication: sum() widens to
-- decimal(38,2), and decimal(38,2) x decimal(18,10) no longer fits 38 digits, so the engine
-- silently cuts the product's scale, and rounding it again then lands a cent off
-- (gotcha #34; found by tests/integration/test_gold.py).
with items as (
    select
        order_id,
        count(*) as items,
        cast(sum(price) as decimal(14, 2)) as items_value,
        cast(sum(freight_value) as decimal(14, 2)) as freight
    from {{ ref('order_items') }} where not _is_deleted group by 1
),
payments as (
    select order_id, cast(sum(payment_value) as decimal(14, 2)) as payment_value
    from {{ ref('order_payments') }} where not _is_deleted group by 1
),
reviews as (
    select order_id, avg(review_score) as review_score
    from {{ ref('order_reviews') }} where not _is_deleted group by 1
)
select
    o.order_id,
    c.customer_unique_id,
    o.order_status,
    o.purchase_ts,
    cast(format_datetime(o.purchase_ts, 'yyyyMMdd') as integer) as purchase_date_key,
    o.approved_at,
    o.delivered_carrier_at,
    o.delivered_customer_at,
    o.estimated_delivery_at,
    cast(date_diff('second', o.purchase_ts, o.delivered_customer_at) / 86400.0 as double)
        as delivery_days,
    o.delivered_customer_at > o.estimated_delivery_at as is_delivered_late,
    cast(coalesce(items.items, 0) as integer) as items,
    cast(coalesce(items.items_value, 0) as decimal(14, 2)) as items_value_brl,
    cast(coalesce(items.freight, 0) as decimal(14, 2)) as freight_value_brl,
    cast(coalesce(payments.payment_value, 0) as decimal(14, 2)) as payment_value_brl,
    cast(fx.rate as decimal(18, 10)) as fx_brl_usd,
    cast(round(coalesce(payments.payment_value, cast(0 as decimal(14, 2))) * fx.rate, 2)
        as decimal(14, 2))
        as payment_value_usd,
    cast(reviews.review_score as double) as review_score,
    o._dq_issues as dq_issues,
    cardinality(o._dq_issues) = 0 as is_trusted
from {{ ref('orders') }} as o
left join {{ ref('customers') }} as c on c.customer_id = o.customer_id and not c._is_deleted
left join items on items.order_id = o.order_id
left join payments on payments.order_id = o.order_id
left join reviews on reviews.order_id = o.order_id
left join {{ ref('fx_brl_usd_daily') }} as fx on fx.day = cast(o.purchase_ts as date)
where not o._is_deleted
