-- One row per order line. The seller and product are joined AS OF the purchase: the version
-- valid at purchase_ts (SCD2), so a seller who moved later is reported where they were.
select
    i.order_id,
    cast(i.order_item_id as integer) as order_item_id,
    o.purchase_ts,
    cast(format_datetime(o.purchase_ts, 'yyyyMMdd') as integer) as purchase_date_key,
    c.customer_unique_id,
    s.seller_sk,
    i.seller_id,
    p.product_sk,
    i.product_id,
    o.order_status,
    cast(i.price as decimal(12, 2)) as price_brl,
    cast(i.freight_value as decimal(12, 2)) as freight_value_brl,
    cast(fx.rate as decimal(18, 10)) as fx_brl_usd,
    cast(round(i.price * fx.rate, 2) as decimal(12, 2)) as price_usd,
    cast(round(i.freight_value * fx.rate, 2) as decimal(12, 2)) as freight_value_usd,
    cardinality(o._dq_issues) = 0 as is_trusted
from {{ ref('order_items') }} as i
join {{ ref('orders') }} as o on o.order_id = i.order_id and not o._is_deleted
left join {{ ref('customers') }} as c on c.customer_id = o.customer_id and not c._is_deleted
left join {{ ref('dim_seller') }} as s
    on s.seller_id = i.seller_id and o.purchase_ts >= s.valid_from and o.purchase_ts < s.valid_to
left join {{ ref('dim_product') }} as p
    on p.product_id = i.product_id and o.purchase_ts >= p.valid_from and o.purchase_ts < p.valid_to
left join {{ ref('fx_brl_usd_daily') }} as fx on fx.day = cast(o.purchase_ts as date)
where not i._is_deleted
