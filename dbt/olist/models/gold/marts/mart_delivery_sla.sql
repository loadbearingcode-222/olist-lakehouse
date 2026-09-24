-- Delivery performance per purchase month and customer state, for delivered trusted orders.
select
    cast(date_trunc('month', o.purchase_ts) as date) as purchase_month,
    coalesce(c.state, 'unknown') as customer_state,
    cast(count(*) as integer) as delivered_orders,
    cast(avg(o.delivery_days) as double) as avg_delivery_days,
    cast(avg(case when o.is_delivered_late then 1.0 else 0.0 end) as double) as late_share
from {{ ref('fct_orders') }} as o
left join {{ ref('dim_customer') }} as c on c.customer_unique_id = o.customer_unique_id
where o.is_trusted and o.delivered_customer_at is not null
group by 1, 2
