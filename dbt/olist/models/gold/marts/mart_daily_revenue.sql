-- Revenue per purchase day, in BRL and USD, from trusted orders that were not cancelled or
-- unavailable. Untrusted (flagged) orders are counted separately, so nothing disappears.
select
    cast(o.purchase_ts as date) as purchase_date,
    cast(count_if(o.is_trusted and o.order_status not in ('canceled', 'unavailable')) as integer)
        as orders,
    cast(count_if(not o.is_trusted) as integer) as flagged_orders,
    cast(sum(case when o.is_trusted and o.order_status not in ('canceled', 'unavailable')
                  then o.payment_value_brl end) as decimal(16, 2)) as revenue_brl,
    cast(sum(case when o.is_trusted and o.order_status not in ('canceled', 'unavailable')
                  then o.payment_value_usd end) as decimal(16, 2)) as revenue_usd
from {{ ref('fct_orders') }} as o
group by 1
