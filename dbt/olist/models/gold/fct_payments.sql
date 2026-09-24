-- One row per payment of an order (grain: order_id + payment_sequential), in BRL and USD at the
-- ECB rate of the purchase day. Every live payment is here: SUM(payment_value_brl) reconciles
-- with the source CSV to the cent.
select
    p.order_id,
    cast(p.payment_sequential as integer) as payment_sequential,
    p.payment_type,
    cast(p.payment_installments as integer) as payment_installments,
    cast(format_datetime(o.purchase_ts, 'yyyyMMdd') as integer) as purchase_date_key,
    cast(p.payment_value as decimal(12, 2)) as payment_value_brl,
    cast(fx.rate as decimal(18, 10)) as fx_brl_usd,
    cast(round(p.payment_value * fx.rate, 2) as decimal(12, 2)) as payment_value_usd
from {{ ref('order_payments') }} as p
join {{ ref('orders') }} as o on o.order_id = p.order_id
left join {{ ref('fx_brl_usd_daily') }} as fx on fx.day = cast(o.purchase_ts as date)
where not p._is_deleted
