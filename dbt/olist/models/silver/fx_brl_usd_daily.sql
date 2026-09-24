-- One BRL->USD rate for EVERY calendar day: weekends and ECB holidays carry the last
-- published rate forward (the ECB publishes on TARGET business days only).
{{ config(materialized='table') }}
with bounds as (
    select min(rate_date) as first_day, max(rate_date) as last_day from {{ ref('stg_fx_rates') }}
),
days as (
    select day
    from bounds
    cross join unnest(sequence(first_day, last_day, interval '1' day)) as t(day)
)
select
    days.day,
    last_value(rates.rate) ignore nulls over (order by days.day) as rate,
    last_value(rates.rate_date) ignore nulls over (order by days.day) as published_on
from days
left join {{ ref('stg_fx_rates') }} as rates on rates.rate_date = days.day
