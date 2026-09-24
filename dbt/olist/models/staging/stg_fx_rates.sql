-- BRL->USD per ECB business day, as loaded by olist_lakehouse.fx.
select rate_date, rate
from {{ source('bronze', 'fx_rates') }}
where base = 'BRL' and quote = 'USD'
