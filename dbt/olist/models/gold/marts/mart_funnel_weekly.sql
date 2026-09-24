-- The web funnel per week of session start. Stages are cumulative: reaching a later stage
-- implies the earlier ones, because an earlier stage's EVENT can be missing even though the
-- customer passed it. Orders without items have nothing to add to a cart (1,713 converted
-- sessions), and a checkout event can be quarantined as schema drift (707).
with stages as (
    select
        started_at,
        converted,
        reached_checkout or converted as reached_checkout,
        add_to_carts > 0 or reached_checkout or converted as reached_cart
    from {{ ref('click_sessions') }}
    where started_at is not null
)
select
    cast(date_trunc('week', started_at) as date) as week,
    cast(count(*) as integer) as sessions,
    cast(count_if(reached_cart) as integer) as sessions_with_cart,
    cast(count_if(reached_checkout) as integer) as sessions_with_checkout,
    cast(count_if(converted) as integer) as converted_sessions,
    cast(count_if(converted) as double) / count(*) as conversion_rate
from stages
group by 1
