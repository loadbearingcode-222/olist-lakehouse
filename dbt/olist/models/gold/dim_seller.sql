-- Sellers with history (SCD type 2): one row per VERSION, built from every change in the CDC log (ADR-0006).
-- valid_from is the business time of the change (the app's updated_at); the current version
-- has valid_to = 9999-12-31. Facts join the version valid at their purchase time.
with changes as (
    select * from {{ ref('stg_sellers') }} where not _is_deleted
),
versions as (
    select
        *,
        row_number() over (partition by seller_id order by _source_lsn) as version,
        lead(updated_at) over (partition by seller_id order by _source_lsn) as next_valid_from
    from changes
)
select
    to_hex(md5(to_utf8(seller_id || '|' || cast(version as varchar)))) as seller_sk,
    versions.seller_id,
    cast(version as integer) as version,
    versions.zip_code_prefix,
    versions.city,
    versions.state,
    geo.lat,
    geo.lng,
    versions.updated_at as valid_from,
    coalesce(next_valid_from, timestamp '9999-12-31 00:00:00.000000') as valid_to,
    next_valid_from is null as is_current
from versions
left join {{ ref('zip_geolocation') }} as geo
    on geo.zip_code_prefix = versions.zip_code_prefix and not geo._is_deleted
