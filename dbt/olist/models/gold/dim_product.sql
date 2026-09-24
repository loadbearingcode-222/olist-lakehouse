-- Products with history (SCD type 2): one row per VERSION, built from every change in the CDC log (ADR-0006).
-- valid_from is the business time of the change (the app's updated_at); the current version
-- has valid_to = 9999-12-31. Facts join the version valid at their purchase time.
with changes as (
    select * from {{ ref('stg_products') }} where not _is_deleted
),
versions as (
    select
        *,
        row_number() over (partition by product_id order by _source_lsn) as version,
        lead(updated_at) over (partition by product_id order by _source_lsn) as next_valid_from
    from changes
)
select
    to_hex(md5(to_utf8(product_id || '|' || cast(version as varchar)))) as product_sk,
    versions.product_id,
    cast(version as integer) as version,
    versions.category_name,
    coalesce(translation.category_name_english, versions.category_name, 'unknown')
        as category_name_english,
    versions.product_weight_g,
    versions.product_length_cm,
    versions.product_height_cm,
    versions.product_width_cm,
    versions.product_photos_qty,
    versions.updated_at as valid_from,
    coalesce(next_valid_from, timestamp '9999-12-31 00:00:00.000000') as valid_to,
    next_valid_from is null as is_current
from versions
left join {{ ref('product_category_translation') }} as translation
    on translation.category_name = versions.category_name and not translation._is_deleted
