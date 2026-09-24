-- Every seller and product has exactly one current version, and its versions tile time with
-- no gap and no overlap (each valid_to is the next version's valid_from).
with versions as (
    select 'seller' as entity, seller_id as id, version, valid_from, valid_to, is_current
    from {{ ref('dim_seller') }}
    union all
    select 'product', product_id, version, valid_from, valid_to, is_current
    from {{ ref('dim_product') }}
),
checked as (
    select *,
        lead(valid_from) over (partition by entity, id order by version) as next_from,
        count_if(is_current) over (partition by entity, id) as current_versions
    from versions
)
select * from checked
where current_versions <> 1
   or (next_from is not null and valid_to <> next_from)
   or (next_from is null and not is_current)
