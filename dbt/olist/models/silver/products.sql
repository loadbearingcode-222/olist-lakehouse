-- Current state of olist.products, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['product_id']) }}
{{ cdc_current_state('products', ['product_id']) }}
