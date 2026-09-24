-- Current state of olist.sellers, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['seller_id']) }}
{{ cdc_current_state('sellers', ['seller_id']) }}
