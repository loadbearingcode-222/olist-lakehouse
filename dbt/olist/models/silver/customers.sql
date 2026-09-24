-- Current state of olist.customers, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['customer_id']) }}
{{ cdc_current_state('customers', ['customer_id']) }}
