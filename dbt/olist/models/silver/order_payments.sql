-- Current state of olist.order_payments, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['order_id', 'payment_sequential']) }}
{{ cdc_current_state('order_payments', ['order_id', 'payment_sequential']) }}
