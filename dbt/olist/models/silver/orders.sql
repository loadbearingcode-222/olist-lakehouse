-- Current state of olist.orders, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['order_id']) }}
{{ cdc_current_state('orders', ['order_id'], checks=[
    ('null_customer_id', 'latest.customer_id is null'),
    ('approved_before_purchase', 'latest.approved_at < latest.purchase_ts'),
]) }}
