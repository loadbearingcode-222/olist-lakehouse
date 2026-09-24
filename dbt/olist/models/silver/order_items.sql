-- Current state of olist.order_items, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['order_id', 'order_item_id']) }}
{{ cdc_current_state('order_items', ['order_id', 'order_item_id'], checks=[
    ('negative_price', 'latest.price < 0'),
]) }}
