-- Current state of olist.order_reviews, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['review_id', 'order_id']) }}
{{ cdc_current_state('order_reviews', ['review_id', 'order_id']) }}
