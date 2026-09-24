-- Current state of olist.product_category_translation, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['category_name']) }}
{{ cdc_current_state('product_category_translation', ['category_name']) }}
