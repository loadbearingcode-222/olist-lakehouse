{#
  The app tables as dbt sees them: (column, kind) per table, in DDL order.
  Mirrors docker/postgres-source/10-schema.sql; tests/unit/test_dbt_contract.py fails if the
  two drift apart.
#}
{% macro olist_columns(table) %}
  {%- set tables = {
    'product_category_translation': [
      ('category_name', 'varchar'), ('category_name_english', 'varchar')],
    'zip_geolocation': [
      ('zip_code_prefix', 'varchar'), ('lat', 'double'), ('lng', 'double'),
      ('city', 'varchar'), ('state', 'varchar'), ('samples', 'integer')],
    'sellers': [
      ('seller_id', 'varchar'), ('zip_code_prefix', 'varchar'), ('city', 'varchar'),
      ('state', 'varchar'), ('updated_at', 'micros')],
    'products': [
      ('product_id', 'varchar'), ('category_name', 'varchar'),
      ('product_name_length', 'integer'), ('product_description_length', 'integer'),
      ('product_photos_qty', 'integer'), ('product_weight_g', 'integer'),
      ('product_length_cm', 'integer'), ('product_height_cm', 'integer'),
      ('product_width_cm', 'integer'), ('updated_at', 'micros')],
    'customers': [
      ('customer_id', 'varchar'), ('customer_unique_id', 'varchar'),
      ('zip_code_prefix', 'varchar'), ('city', 'varchar'), ('state', 'varchar'),
      ('updated_at', 'micros')],
    'orders': [
      ('order_id', 'varchar'), ('customer_id', 'varchar'), ('order_status', 'varchar'),
      ('purchase_ts', 'micros'), ('approved_at', 'micros'),
      ('delivered_carrier_at', 'micros'), ('delivered_customer_at', 'micros'),
      ('estimated_delivery_at', 'micros'), ('updated_at', 'micros')],
    'order_items': [
      ('order_id', 'varchar'), ('order_item_id', 'integer'), ('product_id', 'varchar'),
      ('seller_id', 'varchar'), ('shipping_limit_at', 'micros'), ('price', 'decimal'),
      ('freight_value', 'decimal')],
    'order_payments': [
      ('order_id', 'varchar'), ('payment_sequential', 'integer'), ('payment_type', 'varchar'),
      ('payment_installments', 'integer'), ('payment_value', 'decimal')],
    'order_reviews': [
      ('review_id', 'varchar'), ('order_id', 'varchar'), ('review_score', 'integer'),
      ('review_comment_title', 'varchar'), ('review_comment_message', 'varchar'),
      ('review_creation_at', 'micros'), ('review_answer_at', 'micros')],
  } -%}
  {{ return(tables[table]) }}
{% endmacro %}
