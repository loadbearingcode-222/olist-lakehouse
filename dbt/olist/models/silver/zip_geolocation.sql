-- Current state of olist.zip_geolocation, one row per key; deletes kept as _is_deleted.
{{ config(unique_key=['zip_code_prefix']) }}
{{ cdc_current_state('zip_geolocation', ['zip_code_prefix']) }}
