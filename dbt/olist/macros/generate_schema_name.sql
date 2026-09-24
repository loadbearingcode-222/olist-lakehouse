{#
  Use a model's custom schema as-is (`silver`, `gold`), not dbt's default `<target>_<custom>`.
  The lake's namespaces are the medallion layers themselves.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
