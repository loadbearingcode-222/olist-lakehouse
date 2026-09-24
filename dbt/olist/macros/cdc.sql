{#
  CDC helpers: bronze Debezium envelopes → typed changes → current state per key.

  A column spec is (name, kind) with kind one of:
    varchar | integer | double | decimal (Debezium sends exact strings) | micros (epoch
    microseconds of a naive local timestamp, turned back into that same wall-clock time).
#}

{% macro _json_type(kind) -%}
  {%- if kind == 'micros' -%} bigint
  {%- elif kind == 'decimal' -%} varchar
  {%- else -%} {{ kind }}
  {%- endif -%}
{%- endmacro %}

{% macro _typed(expression, kind) -%}
  {%- if kind == 'micros' -%}
    cast(from_unixtime_nanos({{ expression }} * 1000) at time zone 'UTC' as timestamp(6))
  {%- elif kind == 'decimal' -%}
    cast({{ expression }} as decimal(12, 2))
  {%- else -%}
    {{ expression }}
  {%- endif -%}
{%- endmacro %}


{#
  One row per distinct change of `table`: Kafka redeliveries (same LSN, key and op at a new
  offset, e.g. after Connect is killed) collapse to their first landing. Deletes carry their
  key in `before`. Tombstones (null payload) are not changes and are dropped.
#}
{% macro cdc_changes(table) %}
  {%- set columns = olist_columns(table) %}
  {%- set row_type -%}
    row({% for name, kind in columns %}{{ name }} {{ _json_type(kind) }}{{ ", " if not loop.last }}{% endfor %})
  {%- endset %}
  with raw as (
      select
          op,
          source_lsn,
          source_ts_ms,
          record_key,
          kafka_offset,
          ingested_at,
          -- Choose by op, never coalesce: a delete's `after` is JSON null, which is a value, not
          -- SQL NULL, so coalesce(after, before) keyed every delete on nulls (M3-S1).
          cast(case when op = 'd' then json_extract(payload, '$.before')
                    else json_extract(payload, '$.after') end
               as {{ row_type }}) as r
      from {{ source('bronze', 'cdc_events') }}
      where source_table = '{{ table }}' and payload is not null
  ),
  distinct_changes as (
      select *, row_number() over (
          partition by source_lsn, record_key, op order by kafka_offset
      ) as landing
      from raw
  )
  select
      {% for name, kind in columns -%}
      {{ _typed('r.' ~ name, kind) }} as {{ name }},
      {% endfor -%}
      op = 'd' as _is_deleted,
      op as _op,
      source_lsn as _source_lsn,
      -- Iceberg stores microsecond timestamps only; from_unixtime() yields millisecond
      -- precision, which a CTAS lets through but an incremental temp table rejects.
      cast(from_unixtime(source_ts_ms / 1000.0) as timestamp(6) with time zone)
          as _source_committed_at,
      ingested_at as _ingested_at
  from distinct_changes
  where landing = 1
{% endmacro %}


{#
  Current state per key, merged incrementally.

  - Only bronze landed in the last `lookback_minutes` before the newest merged row is re-read.
  - Per key, the change with the highest LSN wins (LSN is the WAL position: later is newer).
  - On merge, a key is only updated when the incoming LSN is higher than the stored one, so a
    late redelivery of an older change can never roll a row back.
  - A delete keeps the row with `_is_deleted = true` (dbt-trino merges cannot delete).
#}
{#
  `checks` are (name, predicate) pairs judged on the row's CURRENT values. Their names land in
  `_dq_issues` (empty = clean). The bronze gate refuses the message that introduced a bad
  value, but with REPLICA IDENTITY FULL every later update of that row re-sends it whole, bad
  value included, so the value still reaches silver. Silver flags it; gold decides.
#}
{% macro cdc_current_state(table, keys, checks=[]) %}
  {%- set columns = olist_columns(table) %}
  with changes as (
      select * from {{ ref('stg_' ~ table) }}
      {% if is_incremental() %}
      where _ingested_at > (
          select coalesce(max(_ingested_at), timestamp '1970-01-01 00:00:00')
                 - interval '{{ var("lookback_minutes") }}' minute
          from {{ this }}
      )
      {% endif %}
  ),
  latest as (
      select *, row_number() over (
          partition by {{ keys | join(', ') }} order by _source_lsn desc
      ) as recency
      from changes
  )
  select
      {% for name, kind in columns -%}
      latest.{{ name }},
      {% endfor -%}
      {% if checks -%}
      filter(array[
          {%- for name, predicate in checks %}
          case when coalesce({{ predicate }}, false) then '{{ name }}' end{{ "," if not loop.last }}
          {%- endfor %}
      ], issue -> issue is not null) as _dq_issues,
      {%- else -%}
      cast(array[] as array(varchar)) as _dq_issues,
      {%- endif %}
      latest._is_deleted,
      latest._op,
      latest._source_lsn,
      latest._source_committed_at,
      latest._ingested_at
  from latest
  {% if is_incremental() %}
  left join {{ this }} as current_row
      on {% for key in keys %}current_row.{{ key }} = latest.{{ key }}{{ " and " if not loop.last }}{% endfor %}
  {% endif %}
  where latest.recency = 1
  {% if is_incremental() %}
    and (current_row.{{ keys[0] }} is null or latest._source_lsn > current_row._source_lsn)
  {% endif %}
{% endmacro %}
