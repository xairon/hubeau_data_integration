{% macro convert_to_hypertable(time_column, chunk_time_interval='1 year', migrate_data=true) %}
  {#-
    Intended for use in dbt hooks (post_hook/pre_hook).
    IMPORTANT: hooks must return executable SQL; do not call run_query() here,
    otherwise the hook becomes an empty string and Postgres errors with
    "can't execute an empty query".
  -#}
  {% set relation = this %}
  {% set create_hypertable_query %}
    SELECT public.create_hypertable(
      '{{ relation }}',
      '{{ time_column }}',
      {% if chunk_time_interval is string and ' ' in chunk_time_interval %}
        chunk_time_interval => INTERVAL '{{ chunk_time_interval }}',
      {% else %}
        chunk_time_interval => {{ chunk_time_interval }},
      {% endif %}
      migrate_data => {{ migrate_data }},
      if_not_exists => TRUE
    );
  {% endset %}
  {{ return(create_hypertable_query) }}
{% endmacro %}

{% macro enable_compression(segment_by=[], order_by='time DESC', compress_after='365 days') %}
  {#- Intended for use in hooks; return executable SQL -#}
  {% set relation = this %}
  {% set segment_by_clause = segment_by | join(",") %}
  {% set enable_compression_query %}
    ALTER TABLE {{ relation }} SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = '{{ segment_by_clause }}',
      timescaledb.compress_orderby = '{{ order_by }}'
    );
  {% endset %}
  {% set add_policy_query %}
    SELECT public.add_compression_policy('{{ relation }}', INTERVAL '{{ compress_after }}', if_not_exists => TRUE);
  {% endset %}
  {{ return(enable_compression_query ~ "\n" ~ add_policy_query) }}
{% endmacro %}
