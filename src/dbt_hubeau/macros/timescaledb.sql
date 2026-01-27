{% macro convert_to_hypertable(time_column, chunk_time_interval='1 year', migrate_data=true) %}
  {% if execute %}
    {% set relation = this %}
    -- 1. Convert to Hypertable
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

    {% do run_query(create_hypertable_query) %}
    {{ log("Converted " ~ relation ~ " to hypertable on column " ~ time_column, info=True) }}
    
    -- 2. Drop default index if Redundant (TimescaleDB creates an index on time_column)
    -- This is optional but good practice to keep indexes clean
  {% endif %}
{% endmacro %}

{% macro enable_compression(segment_by=[], order_by='time DESC', compress_after='365 days') %}
  {% if execute %}
    {% set relation = this %}
    
    -- 1. Enable Compression
    {% set enable_compression_query %}
      ALTER TABLE {{ relation }} SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = '{{ segment_by | join(",") }}',
        timescaledb.compress_orderby = '{{ order_by }}'
      );
    {% endset %}
    
    {% do run_query(enable_compression_query) %}
    {{ log("Enabled compression on " ~ relation, info=True) }}

    -- 2. Add Compression Policy
    {% set add_policy_query %}
      SELECT public.add_compression_policy('{{ relation }}', INTERVAL '{{ compress_after }}', if_not_exists => TRUE);
    {% endset %}
    
    {% do run_query(add_policy_query) %}
    {{ log("Added compression policy on " ~ relation ~ " after " ~ compress_after, info=True) }}

  {% endif %}
{% endmacro %}
