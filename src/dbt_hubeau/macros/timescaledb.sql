{% macro convert_to_hypertable(time_column, chunk_time_interval='1 year', migrate_data=true) %}
  {#-
    TimescaleDB: conversion en hypertable. À appeler APRÈS add_primary_key (la PK doit inclure time_column).
    chunk_time_interval: INTERVAL ('1 year', '1 month') ou entier pour colonne time integer (ex: 10 = 10 ans).
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

{% macro hypertable_delete(time_column, interval) %}
  {#- Pre-hook for hypertable incremental models using 'append' strategy.
      Replaces dbt's DELETE...USING (delete+insert strategy) which CANNOT do chunk exclusion
      on TimescaleDB hypertables — it Seq Scans ALL chunks including compressed ones.

      This macro does:
      1. Decompress only chunks overlapping with the delete window
      2. Simple DELETE (no JOIN/USING) which supports chunk exclusion natively

      Use with incremental_strategy='append' — dbt only does INSERT, we handle DELETE ourselves. -#}
  {% if is_incremental() %}
    {% set relation = this %}
    DO $$
    DECLARE
      chunk_to_decompress regclass;
    BEGIN
      FOR chunk_to_decompress IN
        SELECT format('%I.%I', chunk_schema, chunk_name)::regclass
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = '{{ relation.schema }}'
          AND hypertable_name = '{{ relation.identifier }}'
          AND is_compressed = true
          AND range_end >= CURRENT_DATE - INTERVAL '{{ interval }}'
      LOOP
        PERFORM decompress_chunk(chunk_to_decompress, if_compressed => true);
      END LOOP;
    END $$;
    DELETE FROM {{ relation }} WHERE "{{ time_column }}" >= CURRENT_DATE - INTERVAL '{{ interval }}';
  {% endif %}
{% endmacro %}

{% macro enable_compression(segment_by=[], order_by='time DESC', compress_after='365 days') %}
  {#- Idempotent compression setup: wraps ALTER TABLE in exception handler for re-runs. -#}
  {% set relation = this %}
  {% set segment_by_clause = segment_by | join(",") %}
  {% set enable_compression_query %}
    DO $$ BEGIN
      ALTER TABLE {{ relation }} SET (
        timescaledb.compress
        {% if segment_by_clause %}, timescaledb.compress_segmentby = '{{ segment_by_clause }}'{% endif %},
        timescaledb.compress_orderby = '{{ order_by }}'
      );
    EXCEPTION WHEN feature_not_supported OR duplicate_object THEN NULL;
    END $$;
  {% endset %}
  {% set add_policy_query %}
    SELECT public.add_compression_policy('{{ relation }}', INTERVAL '{{ compress_after }}', if_not_exists => TRUE);
  {% endset %}
  {{ return(enable_compression_query ~ "\n" ~ add_policy_query) }}
{% endmacro %}
