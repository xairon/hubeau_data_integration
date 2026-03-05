{#-
  Post-hooks pour déclarer clés primaires et étrangères (silver / gold).
  Utilisation: post_hook = ["{{ add_primary_key(['col1','col2']) }}", "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"]

  IDEMPOTENT: tente ADD CONSTRAINT, attrape duplicate_object si la contrainte existe déjà → no-op instantané.
  Sur un run incrémental, la PK/FK existe déjà → skip (0ms au lieu de 460s+ de DROP+ADD sur hypertable 23M rows).
  Sur full-refresh ou table materialization: dbt renomme l'ancienne table en __dbt_backup → on drop la contrainte
  sur la backup d'abord (libère l'index), puis ADD sur la nouvelle table.
  CASCADE sur le DROP backup: TimescaleDB propage les FK/PK aux chunks internes (_hyper_*_chunk).
-#}

{% macro add_primary_key(columns) %}
  {% set cols = columns if columns is iterable and columns is not string else [columns] %}
  {% set cols_quoted = [] %}
  {% for col in cols %}{% do cols_quoted.append('"' ~ col ~ '"') %}{% endfor %}
  {% set cols_sql = cols_quoted | join(', ') %}
  {% set relation = this %}
  {% set constraint_name = relation.identifier ~ '_pkey' %}
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{{ relation.schema }}' AND table_name = '{{ relation.identifier }}__dbt_backup') THEN
    EXECUTE 'ALTER TABLE "{{ relation.schema }}"."{{ relation.identifier }}__dbt_backup" DROP CONSTRAINT IF EXISTS "{{ constraint_name }}" CASCADE';
  END IF;
END $$;
DO $$ BEGIN
  ALTER TABLE {{ relation }} ADD CONSTRAINT "{{ constraint_name }}" PRIMARY KEY ({{ cols_sql }});
EXCEPTION WHEN duplicate_object OR invalid_table_definition THEN NULL;
END $$;
{% endmacro %}

{% macro add_foreign_key(columns, ref_model_name, ref_columns=none) %}
  {% set ref_rel = ref(ref_model_name) %}
  {% set ref_cols = ref_columns if ref_columns is not none else columns %}
  {% set ref_cols_list = ref_cols if ref_cols is iterable and ref_cols is not string else [ref_cols] %}
  {% set cols = columns if columns is iterable and columns is not string else [columns] %}
  {% set cols_quoted = [] %}
  {% for col in cols %}{% do cols_quoted.append('"' ~ col ~ '"') %}{% endfor %}
  {% set ref_cols_quoted = [] %}
  {% for col in ref_cols_list %}{% do ref_cols_quoted.append('"' ~ col ~ '"') %}{% endfor %}
  {% set cols_sql = cols_quoted | join(', ') %}
  {% set ref_cols_joined = ref_cols_quoted | join(', ') %}
  {% set relation = this %}
  {% set fk_name = relation.identifier ~ '_' ~ ref_rel.identifier ~ '_fkey' %}
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{{ relation.schema }}' AND table_name = '{{ relation.identifier }}__dbt_backup') THEN
    EXECUTE 'ALTER TABLE "{{ relation.schema }}"."{{ relation.identifier }}__dbt_backup" DROP CONSTRAINT IF EXISTS "{{ fk_name }}" CASCADE';
  END IF;
END $$;
DO $$ BEGIN
  ALTER TABLE {{ relation }} ADD CONSTRAINT "{{ fk_name }}" FOREIGN KEY ({{ cols_sql }}) REFERENCES {{ ref_rel }} ({{ ref_cols_joined }});
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
{% endmacro %}
