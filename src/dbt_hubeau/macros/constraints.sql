{#-
  Post-hooks pour déclarer clés primaires et étrangères (silver / gold).
  Utilisation: post_hook = ["{{ add_primary_key(['col1','col2']) }}", "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"]
  Gère les runs répétés (contrainte déjà existante ignorée).
-#}

{% macro add_primary_key(columns) %}
  {% set cols = columns if columns is iterable and columns is not string else [columns] %}
  {% set cols_sql = cols | join(', ') %}
  {% set relation = this %}
  {% set constraint_name = relation.identifier ~ '_pkey' %}
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = '{{ relation.schema }}' AND t.relname = '{{ relation.identifier }}' AND c.conname = '{{ constraint_name }}'
  ) THEN
    ALTER TABLE {{ relation }} ADD CONSTRAINT {{ constraint_name }} PRIMARY KEY ({{ cols_sql }});
  END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
{% endmacro %}

{% macro add_foreign_key(columns, ref_model_name, ref_columns=none) %}
  {% set ref_rel = ref(ref_model_name) %}
  {% set ref_cols = ref_columns if ref_columns is not none else columns %}
  {% set ref_cols_sql = ref_cols if ref_cols is iterable and ref_cols is not string else [ref_cols] %}
  {% set cols = columns if columns is iterable and columns is not string else [columns] %}
  {% set cols_sql = cols | join(', ') %}
  {% set ref_cols_joined = ref_cols_sql | join(', ') %}
  {% set relation = this %}
  {% set fk_name = relation.identifier ~ '_' ~ ref_rel.identifier ~ '_fkey' %}
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    WHERE n.nspname = '{{ relation.schema }}' AND t.relname = '{{ relation.identifier }}' AND c.conname = '{{ fk_name }}'
  ) THEN
    ALTER TABLE {{ relation }} ADD CONSTRAINT {{ fk_name }} FOREIGN KEY ({{ cols_sql }}) REFERENCES {{ ref_rel }} ({{ ref_cols_joined }});
  END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
{% endmacro %}
