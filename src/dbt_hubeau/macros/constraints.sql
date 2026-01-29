{#-
  Post-hooks pour déclarer clés primaires et étrangères (silver / gold).
  Utilisation: post_hook = ["{{ add_primary_key(['col1','col2']) }}", "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"]
  Idempotent: DROP IF EXISTS puis ADD pour éviter "relation already exists" sur runs répétés.
-#}

{% macro add_primary_key(columns) %}
  {% set cols = columns if columns is iterable and columns is not string else [columns] %}
  {% set cols_sql = cols | join(', ') %}
  {% set relation = this %}
  {% set constraint_name = relation.identifier ~ '_pkey' %}
ALTER TABLE {{ relation }} DROP CONSTRAINT IF EXISTS {{ constraint_name }};
ALTER TABLE {{ relation }} ADD CONSTRAINT {{ constraint_name }} PRIMARY KEY ({{ cols_sql }});
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
ALTER TABLE {{ relation }} DROP CONSTRAINT IF EXISTS {{ fk_name }};
ALTER TABLE {{ relation }} ADD CONSTRAINT {{ fk_name }} FOREIGN KEY ({{ cols_sql }}) REFERENCES {{ ref_rel }} ({{ ref_cols_joined }});
{% endmacro %}
