{#-
  Post-hooks pour déclarer clés primaires et étrangères (silver / gold).
  Utilisation: post_hook = ["{{ add_primary_key(['col1','col2']) }}", "{{ add_foreign_key(['code_bss'], 'stg_piezo_stations', ['code_bss']) }}"]
  Idempotent: au 2e run dbt renomme la table en __dbt_backup → on drop la contrainte sur la backup d'abord (libère l'index),
  puis on drop/add sur la table courante.
  CASCADE sur le DROP backup: TimescaleDB propage les FK/PK aux chunks internes (_hyper_*_chunk).
  Sans CASCADE, le DROP échoue car des dizaines de contraintes enfants dépendent de la PK/FK.
  Les FK sont recréées par les post_hook des tables enfants lors de leur prochain build.
-#}

{% macro add_primary_key(columns) %}
  {% set cols = columns if columns is iterable and columns is not string else [columns] %}
  {% set cols_sql = cols | join(', ') %}
  {% set relation = this %}
  {% set constraint_name = relation.identifier ~ '_pkey' %}
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{{ relation.schema }}' AND table_name = '{{ relation.identifier }}__dbt_backup') THEN
    EXECUTE 'ALTER TABLE "{{ relation.schema }}"."{{ relation.identifier }}__dbt_backup" DROP CONSTRAINT IF EXISTS "{{ constraint_name }}" CASCADE';
  END IF;
END $$;
ALTER TABLE {{ relation }} DROP CONSTRAINT IF EXISTS {{ constraint_name }} CASCADE;
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
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{{ relation.schema }}' AND table_name = '{{ relation.identifier }}__dbt_backup') THEN
    EXECUTE 'ALTER TABLE "{{ relation.schema }}"."{{ relation.identifier }}__dbt_backup" DROP CONSTRAINT IF EXISTS "{{ fk_name }}" CASCADE';
  END IF;
END $$;
ALTER TABLE {{ relation }} DROP CONSTRAINT IF EXISTS {{ fk_name }} CASCADE;
ALTER TABLE {{ relation }} ADD CONSTRAINT {{ fk_name }} FOREIGN KEY ({{ cols_sql }}) REFERENCES {{ ref_rel }} ({{ ref_cols_joined }});
{% endmacro %}
