{#-
  Casts sûrs pour la couche Silver : gèrent NULL, chaîne vide et littéral 'NULL'.
  Utiliser pour toute colonne venant du bronze (souvent varchar) afin de forcer le bon type.
-#}

{% macro cast_silver_numeric(column_name) %}
NULLIF(NULLIF(TRIM({{ column_name }}::text), ''), 'NULL')::numeric
{% endmacro %}

{% macro cast_silver_int(column_name) %}
NULLIF(NULLIF(TRIM({{ column_name }}::text), ''), 'NULL')::numeric::integer
{% endmacro %}

{% macro cast_silver_date(column_name) %}
NULLIF(NULLIF(TRIM({{ column_name }}::text), ''), 'NULL')::date
{% endmacro %}

{% macro cast_silver_timestamp(column_name) %}
NULLIF(NULLIF(TRIM({{ column_name }}::text), ''), 'NULL')::timestamp
{% endmacro %}

{% macro cast_silver_text(column_name) %}
NULLIF(NULLIF(TRIM({{ column_name }}::text), ''), 'NULL')::text
{% endmacro %}
