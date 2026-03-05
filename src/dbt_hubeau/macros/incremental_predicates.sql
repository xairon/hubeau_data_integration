{% macro time_range_delete_predicate(time_column, interval) %}
"{{ this.identifier }}"."{{ time_column }}" >= CURRENT_DATE - INTERVAL '{{ interval }}'
{% endmacro %}