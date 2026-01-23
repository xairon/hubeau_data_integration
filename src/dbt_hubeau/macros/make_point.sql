{% macro make_point(longitude, latitude, srid=4326) %}
ST_SetSRID(ST_MakePoint({{ longitude }}, {{ latitude }}), {{ srid }})::geometry(Point, {{ srid }})
{% endmacro %}
