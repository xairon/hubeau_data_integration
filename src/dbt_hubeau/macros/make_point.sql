{#-
  PostGIS: point WGS84 (SRID 4326) typé pour index GIST et requêtes spatiales.
  Pour les distances en mètres, utiliser ::geography (ex: ST_Distance(geom::geography, ...)).
  L'opérateur <-> utilise l'index GIST (KNN).
-#}
{% macro make_point(longitude, latitude, srid=4326) %}
ST_SetSRID(ST_MakePoint({{ longitude }}, {{ latitude }}), {{ srid }})::geometry(Point, {{ srid }})
{% endmacro %}
