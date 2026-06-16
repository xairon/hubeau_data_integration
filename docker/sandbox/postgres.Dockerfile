# Image Postgres SANDBOX : TimescaleDB-HA + script d'init intégré.
# Permet un déploiement Portainer SANS bind mount (init.sql forbidden en mount).
# Le script d'init (extensions timescaledb/postgis + schémas) est copié dans
# /docker-entrypoint-initdb.d, exécuté une seule fois à la création du volume.
FROM timescale/timescaledb-ha:pg16

COPY docker/postgres/init.sql /docker-entrypoint-initdb.d/01-init.sql
