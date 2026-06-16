# Image Superset SANDBOX : Superset + config/datasources/init intégrés.
# Permet un déploiement Portainer SANS bind mount.
# Les 3 fichiers (config, script d'init créant l'admin + import datasources)
# sont copiés dans /app et utilisés par l'entrypoint défini dans le compose.
FROM apache/superset:4.0.0

COPY docker/superset/superset_config.py /app/superset_config.py
COPY docker/superset/docker-init.sh /app/docker-init.sh
COPY docker/superset/datasources.yaml /app/datasources.yaml
