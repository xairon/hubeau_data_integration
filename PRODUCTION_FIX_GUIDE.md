# Guide de Fix Production - Portainer + PostgreSQL

## Problèmes Identifiés

1. ❌ **Portainer toujours dans le stack brgm** - Redéployé à chaque push
2. ❌ **PostgreSQL authentication failed** - Mot de passe incorrect

## Solution : 2 Étapes

### Étape 1 : Migrer Portainer vers Standalone

**Connexion SSH** :
```bash
ssh root@srv991054.hstgr.cloud
```

**Arrêt du stack brgm** :
```bash
cd /srv/brgm
docker compose -f docker-compose.production.yml down
```

**Création du répertoire Portainer standalone** :
```bash
mkdir -p /srv/portainer
cd /srv/portainer
```

**Création du docker-compose.yml** :
```bash
cat > docker-compose.yml << 'EOF'
services:
  portainer:
    image: portainer/portainer-ce:2.19.4-alpine
    container_name: portainer
    restart: unless-stopped
    security_opt:
      - no-new-privileges:true
    ports:
      - "9443:9443"
      - "8000:8000"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - /srv/brgm-data/portainer:/data
    command: --http-disabled
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "https://localhost:9443", "--no-check-certificate"]
      interval: 30s
      timeout: 10s
      retries: 5
    mem_limit: 256m
    cpus: 0.1
EOF
```

**Démarrage Portainer standalone** :
```bash
docker compose up -d
sleep 10
docker ps | grep portainer
```

✅ **Portainer est maintenant standalone** : https://srv991054.hstgr.cloud:9443

---

### Étape 2 : Fixer PostgreSQL et Redéployer le Projet

**Définir les variables d'environnement** :

⚠️ **IMPORTANT** : Utilise les mêmes mots de passe que dans GitLab CI/CD Variables !

```bash
# Générer des mots de passe forts SI PAS ENCORE FAIT
# openssl rand -base64 32

# Exporter les variables (remplace par TES valeurs)
export DAGSTER_PG_HOST='dagster_postgres'
export DAGSTER_PG_PORT='5432'
export DAGSTER_PG_PASSWORD='TON_MOT_DE_PASSE_DAGSTER'

export PG_HOST='postgres'
export PG_PASSWORD='TON_MOT_DE_PASSE_POSTGRES'
export POSTGIS_HOST='postgis'

export MINIO_ENDPOINT='http://minio:9000'
export MINIO_USER='admin'
export MINIO_PASS='TON_MOT_DE_PASSE_MINIO'
export MINIO_REGION='us-east-1'
export MINIO_BRONZE_BUCKET='bronze'
```

**Supprimer le volume PostgreSQL Dagster** :

⚠️ Cela supprime l'historique des runs Dagster (métadonnées uniquement). Les données Hub'Eau ne sont PAS affectées.

```bash
cd /srv/brgm
rm -rf /srv/brgm-data/dagster_pg
```

**Redémarrer le projet** :
```bash
docker compose -f docker-compose.production.yml up -d
```

**Attendre que tout démarre (90 secondes)** :
```bash
sleep 90
docker compose -f docker-compose.production.yml ps
```

**Vérifier les logs** :
```bash
docker logs brgm-dagster-webserver --tail 50
```

✅ **Tu ne devrais PLUS voir "password authentication failed"**

---

## Vérification Finale

### 1. Vérifier Portainer (standalone)

```bash
docker ps | grep portainer
# Devrait afficher: portainer (pas brgm-portainer)
```

Ouvrir https://srv991054.hstgr.cloud:9443 - Portainer fonctionne ✅

### 2. Vérifier le projet brgm

```bash
cd /srv/brgm
docker compose -f docker-compose.production.yml ps
```

Devrait afficher :
```
NAME                      STATUS
brgm-dagster-daemon       Up (healthy)
brgm-dagster-postgres     Up (healthy)
brgm-dagster-webserver    Up (healthy)
brgm-dlt-worker           Up (healthy)
brgm-minio                Up (healthy)
brgm-minio-init           Exited (0)
```

**PAS de brgm-portainer** ✅

### 3. Vérifier Dagster UI

Ouvrir http://srv991054.hstgr.cloud:8080 - Interface Dagster accessible ✅

---

## Configuration GitLab CI/CD Variables

Pour que les futurs déploiements fonctionnent, configure dans **GitLab > Settings > CI/CD > Variables** :

```
DAGSTER_PG_HOST = dagster_postgres
DAGSTER_PG_PORT = 5432
DAGSTER_PG_PASSWORD = <même que ci-dessus> [Masked + Protected]

PG_HOST = postgres
PG_PASSWORD = <même que ci-dessus> [Masked + Protected]
POSTGIS_HOST = postgis

MINIO_ENDPOINT = http://minio:9000
MINIO_USER = admin
MINIO_PASS = <même que ci-dessus> [Masked + Protected]
MINIO_REGION = us-east-1
MINIO_BRONZE_BUCKET = bronze
```

---

## Après le Fix

Une fois que tout fonctionne :

1. **Portainer est standalone** → Ne sera plus redéployé avec le projet ✅
2. **PostgreSQL fonctionne** → Plus d'erreur d'authentification ✅
3. **Variables GitLab configurées** → Les prochains déploiements fonctionneront ✅

Tu peux maintenant faire `git push origin main` et le déploiement automatique devrait fonctionner sans erreurs.

---

## En Cas de Problème

### Portainer ne démarre pas
```bash
cd /srv/portainer
docker compose logs
```

### Dagster ne démarre pas
```bash
cd /srv/brgm
docker logs brgm-dagster-webserver
docker logs brgm-dagster-postgres
```

### Tout casser et recommencer
```bash
# Arrêter tout
cd /srv/brgm && docker compose -f docker-compose.production.yml down
cd /srv/portainer && docker compose down

# Supprimer les volumes problématiques
rm -rf /srv/brgm-data/dagster_pg

# Recommencer depuis Étape 1
```
