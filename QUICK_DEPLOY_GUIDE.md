# 🚀 Guide de Déploiement Rapide

Guide pour déployer le projet sur le serveur Hostinger avec Portainer pour le monitoring.

## 📋 Prérequis

- ✅ Accès SSH au serveur: `ssh root@srv991054.hstgr.cloud`
- ✅ GitLab Runner configuré avec tag `hubeau`
- ✅ Variables GitLab CI/CD définies (voir [GITLAB_CI_VARIABLES_SETUP.md](docs/GITLAB_CI_VARIABLES_SETUP.md))

## 🎯 Déploiement en 3 Étapes

### Étape 1: Installer Portainer (une seule fois)

```bash
# SSH sur le serveur
ssh root@srv991054.hstgr.cloud

# Aller dans le répertoire du projet
cd /srv/brgm

# Lancer le script d'installation
bash scripts/setup-portainer.sh
```

**Résultat attendu:**
```
✅ Portainer installé avec succès !
📍 Accès: https://srv991054.hstgr.cloud:9443
```

**Configuration initiale** (dans les 5 minutes):
1. Ouvrir https://srv991054.hstgr.cloud:9443
2. Accepter le certificat auto-signé
3. Créer compte admin (username: `admin`, password: **12+ caractères**)
4. Cliquer "Get Started"

---

### Étape 2: Push le Code et Déclencher le Pipeline

```bash
# Sur votre machine locale
git push origin main
```

**Le pipeline GitLab va automatiquement:**
1. ✅ Utiliser Alpine 3.19 (évite Docker Hub 503)
2. ✅ Synchroniser les fichiers vers `/srv/brgm`
3. ✅ Builder les images `hubeau-orchestrator` et `hubeau-worker`
4. ✅ Déployer avec `docker compose`
5. ✅ Vérifier le health check des conteneurs

**Durée:** ~3-5 minutes

---

### Étape 3: Vérifier le Déploiement avec Portainer

1. **Ouvrir Portainer**: https://srv991054.hstgr.cloud:9443

2. **Aller dans Containers**:
   - Vous devriez voir 5 conteneurs en "running":
     - ✅ `brgm-dagster-postgres`
     - ✅ `brgm-dagster-webserver`
     - ✅ `brgm-dagster-daemon`
     - ✅ `brgm-dlt-worker`
     - ✅ `brgm-adminer`

3. **Vérifier les logs du worker** (si unhealthy):
   - Cliquer sur `brgm-dlt-worker`
   - Onglet "Logs"
   - Activer "Auto-refresh logs"
   - Chercher les erreurs (connexion DB, variables manquantes, etc.)

4. **Accéder aux services**:
   - Dagster UI: http://srv991054.hstgr.cloud:8080
   - Adminer: http://srv991054.hstgr.cloud:8081

---

## 🐛 Problèmes Courants

### Problème 1: Worker Unhealthy

**Symptôme:**
```
dependency failed to start: container brgm-dlt-worker is unhealthy
```

**Solution dans Portainer:**
1. Containers → `brgm-dlt-worker` → Logs
2. Chercher l'erreur exacte
3. Vérifier les variables d'environnement (onglet "Inspect" → "Env")
4. Restart le conteneur si config OK

**Solution en CLI:**
```bash
ssh root@srv991054.hstgr.cloud
cd /srv/brgm
bash scripts/debug-worker-health.sh
```

### Problème 2: Pipeline Échoue au Build

**Symptôme:**
```
ERROR: BuildKit is enabled but buildx component is missing
```

**Cause:** Docker Hub est down ou BuildKit mal configuré

**Solution:** Déjà corrigée dans `.gitlab-ci.yml` :
- `DOCKER_BUILDKIT: 0` (legacy builder)
- Utilisation de `alpine:3.19` au lieu de `docker:24-cli`

### Problème 3: Images Pas Rebuildées

**Symptôme:** Le code est mis à jour mais l'application utilise l'ancien code

**Solution:**
```bash
ssh root@srv991054.hstgr.cloud
cd /srv/brgm

# Forcer rebuild des images
docker compose -f docker-compose.production.yml build --no-cache
docker compose -f docker-compose.production.yml up -d
```

---

## 📊 Commandes Utiles

### Voir les Logs en Temps Réel

**Dans Portainer:**
- Containers → Sélectionner conteneur → Logs → Auto-refresh

**En CLI:**
```bash
# Logs du worker
docker logs brgm-dlt-worker -f

# Logs de tous les services
docker compose -f /srv/brgm/docker-compose.production.yml logs -f
```

### Redémarrer un Service

**Dans Portainer:**
- Containers → Sélectionner conteneur → Restart

**En CLI:**
```bash
# Redémarrer le worker uniquement
docker restart brgm-dlt-worker

# Redémarrer tous les services
docker compose -f /srv/brgm/docker-compose.production.yml restart
```

### Vérifier le Status

```bash
# Status de tous les conteneurs
docker ps

# Health check du worker
docker inspect brgm-dlt-worker --format='{{.State.Health.Status}}'
```

### Rebuild et Redéployer

```bash
cd /srv/brgm

# Rebuild les images
docker build -f docker/orchestrator/Dockerfile -t hubeau-orchestrator:latest .
docker build -f docker/worker/Dockerfile -t hubeau-worker:latest .

# Redéployer
docker compose -f docker-compose.production.yml up -d
```

---

## 🔐 Variables d'Environnement Importantes

Vérifiées automatiquement par le pipeline:

| Variable | Description | Masqué | Protégé |
|----------|-------------|--------|---------|
| `DAGSTER_PG_PASSWORD` | Password PostgreSQL Dagster | ✅ | ✅ |
| `PG_PASSWORD` | Password PostgreSQL Hub'Eau | ✅ | ✅ |
| `MINIO_PASS` | Password MinIO | ✅ | ✅ |
| `FORCE_POSTGRES_RESET` | Reset PostgreSQL (optional) | ❌ | ❌ |

**Définies dans:** Settings → CI/CD → Variables

---

## 🎯 Checklist Post-Déploiement

- [ ] Portainer installé et accessible (https://srv991054.hstgr.cloud:9443)
- [ ] Pipeline GitLab passé (green checkmark)
- [ ] Tous les conteneurs "running" dans Portainer
- [ ] Worker "healthy" (pas d'erreur dans les logs)
- [ ] Dagster UI accessible (http://srv991054.hstgr.cloud:8080)
- [ ] Assets visibles dans Dagster
- [ ] Schedules activés (optionnel)
- [ ] PostgreSQL accessible via Adminer (http://srv991054.hstgr.cloud:8081)

---

## 📚 Documentation Complète

- [Architecture](docs/ARCHITECTURE.md)
- [Configuration Environnement](docs/ENVIRONMENT_CONFIGURATION.md)
- [Setup Portainer](docs/PORTAINER_SETUP.md)
- [Optimisations (Indexes, FK, PostGIS)](docs/OPTIMISATIONS.md)
- [Troubleshooting GitLab CI](docs/GITLAB_CI_TROUBLESHOOTING.md)

---

## 🆘 Support

**En cas de problème:**

1. Vérifier les logs dans Portainer
2. Exécuter `scripts/debug-worker-health.sh`
3. Consulter [GITLAB_CI_TROUBLESHOOTING.md](docs/GITLAB_CI_TROUBLESHOOTING.md)
4. Vérifier https://status.docker.com/ (pannes Docker Hub)

**Contacts:**
- GitLab Issues: https://gitlab.com/ringuet/hubeau_data_integration/-/issues
- Status Docker: https://status.docker.com/
