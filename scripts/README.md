# Scripts de Production

Ce dossier contient **uniquement** les scripts de gestion du VPS en production. Tous les scripts de développement, debug et test ont été supprimés.

## 📁 Scripts Disponibles

### 🚀 Déploiement

#### `deploy.sh`
**Rôle** : Script de déploiement manuel (backup du pipeline GitLab CI/CD)

**Usage** :
```bash
./scripts/deploy.sh
```

**Fonctionnalités** :
- Création répertoires de données (`/srv/brgm-data`)
- Backup automatique MinIO avant déploiement
- Arrêt des conteneurs existants
- Build nouvelle image Docker
- Démarrage services (MinIO, Dagster)
- Health checks automatiques
- Affichage état final + credentials

**⚠️ Note** : En production, le déploiement est **automatique via GitLab CI/CD**. Ce script sert de backup manuel si besoin.

---

### 🛠️ Gestion VPS

#### `vps-manage.sh`
**Rôle** : Outil complet de gestion du VPS de production

**Usage** :
```bash
./scripts/vps-manage.sh [command]
```

**Commandes disponibles** :
- `status` : État des services
- `logs` : Voir les logs en temps réel
- `restart` : Redémarrer tous les services
- `stop` : Arrêter tous les services
- `start` : Démarrer tous les services
- `backup` : Créer backup MinIO
- `restore` : Restaurer backup MinIO
- `cleanup` : Nettoyer conteneurs/images inutilisés
- `update` : Mise à jour depuis Git
- `shell` : Accès shell conteneur Dagster
- `minio-shell` : Accès shell conteneur MinIO
- `psql` : Accès PostgreSQL Dagster

**Exemples** :
```bash
# Voir l'état
./scripts/vps-manage.sh status

# Voir les logs
./scripts/vps-manage.sh logs

# Créer un backup
./scripts/vps-manage.sh backup

# Redémarrer les services
./scripts/vps-manage.sh restart
```

---

### 🧹 Maintenance

#### `cleanup_server.sh`
**Rôle** : Nettoyage complet du serveur VPS

**Usage** :
```bash
./scripts/cleanup_server.sh
```

**Actions** :
- Backup de sécurité avant nettoyage
- Suppression conteneurs/images Docker inutilisés
- Nettoyage volumes orphelins
- Purge logs système (garder 3 jours)
- Nettoyage cache APT
- Nettoyage cache GitLab Runner

**⚠️ Attention** : Demande confirmation avant chaque action destructive

---

#### `configure_runner.sh`
**Rôle** : Configuration du GitLab Runner pour accès aux volumes

**Usage** :
```bash
./scripts/configure_runner.sh
```

**Fonctionnalités** :
- Backup config runner actuelle
- Affiche instructions pour ajouter volumes `/srv/brgm` et `/srv/brgm-data`
- Redémarrage runner après modification

**Volumes requis** :
```toml
volumes = [
  "/var/run/docker.sock:/var/run/docker.sock",
  "/srv/brgm:/srv/brgm",
  "/srv/brgm-data:/srv/brgm-data"
]
```

---

## 🔒 Variables d'Environnement

### `env.example`
**Rôle** : Template des variables d'environnement pour **développement local**

**Contenu** :
```bash
# Dagster
DAGSTER_PG_PASSWORD=your_dagster_password

# TimescaleDB (roadmap)
PG_PASSWORD=your_postgres_password

# Neo4j (roadmap)
NEO4J_PASSWORD=your_neo4j_password

# MinIO
MINIO_USER=admin
MINIO_PASS=your_minio_password
MINIO_REGION=us-east-1
MINIO_BRONZE_BUCKET=bronze
MINIO_ENDPOINT=http://minio:9000
```

### 📝 Usage en Développement Local

```bash
# 1. Copier le template
cp env.example .env

# 2. Éditer avec vos mots de passe
nano .env

# 3. Démarrer Docker Compose
docker-compose up -d
```

### 🚀 Production (GitLab CI/CD)

**En production, PAS DE .env local !**

Le pipeline GitLab CI/CD génère automatiquement `.env.production` depuis les **secrets GitLab** :

```yaml
# .gitlab-ci.yml (extrait)
- |
  cat > .env.production << EOF
  DAGSTER_PG_PASSWORD=${DAGSTER_PG_PASSWORD}
  MINIO_USER=${MINIO_USER}
  MINIO_PASS=${MINIO_PASS}
  # ... autres variables depuis secrets GitLab
  EOF
```

**Configuration secrets GitLab** :
1. Aller dans **Settings > CI/CD > Variables**
2. Ajouter les variables :
   - `DAGSTER_PG_PASSWORD`
   - `MINIO_USER`
   - `MINIO_PASS`
   - etc.
3. Marquer comme **Protected** et **Masked**

**Avantages** :
- ✅ Secrets centralisés et sécurisés
- ✅ Pas de fichiers sensibles dans Git
- ✅ Rotation facile des secrets
- ✅ Traçabilité des modifications

---

## 📊 Architecture de Déploiement

### Développement Local
```
env.example (template)
    ↓ cp
.env (local, git-ignored)
    ↓
docker-compose.yml
    ↓
Services locaux
```

### Production VPS
```
GitLab CI/CD Variables (secrets)
    ↓ generate
.env.production (généré auto)
    ↓
docker-compose.production.yml
    ↓
Services production
```

---

## 🗑️ Scripts Supprimés (Archive)

Les scripts suivants ont été **supprimés** car obsolètes/dev :

### Scripts Dev/Init (21 scripts)
- `bootstrap_all_services.py` - Init dev
- `bootstrap_minio.py` - Init MinIO dev
- `init_all.bat` - Init Windows dev
- `init_all.sh` - Init dev
- `init_all_databases.sh` - Roadmap (TimescaleDB/Neo4j pas utilisés)
- `init_minio.py` - Init MinIO dev
- `init_neo4j.cypher` - Roadmap (Neo4j Silver layer)
- `init_postgis.sql` - Roadmap (PostGIS Silver layer)
- `init_timescaledb.sql` - Roadmap (TimescaleDB Silver layer)

### Scripts Debug (6 scripts)
- `diagnose_dagster.sh` - Diagnostic dev
- `fix_dagster_connection.sh` - Fix dev
- `quick_check.sh` - Check dev
- `test_memory_fix.sh` - Test mémoire (fix implémenté)
- `verify_dlt_config.sh` - Vérification dev

### Scripts Monitoring/Test (6 scripts)
- `monitor_minio_data.py` - Monitoring dev
- `monitor_minio_json_report.py` - Monitoring dev
- `optimize_page_sizes.py` - Optimisation (tests terminés)
- `start_clean.sh` - Démarrage dev
- `test_architecture.py` - Test dev
- `test_dlt_architecture.py` - Test DLT dev
- `test_hydrobio_fixes.py` - Test hydrobiologie (terminé)

**Raison** : Ces scripts étaient utilisés pendant le développement. La production utilise maintenant GitLab CI/CD pour tout automatiser.

---

## 📚 Ressources

- **[GitLab CI/CD](./.gitlab-ci.yml)** : Pipeline de déploiement automatique
- **[Docker Compose Production](../docker-compose.production.yml)** : Configuration production
- **[Architecture](../docs/ARCHITECTURE_MODERNE.md)** : Documentation architecture
- **[Tutoriel DLT](../docs/TUTORIEL_DLT.md)** : Guide configuration DLT

