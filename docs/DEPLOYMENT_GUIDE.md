# 🚀 Guide de Déploiement Production VPS Hostinger

## Vue d'ensemble

Ce guide explique comment déployer automatiquement le projet Hub'Eau sur votre VPS Hostinger avec:
- ✅ GitLab Runner pour CI/CD automatique
- ✅ Volumes persistants MinIO (données conservées)
- ✅ Mode incrémental DLT (pas de duplication)
- ✅ Auto-déploiement sur push Git

---

## 📋 Prérequis

- VPS Hostinger (minimum KVM 1: 1 vCPU, 4 GB RAM, 50 GB SSD)
- Accès SSH au VPS: `ssh root@srv991054.hstgr.cloud`
- Projet GitLab configuré
- Docker et Docker Compose installés sur le VPS

---

## 🔧 Installation Initiale (Une seule fois)

### 1. Se connecter au VPS

```bash
ssh root@srv991054.hstgr.cloud
```

### 2. Installer les dépendances

```bash
# Mettre à jour le système
apt update && apt upgrade -y

# Installer Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Installer Docker Compose
apt install docker-compose-plugin -y

# Vérifier l'installation
docker --version
docker compose version
```

### 3. Installer GitLab Runner

```bash
# Ajouter le dépôt GitLab
curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh" | bash

# Installer GitLab Runner
apt-get install gitlab-runner

# Vérifier
gitlab-runner --version
```

### 4. Créer la structure de répertoires

```bash
# Répertoire du projet
mkdir -p /srv/brgm
cd /srv/brgm

# Répertoire des données persistantes
mkdir -p /srv/brgm-data/{minio,dagster_pg,backups}

# Donner les permissions
chown -R 1000:1000 /srv/brgm-data
```

### 5. Cloner le projet

```bash
cd /srv/brgm

# Remplacer par votre URL GitLab
git clone https://gitlab.com/VOTRE_USERNAME/brgm.git .

# Créer le fichier .env.production
cp .env.production.example .env.production

# Éditer les credentials
nano .env.production
```

**Remplir `.env.production`:**
```env
DAGSTER_PG_PASSWORD=VotreMotDePasseSecurise123!
MINIO_USER=admin
MINIO_PASS=VotreMotDePasseMinIOSecurise456!
MINIO_ENDPOINT=http://minio:9000
MINIO_BRONZE_BUCKET=bronze
MINIO_REGION=us-east-1
```

### 6. Enregistrer le GitLab Runner

```bash
# Obtenir le token depuis GitLab:
# Aller sur: https://gitlab.com/VOTRE_PROJET/-/settings/ci_cd
# Section: Runners > New project runner
# Copier le token

# Enregistrer le runner
gitlab-runner register \
  --url https://gitlab.com/ \
  --registration-token VOTRE_TOKEN_ICI \
  --executor docker \
  --docker-image docker:24-dind \
  --docker-privileged \
  --docker-volumes /var/run/docker.sock:/var/run/docker.sock \
  --docker-volumes /srv/brgm:/srv/brgm \
  --docker-volumes /srv/brgm-data:/srv/brgm-data \
  --description "Hostinger VPS Runner" \
  --tag-list "vps,production,auto-deploy" \
  --non-interactive

# Démarrer le runner
gitlab-runner start
```

### 7. Premier déploiement manuel

```bash
cd /srv/brgm

# Rendre le script exécutable
chmod +x scripts/deploy.sh

# Lancer le déploiement
bash scripts/deploy.sh
```

**Attendre ~2-3 minutes** que tous les services démarrent.

### 8. Vérifier que tout fonctionne

```bash
# Voir l'état des conteneurs
docker compose -f docker-compose.production.yml ps

# Tous les conteneurs doivent être "Up (healthy)"

# Accéder aux interfaces web:
# - Dagster UI: http://srv991054.hstgr.cloud:8080
# - MinIO Console: http://srv991054.hstgr.cloud:9001
```

---

## 🔄 Workflow de Développement

### Développement local

```bash
# Faire des modifications
git add .
git commit -m "Ajout fonctionnalité X"

# Pousser sur GitLab
git push origin main
```

### Auto-déploiement

1. GitLab détecte le push sur `main`
2. Le runner VPS démarre automatiquement
3. Étapes CI/CD:
   - ✅ Build de la nouvelle image Docker
   - ⏸️ Attend confirmation manuelle pour déployer
   - 🚀 Déploiement automatique
   - ✅ Health checks
   - 📊 Services redémarrés

4. Aller sur GitLab: `CI/CD > Pipelines`
5. Cliquer sur le bouton ▶️ "Play" pour `deploy:production`

### Rollback en cas de problème

Si le déploiement a cassé quelque chose:

```bash
# Option 1: Via GitLab UI
# Aller sur: CI/CD > Pipelines
# Cliquer sur ▶️ "rollback:production"

# Option 2: Manuellement sur le VPS
ssh root@srv991054.hstgr.cloud
cd /srv/brgm
git checkout HEAD~1  # Revenir à la version précédente
bash scripts/deploy.sh
```

---

## 💾 Gestion des Données

### Persistance MinIO

Les données sont stockées dans: `/srv/brgm-data/minio/`

**Structure:**
```
/srv/brgm-data/minio/
├── bronze/
│   ├── temperature_api/
│   │   ├── temperature_stations/*.parquet
│   │   └── temperature_chroniques/*.parquet
│   ├── quality_groundwater_api/
│   │   └── quality_groundwater_stations/*.parquet
│   ├── _dlt_pipeline_state/  # État DLT (incrémental)
│   └── _dlt_loads/           # Métadonnées des chargements
```

### Sauvegardes automatiques

Le script `deploy.sh` crée automatiquement une sauvegarde avant chaque déploiement:

```bash
# Voir les sauvegardes
ls -lh /srv/brgm-data/backups/

# Restaurer une sauvegarde
cd /srv/brgm-data
tar -xzf backups/minio_backup_20250109_120000.tar.gz
```

### Sauvegarde manuelle

```bash
# Créer une sauvegarde complète
cd /srv/brgm-data
tar -czf backups/manual_backup_$(date +%Y%m%d).tar.gz minio/

# Copier la sauvegarde en local
scp root@srv991054.hstgr.cloud:/srv/brgm-data/backups/*.tar.gz ./
```

---

## 🔁 Mode Incrémental DLT

### Comment ça marche

DLT évite automatiquement les duplications grâce à:

1. **Primary Keys** (définis dans `configs/hubeau/*.yml`):
   ```yaml
   primary_keys:
     - code_station
     - date_mesure_temp
   ```

2. **State Management** (stocké dans MinIO `_dlt_pipeline_state/`):
   - Dernier `load_id` pour chaque table
   - Checksums des données
   - Métadonnées de chaque run

3. **Write Disposition**:
   - `merge`: Remplace les doublons (recommandé pour observations)
   - `replace`: Remplace toute la table (pour référentiels)

### Test de non-duplication

```bash
# 1. Lancer un job (ex: temperature_chroniques pour 2024)
# Dans Dagster UI: Matérialiser l'asset

# 2. Compter les records
docker exec brgm-minio-1 mc ls -r local/bronze/temperature_api/temperature_chroniques/

# 3. Relancer le même job

# 4. Vérifier que le nombre de records n'a PAS doublé
# Les fichiers avec le même load_id sont remplacés, pas dupliqués
```

---

## 📊 Monitoring

### Voir les logs en temps réel

```bash
ssh root@srv991054.hstgr.cloud

# Tous les logs
docker compose -f /srv/brgm/docker-compose.production.yml logs -f

# Logs d'un service spécifique
docker compose -f /srv/brgm/docker-compose.production.yml logs -f dagster_daemon
docker compose -f /srv/brgm/docker-compose.production.yml logs -f minio
```

### Vérifier l'utilisation des ressources

```bash
# CPU, RAM, Disk
docker stats

# Espace disque MinIO
du -sh /srv/brgm-data/minio/
```

### Interfaces Web

- **Dagster UI**: http://srv991054.hstgr.cloud:8080
  - Voir les jobs en cours
  - Historique des runs
  - Logs détaillés

- **MinIO Console**: http://srv991054.hstgr.cloud:9001
  - Explorer les buckets
  - Voir les fichiers Parquet
  - Statistiques de stockage

---

## 🔒 Sécurité

### Configurer le firewall

```bash
# Autoriser uniquement les ports nécessaires
ufw allow 22/tcp    # SSH
ufw allow 8080/tcp  # Dagster
ufw allow 9001/tcp  # MinIO Console
ufw enable
```

### Changer les mots de passe par défaut

```bash
# Éditer .env.production
nano /srv/brgm/.env.production

# Utiliser des mots de passe forts (>16 caractères)
# Redéployer
cd /srv/brgm
bash scripts/deploy.sh
```

### Mettre à jour régulièrement

```bash
# Mettre à jour le système
apt update && apt upgrade -y

# Mettre à jour les images Docker
docker compose -f /srv/brgm/docker-compose.production.yml pull
bash /srv/brgm/scripts/deploy.sh
```

---

## 🐛 Dépannage

### Les conteneurs ne démarrent pas

```bash
# Voir les logs d'erreur
docker compose -f /srv/brgm/docker-compose.production.yml logs

# Vérifier les ressources
free -h  # RAM disponible
df -h    # Espace disque

# Redémarrer
docker compose -f /srv/brgm/docker-compose.production.yml restart
```

### Le runner GitLab ne se connecte pas

```bash
# Vérifier l'état
gitlab-runner verify

# Redémarrer
gitlab-runner restart

# Voir les logs
journalctl -u gitlab-runner -f
```

### MinIO est plein

```bash
# Voir l'utilisation
du -sh /srv/brgm-data/minio/*

# Supprimer les anciennes sauvegardes
rm /srv/brgm-data/backups/minio_backup_old*.tar.gz

# Nettoyer les anciens loads DLT (si nécessaire)
docker exec brgm-minio-1 mc rm -r --force local/bronze/_dlt_loads/old_loads/
```

---

## 📚 Ressources

- [Documentation DLT](https://dlthub.com/docs)
- [Documentation Dagster](https://docs.dagster.io)
- [Documentation MinIO](https://min.io/docs/minio/linux/index.html)
- [GitLab CI/CD](https://docs.gitlab.com/ee/ci/)

---

## 🎯 Checklist de Déploiement

- [ ] VPS créé et accessible via SSH
- [ ] Docker et Docker Compose installés
- [ ] GitLab Runner installé et enregistré
- [ ] Répertoires `/srv/brgm` et `/srv/brgm-data` créés
- [ ] Projet cloné dans `/srv/brgm`
- [ ] Fichier `.env.production` configuré
- [ ] Premier déploiement réussi (`bash scripts/deploy.sh`)
- [ ] Dagster UI accessible (port 8080)
- [ ] MinIO Console accessible (port 9001)
- [ ] GitLab Runner visible dans GitLab UI (point vert)
- [ ] Test de push Git → déploiement automatique
- [ ] Test de non-duplication des données
- [ ] Sauvegardes configurées
- [ ] Firewall configuré
