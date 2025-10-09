# 🖥️ Setup Serveur VPS Production

Guide complet pour configurer le serveur VPS depuis zéro.

## 📋 Informations Serveur

- **Serveur**: srv991054.hstgr.cloud
- **OS**: Ubuntu 22.04 LTS
- **RAM**: 4 GB
- **CPU**: 1 vCPU
- **Stockage**: 50 GB SSD

## 🗂️ Structure des Répertoires

```
/srv/
├── brgm/                      # Projet (géré par Git + pipeline)
│   ├── .env.production        # Variables d'environnement (NON commité)
│   ├── docker-compose.production.yml
│   ├── src/
│   ├── pipelines/
│   └── ...
└── brgm-data/                 # Données persistantes (JAMAIS supprimées)
    ├── minio/                 # Données MinIO (Parquet files)
    ├── dagster_pg/            # Base PostgreSQL Dagster
    └── backups/               # Sauvegardes automatiques
```

## 🚀 Installation Initiale (Une fois)

### 1. Connexion SSH

```bash
ssh root@srv991054.hstgr.cloud
```

### 2. Mise à jour du système

```bash
apt update && apt upgrade -y
apt install -y curl wget git vim htop
```

### 3. Installation Docker

```bash
# Installer Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# Vérifier
docker --version
docker compose version
```

### 4. Installation GitLab Runner

```bash
# Ajouter le dépôt GitLab
curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh" | bash

# Installer GitLab Runner
apt-get install gitlab-runner

# Vérifier
gitlab-runner --version
```

### 5. Créer la structure de répertoires

```bash
# Répertoires du projet
mkdir -p /srv/brgm
mkdir -p /srv/brgm-data/{minio,dagster_pg,backups}

# Permissions
chown -R 1000:1000 /srv/brgm-data
```

### 6. Cloner le projet

```bash
cd /srv/brgm

# Cloner depuis GitLab (adapter l'URL)
git clone https://scm.univ-tours.fr/ringuet/hubeau_data_integration.git .

# Configurer Git pour les pulls automatiques
git config --global user.name "GitLab Runner"
git config --global user.email "runner@brgm.local"
```

### 7. Créer le fichier `.env.production`

```bash
cd /srv/brgm

# Copier le template
cp env.example .env.production

# Éditer avec VOS mots de passe
nano .env.production
```

**Contenu du `.env.production` :**

```env
# Dagster PostgreSQL
DAGSTER_PG_PASSWORD=VotreMotDePasseSecurisePostgres123!

# MinIO
MINIO_USER=admin
MINIO_PASS=VotreMotDePasseSecuriseMinIO456!
MINIO_REGION=us-east-1
MINIO_BRONZE_BUCKET=bronze
MINIO_ENDPOINT=http://minio:9000
```

⚠️ **Important** : 
- Utilisez des mots de passe FORTS (min 16 caractères)
- Ne partagez JAMAIS ce fichier
- Il est dans `.gitignore` et ne sera JAMAIS commité

### 8. Configuration GitLab Runner

```bash
# Enregistrer le runner
gitlab-runner register \
  --url https://scm.univ-tours.fr/ \
  --registration-token VOTRE_TOKEN_GITLAB \
  --executor docker \
  --docker-image docker:24-cli \
  --docker-privileged \
  --docker-volumes /var/run/docker.sock:/var/run/docker.sock \
  --docker-volumes /srv/brgm:/srv/brgm \
  --docker-volumes /srv/brgm-data:/srv/brgm-data \
  --description "BRGM VPS Runner" \
  --tag-list "hubeau" \
  --non-interactive

# Démarrer le runner
gitlab-runner start
gitlab-runner verify
```

### 9. Premier déploiement

```bash
cd /srv/brgm

# Build et démarrage
docker compose -f docker-compose.production.yml --env-file .env.production up -d

# Attendre ~2 minutes
sleep 120

# Vérifier les services
docker compose -f docker-compose.production.yml ps
docker logs brgm-dagster-webserver --tail 30
```

### 10. Vérification

Accédez aux services :

- **Dagster UI** : http://srv991054.hstgr.cloud:8080
- **MinIO Console** : http://srv991054.hstgr.cloud:9001

## 🔄 Workflow de Déploiement

Une fois configuré, le déploiement est **100% automatique** :

```bash
# Sur votre PC
git add .
git commit -m "feat: ma nouvelle fonctionnalité"
git push origin main

# Le pipeline GitLab fait automatiquement :
# 1. Clone le code sur le serveur
# 2. Build l'image Docker
# 3. Redémarre les services
# 4. Vérifie que tout fonctionne
```

## 🧹 Nettoyage du Serveur

Pour nettoyer l'ancien projet et libérer de l'espace :

```bash
ssh root@srv991054.hstgr.cloud
cd /srv/brgm

# Lancer le script de nettoyage
bash scripts/cleanup_server.sh
```

Le script va :
- ✅ Sauvegarder l'ancien projet
- ✅ Supprimer `/hubeau_data_integration`
- ✅ Nettoyer les conteneurs Docker inutilisés
- ✅ Nettoyer les images Docker inutilisées
- ✅ Nettoyer les logs système
- ✅ Libérer l'espace disque

## 🔒 Sécurité

### Firewall

```bash
# Installer UFW
apt install ufw

# Autoriser SSH
ufw allow 22/tcp

# Autoriser Dagster
ufw allow 8080/tcp

# Autoriser MinIO Console
ufw allow 9001/tcp

# Activer
ufw enable
ufw status
```

### Secrets GitLab

Dans GitLab : **Settings > CI/CD > Variables**

Ajouter (Protected + Masked) :
- `DAGSTER_PG_PASSWORD`
- `MINIO_USER`
- `MINIO_PASS`

### Mise à jour système

```bash
# Automatique chaque semaine
apt update && apt upgrade -y
docker system prune -af
```

## 📊 Monitoring

### Vérifier l'espace disque

```bash
df -h
du -sh /srv/brgm-data/*
```

### Logs des services

```bash
# Logs Dagster
docker logs -f brgm-dagster-webserver

# Logs MinIO
docker logs -f brgm-minio

# Logs PostgreSQL
docker logs -f brgm-dagster-postgres
```

### Statistiques Docker

```bash
docker stats
docker system df
```

## 🆘 Dépannage

### Les services ne démarrent pas

```bash
# Vérifier les logs
docker compose -f /srv/brgm/docker-compose.production.yml logs

# Redémarrer
docker compose -f /srv/brgm/docker-compose.production.yml restart
```

### Dagster UI ne charge pas

```bash
# Lancer le diagnostic
cd /srv/brgm
bash scripts/diagnose_dagster.sh
```

### Manque d'espace disque

```bash
# Nettoyer Docker
docker system prune -af --volumes

# Nettoyer les anciennes sauvegardes
rm /srv/brgm-data/backups/minio_backup_old*.tar.gz

# Lancer le nettoyage complet
bash /srv/brgm/scripts/cleanup_server.sh
```

### GitLab Runner ne fonctionne pas

```bash
# Vérifier le statut
gitlab-runner verify

# Redémarrer
gitlab-runner restart

# Voir les logs
journalctl -u gitlab-runner -f
```

## 💾 Sauvegardes

### Automatique

Le script `deploy.sh` crée automatiquement une sauvegarde avant chaque déploiement dans `/srv/brgm-data/backups/`.

### Manuelle

```bash
# Sauvegarder MinIO
cd /srv/brgm-data
tar -czf backups/manual_backup_$(date +%Y%m%d).tar.gz minio/

# Copier en local
scp root@srv991054.hstgr.cloud:/srv/brgm-data/backups/*.tar.gz ./
```

### Restauration

```bash
# Restaurer une sauvegarde
cd /srv/brgm-data
tar -xzf backups/minio_backup_20250109_120000.tar.gz
docker compose -f /srv/brgm/docker-compose.production.yml restart
```

## 📚 Checklist Complète

### Installation initiale
- [x] Serveur accessible en SSH
- [x] Docker et Docker Compose installés
- [x] GitLab Runner installé et configuré
- [x] Répertoires `/srv/brgm` et `/srv/brgm-data` créés
- [x] Projet cloné dans `/srv/brgm`
- [x] Fichier `.env.production` créé avec mots de passe forts
- [x] Premier déploiement réussi
- [x] Dagster UI accessible (port 8080)
- [x] MinIO Console accessible (port 9001)

### Sécurité
- [x] Firewall UFW configuré
- [x] Mots de passe forts (>16 caractères)
- [x] `.env.production` NON commité
- [x] Secrets GitLab configurés
- [x] Mises à jour système régulières

### Workflow
- [x] Pipeline GitLab fonctionnel
- [x] Déploiement automatique sur push
- [x] Sauvegardes automatiques
- [x] Monitoring en place

---

**Dernière mise à jour** : 9 janvier 2025

