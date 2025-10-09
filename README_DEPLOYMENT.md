# 🚀 Déploiement Automatique VPS Hostinger

## Résumé Rapide

✅ **VPS Hostinger KVM 1** (4.99$/mois) - Parfait pour Bronze uniquement
✅ **GitLab Runner** - Auto-déploiement sur push
✅ **Volumes persistants** - Données MinIO conservées
✅ **Mode incrémental DLT** - Pas de duplication
✅ **Sauvegardes automatiques** - Avant chaque déploiement

---

## 📝 Quick Start (5 minutes)

### Sur votre VPS

```bash
# 1. Se connecter
ssh root@srv991054.hstgr.cloud

# 2. Installer
curl -fsSL https://get.docker.com | sh
apt install docker-compose-plugin gitlab-runner -y

# 3. Créer les répertoires
mkdir -p /srv/brgm /srv/brgm-data/{minio,dagster_pg,backups}

# 4. Cloner le projet
cd /srv/brgm
git clone https://gitlab.com/VOTRE_USERNAME/brgm.git .

# 5. Configurer les secrets
cp .env.production.example .env.production
nano .env.production  # Remplir les mots de passe

# 6. Enregistrer le GitLab Runner
gitlab-runner register \
  --url https://gitlab.com/ \
  --registration-token VOTRE_TOKEN \
  --executor docker \
  --docker-image docker:24-dind \
  --docker-privileged \
  --docker-volumes /var/run/docker.sock:/var/run/docker.sock \
  --docker-volumes /srv/brgm:/srv/brgm \
  --docker-volumes /srv/brgm-data:/srv/brgm-data \
  --tag-list "vps,production"

# 7. Premier déploiement
chmod +x scripts/*.sh
bash scripts/deploy.sh
```

**Attendre 2-3 minutes**, puis accéder à:
- Dagster: http://srv991054.hstgr.cloud:8080
- MinIO: http://srv991054.hstgr.cloud:9001

---

## 🔄 Workflow Quotidien

### Développer localement

```bash
# Faire vos modifications
git add .
git commit -m "Description des changements"
git push origin main
```

### Déployer automatiquement

1. GitLab détecte le push
2. Le runner VPS build l'image
3. **Vous cliquez sur ▶️ "Play"** dans GitLab UI pour valider
4. Déploiement automatique avec sauvegarde
5. Services redémarrés

---

## 🛠️ Commandes Utiles (sur le VPS)

```bash
# Outil de gestion simplifié
cd /srv/brgm
./scripts/vps-manage.sh [commande]

# Commandes fréquentes:
./scripts/vps-manage.sh status      # État des services
./scripts/vps-manage.sh logs        # Voir tous les logs
./scripts/vps-manage.sh health      # Health check complet
./scripts/vps-manage.sh backup      # Sauvegarde manuelle
./scripts/vps-manage.sh storage     # Usage du stockage
./scripts/vps-manage.sh restart     # Redémarrer tout
```

### Accès direct Docker

```bash
# Voir l'état
docker compose -f docker-compose.production.yml ps

# Logs en temps réel
docker compose -f docker-compose.production.yml logs -f

# Redémarrer un service
docker compose -f docker-compose.production.yml restart dagster_daemon

# Shell dans un conteneur
docker exec -it brgm-dagster-daemon bash
```

---

## 💾 Données Persistantes

### Structure des données

```
/srv/brgm-data/
├── minio/                    # ✅ Données MinIO (PERSISTANT)
│   └── bronze/
│       ├── temperature_api/
│       ├── quality_groundwater_api/
│       ├── _dlt_pipeline_state/  # État DLT
│       └── _dlt_loads/
├── dagster_pg/              # ✅ Base Dagster (PERSISTANT)
└── backups/                 # ✅ Sauvegardes auto (PERSISTANT)
    ├── minio_backup_20250109_120000.tar.gz
    └── manual_backup_20250110.tar.gz
```

### Vérifier l'incrémental DLT

```bash
# 1. Lancer un job (ex: temperature_chroniques pour 2024)
# Via Dagster UI: Matérialiser l'asset

# 2. Voir les fichiers créés
docker exec brgm-minio-1 mc ls -r local/bronze/temperature_api/

# 3. Relancer le MÊME job

# 4. Vérifier que les fichiers n'ont pas doublé
# ✅ Même load_id = remplacé, pas dupliqué
```

---

## 🔒 Sécurité

### Firewall

```bash
ufw allow 22/tcp     # SSH
ufw allow 8080/tcp   # Dagster
ufw allow 9001/tcp   # MinIO
ufw enable
```

### Changer les secrets

```bash
nano /srv/brgm/.env.production
# Modifier DAGSTER_PG_PASSWORD et MINIO_PASS
./scripts/vps-manage.sh deploy
```

---

## 📊 Monitoring

### Ressources VPS

```bash
# CPU, RAM, Disque
./scripts/vps-manage.sh stats

# Espace MinIO
./scripts/vps-manage.sh storage

# Health check complet
./scripts/vps-manage.sh health
```

### Logs Dagster

- **UI Web**: http://srv991054.hstgr.cloud:8080
  - Runs > Sélectionner un run > Logs
  - Assets > Voir les matérialisations

- **Ligne de commande**:
  ```bash
  docker compose -f /srv/brgm/docker-compose.production.yml logs -f dagster_daemon
  ```

---

## 🐛 Dépannage

### Problème: Conteneurs ne démarrent pas

```bash
# Voir les erreurs
./scripts/vps-manage.sh logs

# Vérifier les ressources
free -h  # RAM
df -h    # Disque

# Redémarrer
./scripts/vps-manage.sh restart
```

### Problème: GitLab Runner ne fonctionne pas

```bash
# Vérifier
gitlab-runner verify

# Redémarrer
gitlab-runner restart

# Logs
journalctl -u gitlab-runner -f
```

### Problème: Données dupliquées

```bash
# Vérifier le state DLT
docker exec brgm-minio-1 mc ls -r local/bronze/_dlt_pipeline_state/

# Si vide, le state n'est pas persisté !
# Solution: Vérifier que MinIO stocke bien dans /srv/brgm-data/minio/
```

### Rollback d'urgence

```bash
# Option 1: Via GitLab (recommandé)
# UI > Pipelines > Cliquer sur ▶️ rollback:production

# Option 2: Manuellement
cd /srv/brgm
git checkout HEAD~1
./scripts/deploy.sh

# Option 3: Restaurer une sauvegarde
./scripts/vps-manage.sh restore minio_backup_YYYYMMDD_HHMMSS.tar.gz
```

---

## 📚 Documentation

- **Guide complet**: [docs/DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md)
- **Setup GitLab Runner**: [docs/gitlab-runner-setup.md](docs/gitlab-runner-setup.md)
- **Mode incrémental DLT**: [docs/dlt-incremental-mode.md](docs/dlt-incremental-mode.md)

---

## ✅ Checklist Avant Production

- [ ] VPS accessible via SSH
- [ ] Docker + Docker Compose installés
- [ ] GitLab Runner installé et enregistré
- [ ] Projet cloné dans `/srv/brgm`
- [ ] `.env.production` configuré avec secrets forts
- [ ] Premier déploiement réussi
- [ ] Dagster UI accessible (http://srv991054.hstgr.cloud:8080)
- [ ] MinIO Console accessible (http://srv991054.hstgr.cloud:9001)
- [ ] Runner visible dans GitLab (point vert)
- [ ] Test: push Git → CI/CD → déploiement auto
- [ ] Test: job Dagster → vérifier données dans MinIO
- [ ] Test: relancer job → vérifier pas de duplication
- [ ] Sauvegarde initiale créée
- [ ] Firewall configuré
- [ ] Monitoring configuré (optionnel: Grafana, Uptime Kuma)

---

## 🎯 Ressources VPS

### Configuration actuelle: VPS KVM 1

- **CPU**: 1 vCPU
- **RAM**: 4 GB
- **SSD**: 50 GB NVMe
- **Bandwidth**: 4 TB/mois
- **Prix**: 4.99$/mois

### Limites Docker configurées

```yaml
dagster_daemon:   2 GB RAM, 0.7 CPU
dagster_webserver: 1 GB RAM, 0.3 CPU
minio:            512 MB RAM, 0.2 CPU
dagster_postgres: 512 MB RAM, 0.2 CPU
```

### Utilisation estimée

- **Stockage**: ~40 GB / 50 GB (80%)
- **RAM**: ~3.5 GB / 4 GB (87%)
- **CPU**: Variable selon jobs (pics à 100% OK)

**⚠️ Astuce**: Lancer les jobs 1 par 1 (pas en parallèle) pour éviter les pics RAM.

---

## 💡 Tips & Tricks

### Optimiser l'espace disque

```bash
# Nettoyer les anciennes images Docker
./scripts/vps-manage.sh clean

# Supprimer les anciennes sauvegardes (garder 5 dernières)
cd /srv/brgm-data/backups
ls -t *.tar.gz | tail -n +6 | xargs rm
```

### Automatiser les jobs Dagster

Dans Dagster UI:
1. Automation > Schedules
2. Créer un schedule quotidien (ex: 2h du matin)
3. Activer

### Monitorer les coûts

- Bandwidth: Vérifier dans Hostinger Dashboard
- Si >4TB/mois: Limiter la fréquence des jobs

### Backup vers cloud externe

```bash
# Copier les sauvegardes vers S3/Google Drive
rclone copy /srv/brgm-data/backups remote:backups/

# OU vers votre machine locale
scp root@srv991054.hstgr.cloud:/srv/brgm-data/backups/*.tar.gz ./backups/
```

---

## 🆘 Support

- **Issues GitHub/GitLab**: Ouvrir un ticket
- **Logs Dagster**: Toujours inclure dans les rapports de bug
- **Config système**: `uname -a`, `docker --version`, `free -h`, `df -h`

---

**Bon déploiement ! 🚀**
