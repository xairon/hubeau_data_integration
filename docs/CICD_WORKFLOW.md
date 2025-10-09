# 🚀 Workflow CI/CD GitLab

## Architecture

```
Développement Local  →  Git Push  →  GitLab  →  Runner (sur VPS)  →  Déploiement Auto
```

Le **GitLab Runner tourne directement sur le VPS** `srv991054.hstgr.cloud`, ce qui permet un déploiement ultra-rapide sans SSH.

## Configuration du Runner (Une seule fois)

### 1. Ajouter le volume `/srv/brgm` au runner

Sur le serveur VPS :

```bash
ssh root@srv991054.hstgr.cloud

# Éditer la config du runner
nano /etc/gitlab-runner/config.toml
```

Ajouter `/srv/brgm:/srv/brgm` dans les volumes :

```toml
[[runners]]
  [runners.docker]
    volumes = [
      "/var/run/docker.sock:/var/run/docker.sock",
      "/srv/brgm:/srv/brgm",              # ← AJOUTER CETTE LIGNE
      "/srv/brgm-data:/srv/brgm-data"
    ]
```

Redémarrer le runner :

```bash
sudo gitlab-runner restart
sudo gitlab-runner verify
```

## Workflow de Développement

### 1. Développement local

```bash
# Faire vos modifications
git add .
git commit -m "feat: nouvelle fonctionnalité"
git push origin main
```

### 2. Pipeline automatique (tout seul !)

GitLab déclenche **automatiquement** le pipeline qui :

**Stage 1: Build (automatique)**
1. ✅ Clone le code sur le runner (dans `/builds/...`)
2. ✅ Copie les fichiers vers `/srv/brgm`
3. ✅ Build de l'image Docker `hubeau-dagster:latest`

**Stage 2: Deploy (automatique)**
1. ✅ Arrêt des anciens conteneurs
2. ✅ Démarrage avec la nouvelle image
3. ✅ Vérification que tout fonctionne

**Durée totale : ~3-4 minutes**

### 3. Vérification

Suivre le pipeline en temps réel :
```
https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/pipelines
```

Ou regarder les logs :
```bash
ssh root@srv991054.hstgr.cloud
docker logs -f brgm-dagster-webserver
```

## Rollback

Si un déploiement casse quelque chose :

1. Dans GitLab : `CI/CD > Pipelines`
2. Cliquer sur ▶️ **"rollback:production"**

Ça redémarre les services (utile si un conteneur crashe).

## Avantages de ce setup

✅ **Pas de SSH** : Le runner est déjà sur le serveur  
✅ **Rapide** : Déploiement en ~2 minutes  
✅ **Sécurisé** : Déploiement manuel (évite les accidents)  
✅ **Traçable** : Historique complet dans GitLab  
✅ **Volumes persistants** : Les données MinIO ne sont jamais supprimées  

## Dépannage

### Le runner n'a pas accès à `/srv/brgm`

```bash
# Vérifier les volumes montés
ssh root@srv991054.hstgr.cloud
cat /etc/gitlab-runner/config.toml | grep -A 5 volumes

# Devrait afficher /srv/brgm:/srv/brgm
```

### Le pipeline échoue sur `rsync`

```bash
# Installer rsync dans l'image si nécessaire (déjà fait normalement)
# Ou utiliser cp à la place :
cp -r ./* /srv/brgm/
```

### Les conteneurs ne redémarrent pas

```bash
# SSH sur le serveur et vérifier manuellement
ssh root@srv991054.hstgr.cloud
cd /srv/brgm
docker compose -f docker-compose.production.yml ps
docker compose -f docker-compose.production.yml logs
```

