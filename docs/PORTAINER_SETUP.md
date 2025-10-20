# Installation Portainer (Standalone)

Portainer est un outil de gestion Docker avec interface web. Il doit être **installé une seule fois sur le serveur**, indépendamment du projet Hub'Eau.

## Pourquoi Standalone ?

Portainer gère **tous les conteneurs du serveur**, pas seulement ceux du projet Hub'Eau. Il doit donc :
- ✅ Être installé séparément du projet
- ✅ Persister indépendamment des déploiements
- ✅ Rester accessible même si le projet est down

## Installation sur le Serveur

### 1. SSH au serveur

```bash
ssh root@srv991054.hstgr.cloud
```

### 2. Créer un répertoire pour Portainer

```bash
mkdir -p /srv/portainer
cd /srv/portainer
```

### 3. Télécharger le fichier compose

Depuis votre machine locale, copier le fichier :

```bash
# Depuis votre machine locale
scp portainer-compose.yml root@srv991054.hstgr.cloud:/srv/portainer/docker-compose.yml
```

Ou créer directement sur le serveur :

```bash
cat > /srv/portainer/docker-compose.yml << 'HEREDOC'
services:
  portainer:
    image: portainer/portainer-ce:2.19.4-alpine
    container_name: portainer
    restart: unless-stopped
    security_opt:
      - no-new-privileges:true
    ports:
      - "9443:9443"   # HTTPS interface
      - "8000:8000"   # Edge agent (optionnel)
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - portainer_data:/data
    command: --http-disabled
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "https://localhost:9443", "--no-check-certificate"]
      interval: 30s
      timeout: 10s
      retries: 5
    mem_limit: 256m
    cpus: 0.1

volumes:
  portainer_data:
    driver: local
HEREDOC
```

### 4. Démarrer Portainer

```bash
docker compose up -d
```

### 5. Vérifier le status

```bash
docker ps | grep portainer
# Doit afficher: portainer ... Up ... 0.0.0.0:9443->9443/tcp
```

## Premier Accès

### 1. Ouvrir l'interface web

```
https://srv991054.hstgr.cloud:9443
```

⚠️ Le navigateur affichera un avertissement de sécurité (certificat auto-signé). C'est normal, cliquez sur "Avancé" puis "Continuer".

### 2. Créer le compte admin

**Lors de la première connexion (dans les 5 minutes après le démarrage)** :

1. Username : `admin`
2. Password : **Choisir un mot de passe fort (12+ caractères)**
3. Confirmer le mot de passe
4. Cliquer "Create user"

⚠️ **IMPORTANT** : Si vous ne créez pas le compte dans les 5 minutes, Portainer se verrouille. Vous devrez alors restart le conteneur :

```bash
docker restart portainer
```

### 3. Sélectionner l'environnement

1. Sélectionner "Get Started"
2. Cliquer sur "local"
3. Vous devriez voir tous les conteneurs Docker du serveur

## Utilisation

### Dashboard

- **Stacks** : Voir tous les docker-compose (brgm, portainer, etc.)
- **Containers** : Gérer les conteneurs individuellement
- **Images** : Voir toutes les images Docker
- **Volumes** : Gérer les volumes persistants
- **Networks** : Voir les réseaux Docker

### Gérer le projet Hub'Eau

1. Aller dans **Stacks**
2. Vous verrez le stack `brgm` (si déployé)
3. Vous pouvez :
   - Voir les logs de chaque conteneur
   - Restart/Stop/Start des conteneurs
   - Voir les statistiques (CPU, RAM, réseau)

### Logs en temps réel

1. Aller dans **Containers**
2. Cliquer sur un conteneur (ex: `brgm-dagster-webserver`)
3. Cliquer sur **Logs**
4. Activer "Auto-refresh logs"

### Débugger les problèmes de health check

Si un conteneur est marqué "unhealthy" (ex: `brgm-dlt-worker`) :

1. **Voir les logs en direct** :
   - Containers → Cliquer sur le conteneur
   - Onglet "Logs" → Activer "Auto-refresh"
   - Chercher les erreurs de connexion, variables manquantes, etc.

2. **Inspecter la configuration** :
   - Onglet "Inspect" → Voir la config complète
   - Section "Health" → Voir le statut du health check
   - Section "Env" → Vérifier les variables d'environnement

3. **Console interactive** :
   - Onglet "Console" → Cliquer "Connect"
   - Lancer des commandes de test :
     ```bash
     # Test connexion PostgreSQL
     psql -h postgres -U postgres -d hubeau

     # Test variables
     env | grep PG_

     # Test Python
     python -c "import psycopg2; print('OK')"
     ```

4. **Statistiques de ressources** :
   - Onglet "Stats" → Voir CPU, RAM, Network
   - Identifier les problèmes de performance

5. **Restart rapide** :
   - Bouton "Restart" en haut
   - Plus rapide que `docker restart` en SSH

## Maintenance

### Mise à jour Portainer

```bash
cd /srv/portainer
docker compose pull
docker compose up -d
```

### Backup des données

```bash
# Backup du volume Portainer (configurations, users, etc.)
docker run --rm -v portainer_data:/data -v $(pwd):/backup alpine tar czf /backup/portainer-backup-$(date +%Y%m%d).tar.gz -C /data .
```

### Restauration

```bash
# Restore depuis backup
docker run --rm -v portainer_data:/data -v $(pwd):/backup alpine sh -c "cd /data && tar xzf /backup/portainer-backup-YYYYMMDD.tar.gz"
docker restart portainer
```

### Réinitialiser le mot de passe admin

Si vous avez oublié le mot de passe :

```bash
docker stop portainer
docker run --rm -v portainer_data:/data portainer/helper-reset-password
docker start portainer
```

## Dépannage

### Portainer ne démarre pas

```bash
# Vérifier les logs
docker logs portainer

# Vérifier que le port 9443 est libre
netstat -tulpn | grep 9443

# Restart
docker restart portainer
```

### Impossible de se connecter

1. Vérifier que le firewall autorise le port 9443
2. Vérifier que le conteneur est up : `docker ps | grep portainer`
3. Vérifier les logs : `docker logs portainer`

### "Admin user already initialized"

Le compte admin existe déjà. Vous devez réinitialiser le mot de passe (voir ci-dessus).

## Sécurité

### Bonnes pratiques

- ✅ Utiliser un mot de passe fort pour l'admin (12+ caractères)
- ✅ HTTP désactivé (HTTPS uniquement)
- ✅ Volume en lecture seule pour Docker socket (`:ro`)
- ✅ `no-new-privileges` activé
- ✅ Limites de ressources (256 MB RAM, 0.1 CPU)

### Firewall (optionnel)

Pour restreindre l'accès à Portainer depuis certaines IPs uniquement :

```bash
# Autoriser uniquement depuis votre IP
ufw allow from VOTRE_IP to any port 9443
ufw deny 9443
```

## Comparaison avec l'Ancienne Configuration

### ❌ Avant (dans docker-compose.production.yml)

```yaml
services:
  portainer:
    # ... configuration ...
    volumes:
      - /srv/brgm-data/portainer:/data  # ❌ Couplé au projet
```

**Problèmes** :
- Portainer redémarré à chaque déploiement du projet
- Données dans `/srv/brgm-data/` (couplées au projet)
- Arrêt de Portainer si le projet est down

### ✅ Maintenant (standalone)

```bash
/srv/portainer/docker-compose.yml  # ✅ Séparé du projet
```

**Avantages** :
- Portainer indépendant des déploiements
- Gère tous les conteneurs du serveur
- Toujours accessible
- Standard Docker/Portainer

## Ressources

- [Documentation officielle Portainer](https://docs.portainer.io/)
- [Portainer CE sur GitHub](https://github.com/portainer/portainer)

---

**Note** : Portainer Community Edition (CE) est gratuit et open-source. Pour des fonctionnalités avancées (RBAC, registries multiples, etc.), voir Portainer Business Edition.
