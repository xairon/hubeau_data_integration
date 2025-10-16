# Guide de Test Local - Hub'Eau Pipeline

Guide pour tester le projet en local avec Docker Desktop.

## ✅ Prérequis

1. ✅ Docker Desktop installé et démarré
2. ✅ Fichier `.env` présent (déjà fait)
3. ✅ Fichier `dagster_home/workspace.yaml` présent (déjà fait)

## 🚀 Démarrage Étape par Étape

### Option 1 : Démarrage Complet (Recommandé pour première fois)

```bash
cd e:\brgm

# Construire toutes les images
docker-compose build

# Démarrer tous les services
docker-compose up -d

# Attendre 1-2 minutes que tout démarre

# Vérifier l'état
docker-compose ps
```

### Option 2 : Démarrage Progressif (Pour Diagnostic)

```bash
cd e:\brgm

# 1. Démarrer les bases de données d'abord
docker-compose up -d dagster_postgres timescaledb postgis neo4j minio
echo "⏳ Attente 30 secondes..."
timeout /t 30

# 2. Vérifier que les DBs sont UP
docker-compose ps

# 3. Initialiser MinIO
docker-compose up minio_init

# 4. Construire et démarrer le worker
docker-compose build dlt_worker
docker-compose up -d dlt_worker
echo "⏳ Attente 20 secondes..."
timeout /t 20

# 5. Construire et démarrer l'orchestrator
docker-compose build dagster_webserver
docker-compose up -d dagster_webserver dagster_daemon

# 6. Attendre le démarrage
echo "⏳ Attente 30 secondes..."
timeout /t 30

# 7. Vérifier
docker-compose ps
```

## 🔍 Vérifications

### 1. État des Conteneurs

```bash
docker-compose ps
```

**Résultat attendu :** Tous les services en "Up" ou "Up (healthy)"

### 2. Logs du Webserver

```bash
docker-compose logs dagster_webserver
```

**Chercher :**
- ✅ "Serving on http://0.0.0.0:3000"
- ✅ "dagster.webapp"
- ❌ Erreurs de connexion à dagster_postgres
- ❌ Erreurs de connexion à dlt_worker

### 3. Logs du Worker

```bash
docker-compose logs dlt_worker
```

**Chercher :**
- ✅ "Starting gRPC server"
- ✅ "Serving on 0.0.0.0:4000"
- ❌ Erreurs d'import Python
- ❌ Erreurs de connexion aux DBs

### 4. Test du Port 8080

Dans ton navigateur : http://localhost:8080

Ou en ligne de commande :
```bash
curl http://localhost:8080
```

## 🐛 Problèmes Courants en Local

### Problème 1 : Port 8080 déjà utilisé

**Symptôme :** Erreur "port is already allocated"

**Solution :**
```bash
# Trouver le processus
netstat -ano | findstr :8080

# Arrêter le processus (remplacer PID)
taskkill /PID <PID> /F

# Ou changer le port dans docker-compose.yml
```

### Problème 2 : Pas assez de mémoire Docker

**Symptôme :** Conteneurs qui crashent, "OOMKilled"

**Solution :**
1. Ouvrir Docker Desktop
2. Settings > Resources
3. Augmenter la mémoire à minimum 8GB
4. Redémarrer Docker Desktop

### Problème 3 : Images pas à jour

**Symptôme :** Erreur "module not found" ou comportement bizarre

**Solution :**
```bash
# Rebuild sans cache
docker-compose build --no-cache

# Redémarrer
docker-compose down
docker-compose up -d
```

### Problème 4 : Worker ne démarre pas

**Symptôme :** dlt_worker en "Restarting" ou "Exit 1"

**Solution :**
```bash
# Voir les logs détaillés
docker-compose logs dlt_worker --tail 100

# Souvent c'est un problème d'import ou de dépendance
# Vérifier que toutes les deps sont installées
docker-compose exec dlt_worker pip list
```

### Problème 5 : Erreur de connexion PostgreSQL

**Symptôme :** "could not connect to server"

**Solution :**
```bash
# Vérifier que postgres est healthy
docker-compose ps dagster_postgres

# Tester la connexion
docker-compose exec dagster_postgres psql -U postgres -d dagster -c "SELECT 1"

# Recréer la DB si nécessaire
docker-compose down
docker volume rm brgm_dagster_pg
docker-compose up -d dagster_postgres
```

## 📊 Commandes Utiles

### Voir tous les logs en temps réel

```bash
docker-compose logs -f
```

### Voir les logs d'un service spécifique

```bash
docker-compose logs -f dagster_webserver
```

### Redémarrer un service

```bash
docker-compose restart dagster_webserver
```

### Reconstruire une image

```bash
docker-compose build dagster_webserver
docker-compose up -d dagster_webserver
```

### Entrer dans un conteneur

```bash
# Entrer dans le webserver
docker-compose exec dagster_webserver bash

# Entrer dans le worker
docker-compose exec dlt_worker bash
```

### Vérifier les variables d'environnement

```bash
docker-compose exec dagster_webserver env | grep DAGSTER
```

## 🧪 Tests de Fonctionnalité

### 1. Interface Web Dagster

1. Ouvrir http://localhost:8080
2. Tu devrais voir le logo Dagster
3. Dans "Deployments", vérifier que "hubeau_pipeline" est listé
4. Cliquer dessus pour voir les assets

### 2. Test d'un Asset Simple

Dans l'UI Dagster :
1. Aller dans "Assets"
2. Chercher un asset simple (ex: `temperature_stations_reference`)
3. Cliquer sur "Materialize"
4. Observer l'exécution dans "Runs"

### 3. Vérifier MinIO

1. Ouvrir http://localhost:9001
2. Login : admin / BrgmMinio2024!
3. Vérifier que le bucket "bronze" existe
4. Après matérialisation d'un asset, vérifier les fichiers parquet

## 🔧 Nettoyage

### Arrêter tous les services

```bash
docker-compose down
```

### Supprimer les volumes (⚠️ PERTE DE DONNÉES)

```bash
docker-compose down -v
```

### Tout supprimer et recommencer

```bash
# Arrêter et supprimer
docker-compose down -v

# Supprimer les images locales
docker rmi hubeau-orchestrator:latest hubeau-worker:latest

# Rebuild complet
docker-compose build --no-cache

# Redémarrer
docker-compose up -d
```

## 📝 Checklist de Test

Utilise cette checklist pour vérifier que tout fonctionne :

- [ ] Docker Desktop est démarré
- [ ] `.env` existe avec les bonnes valeurs
- [ ] `workspace.yaml` existe
- [ ] `docker-compose build` réussit sans erreur
- [ ] `docker-compose up -d` démarre tous les services
- [ ] `docker-compose ps` montre tous les services "Up"
- [ ] http://localhost:8080 affiche l'UI Dagster
- [ ] L'onglet "Deployments" montre "hubeau_pipeline"
- [ ] Les assets sont visibles dans l'onglet "Assets"
- [ ] MinIO est accessible sur http://localhost:9001
- [ ] Le bucket "bronze" existe dans MinIO
- [ ] Un asset peut être matérialisé avec succès
- [ ] Les logs ne montrent pas d'erreurs critiques

## 🆘 Si Rien ne Fonctionne

1. **Capturer les logs complets :**
```bash
docker-compose logs > all_logs.txt
```

2. **Vérifier les ressources Docker :**
```bash
docker system df
```

3. **État détaillé de chaque service :**
```bash
docker-compose ps -a
docker inspect dagster_webserver
```

4. **Partager ces infos pour diagnostic**

---

**Astuce :** Utilise Windows Terminal ou PowerShell pour une meilleure expérience avec les couleurs et le formatage.
