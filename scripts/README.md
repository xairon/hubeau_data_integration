# Scripts de Production

## 📋 Déploiement avec GitLab Runner

### Configuration GitLab Runner

Le projet se déploie automatiquement via **GitLab CI/CD** (.gitlab-ci.yml) :

1. **Sur push vers `main`** : Build + déploiement automatique
2. **Variables secrets** : Configurées dans GitLab (Settings > CI/CD > Variables)
3. **Runner** : Doit avoir accès Docker et aux volumes de données

### Variables Secrets Requises

Configurer dans **GitLab > Settings > CI/CD > Variables** :

```
DAGSTER_PG_PASSWORD     # Mot de passe PostgreSQL Dagster
MINIO_USER              # Utilisateur MinIO
MINIO_PASS              # Mot de passe MinIO
```

Marquer comme **Protected** et **Masked**.

### Flux de Déploiement

```
1. git push origin main
   ↓
2. GitLab CI/CD déclenché
   ↓
3. Build image Docker
   ↓
4. Génération .env.production depuis secrets GitLab
   ↓
5. Déploiement sur serveur cible
   ↓
6. Health checks
   ↓
7. Services opérationnels
```

### Configuration du Runner

Le runner GitLab doit avoir :
- ✅ Docker installé
- ✅ Accès aux volumes persistants
- ✅ Tag `hubeau` (défini dans .gitlab-ci.yml)

Voir `.gitlab-ci.yml` pour les détails de configuration.

---

## 🔐 Variables d'Environnement

### Développement Local

```bash
# Copier le template
cp env.example .env

# Éditer avec vos valeurs
nano .env

# Démarrer
docker-compose up -d
```

### Production (GitLab CI/CD)

Le fichier `.env.production` est **généré automatiquement** par le pipeline GitLab depuis les secrets configurés.

**Pas de fichier .env à gérer manuellement en production !**
