# Configuration des Variables GitLab CI/CD

Ce document explique comment configurer les variables GitLab CI/CD pour le déploiement en production.

## Variables Obligatoires

Accédez à **GitLab > Settings > CI/CD > Variables** et ajoutez les variables suivantes :

### 1. Dagster Orchestration (Métadonnées)

```
DAGSTER_PG_HOST
  Type: Variable
  Value: dagster_postgres
  Protected: Non
  Masked: Non
  Description: Hostname du serveur PostgreSQL pour les métadonnées Dagster

DAGSTER_PG_PORT
  Type: Variable
  Value: 5432
  Protected: Non
  Masked: Non
  Description: Port du serveur PostgreSQL Dagster

DAGSTER_PG_PASSWORD
  Type: Variable
  Value: <générer un mot de passe fort>
  Protected: Oui
  Masked: Oui
  Description: Mot de passe PostgreSQL pour Dagster (20+ caractères recommandés)
```

### 2. Data Storage (Données Hub'Eau)

```
PG_HOST
  Type: Variable
  Value: postgres
  Protected: Non
  Masked: Non
  Description: Hostname du serveur PostgreSQL pour les données

PG_PASSWORD
  Type: Variable
  Value: <générer un mot de passe fort différent>
  Protected: Oui
  Masked: Oui
  Description: Mot de passe PostgreSQL pour les données

POSTGIS_HOST
  Type: Variable
  Value: postgis
  Protected: Non
  Masked: Non
  Description: Hostname du serveur PostGIS
```

### 3. Object Storage MinIO (Bronze Layer)

```
MINIO_ENDPOINT
  Type: Variable
  Value: http://minio:9000
  Protected: Non
  Masked: Non
  Description: URL du serveur MinIO (interne au réseau Docker)

MINIO_USER
  Type: Variable
  Value: admin
  Protected: Non
  Masked: Non
  Description: Utilisateur root MinIO

MINIO_PASS
  Type: Variable
  Value: <générer un mot de passe fort différent>
  Protected: Oui
  Masked: Oui
  Description: Mot de passe MinIO (20+ caractères recommandés)

MINIO_REGION
  Type: Variable
  Value: us-east-1
  Protected: Non
  Masked: Non
  Description: Région AWS (S3-compatible)

MINIO_BRONZE_BUCKET
  Type: Variable
  Value: bronze
  Protected: Non
  Masked: Non
  Description: Nom du bucket pour la Bronze Layer
```

## Génération de Mots de Passe Sécurisés

### Linux/Mac
```bash
openssl rand -base64 32
```

### Python
```python
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### PowerShell (Windows)
```powershell
-join((48..57)+(65..90)+(97..122)|Get-Random -Count 32|%{[char]$_})
```

## Vérification

Une fois les variables configurées, le pipeline GitLab effectuera des vérifications automatiques :

1. Le stage `deploy:production` vérifie que `DAGSTER_PG_PASSWORD`, `MINIO_PASS` et `PG_PASSWORD` sont définies
2. Si une variable manque, le pipeline échouera avec un message d'erreur explicite
3. Si toutes les variables sont présentes, le déploiement continue normalement

## Bonnes Pratiques

**À FAIRE** :
- ✅ Utiliser des mots de passe de 20+ caractères
- ✅ Masquer toutes les variables sensibles (passwords)
- ✅ Protéger les variables de production
- ✅ Utiliser des mots de passe différents pour chaque service
- ✅ Effectuer une rotation des mots de passe tous les 6 mois

**À NE PAS FAIRE** :
- ❌ Utiliser des mots de passe simples (admin, password123, etc.)
- ❌ Réutiliser les mêmes mots de passe entre environnements
- ❌ Commit des fichiers `.env` dans Git
- ❌ Logger les mots de passe en clair dans les logs

## Dépannage

### Erreur: "password authentication failed for user postgres"

**Cause** : La variable `DAGSTER_PG_PASSWORD` n'est pas définie ou est vide dans GitLab CI/CD Variables.

**Solution** :
1. Vérifiez que la variable `DAGSTER_PG_PASSWORD` existe dans **GitLab > Settings > CI/CD > Variables**
2. Vérifiez que la variable n'est pas vide
3. Vérifiez que la variable est bien **Masked** et **Protected**
4. Relancez le pipeline GitLab

### Erreur: "Variable not found"

**Cause** : Une variable obligatoire n'est pas définie.

**Solution** : Le pipeline affichera exactement quelle variable manque. Ajoutez-la dans GitLab CI/CD Variables.

### Comment tester la configuration localement ?

Pour tester la configuration localement avant de déployer en production :

```bash
# Copier le template
cp .env.template .env

# Éditer .env avec vos valeurs
nano .env

# Tester avec Docker Compose local
docker compose up -d

# Vérifier les logs
docker compose logs dagster_webserver
```

## Variables par Environnement

| Variable | Local (.env) | Production (GitLab CI/CD) |
|----------|--------------|---------------------------|
| DAGSTER_PG_HOST | localhost ou dagster_postgres | dagster_postgres |
| DAGSTER_PG_PASSWORD | dev_password | <mot de passe fort> |
| MINIO_ENDPOINT | http://localhost:9000 | http://minio:9000 |
| MINIO_PASS | minioadmin | <mot de passe fort> |
| PG_PASSWORD | postgres | <mot de passe fort> |

**Note** : En production, utilisez toujours des mots de passe forts et différents pour chaque service.

---

**Référence** : [Variables GitLab CI/CD](cicd-variables) | [Pipeline GitLab](cicd-pipeline)
