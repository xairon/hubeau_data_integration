# Variables GitLab CI/CD à Configurer

## ✅ Variables DÉJÀ configurées (NE PAS MODIFIER)

Ces variables existent déjà dans GitLab :

```
DAGSTER_PG_PASSWORD = BrgmDagster2024
MINIO_PASS = BrgmMinio2024
MINIO_USER = admin
PG_PASSWORD = BrgmPostgres2024
```

## 🆕 Nouvelles variables à AJOUTER

Va dans **Settings > CI/CD > Variables** et clique sur **"Add variable"** pour chacune :

---

### Étape 1 : Ajouter DAGSTER_PG_HOST

- **Key** : `DAGSTER_PG_HOST`
- **Value** : `dagster_postgres`
- **Type** : Variable
- **Environment scope** : All (default)
- **Protect variable** : ✅ OUI
- **Mask variable** : ❌ NON

---

### Étape 2 : Ajouter PG_HOST

- **Key** : `PG_HOST`
- **Value** : `postgres`
- **Type** : Variable
- **Environment scope** : All (default)
- **Protect variable** : ✅ OUI
- **Mask variable** : ❌ NON

---

### Étape 3 : Ajouter POSTGIS_HOST

- **Key** : `POSTGIS_HOST`
- **Value** : `postgis`
- **Type** : Variable
- **Environment scope** : All (default)
- **Protect variable** : ✅ OUI
- **Mask variable** : ❌ NON

---

### Étape 4 : Ajouter MINIO_ENDPOINT

- **Key** : `MINIO_ENDPOINT`
- **Value** : `http://minio:9000`
- **Type** : Variable
- **Environment scope** : All (default)
- **Protect variable** : ✅ OUI
- **Mask variable** : ❌ NON

---

### Étape 5 : Ajouter MINIO_REGION

- **Key** : `MINIO_REGION`
- **Value** : `us-east-1`
- **Type** : Variable
- **Environment scope** : All (default)
- **Protect variable** : ✅ OUI
- **Mask variable** : ❌ NON

---

### Étape 6 : Ajouter MINIO_BRONZE_BUCKET

- **Key** : `MINIO_BRONZE_BUCKET`
- **Value** : `bronze`
- **Type** : Variable
- **Environment scope** : All (default)
- **Protect variable** : ✅ OUI
- **Mask variable** : ❌ NON

---

## 📝 Résumé des 6 variables à ajouter

| Variable | Valeur | Protected | Masked |
|----------|--------|-----------|--------|
| `DAGSTER_PG_HOST` | `dagster_postgres` | ✅ | ❌ |
| `PG_HOST` | `postgres` | ✅ | ❌ |
| `POSTGIS_HOST` | `postgis` | ✅ | ❌ |
| `MINIO_ENDPOINT` | `http://minio:9000` | ✅ | ❌ |
| `MINIO_REGION` | `us-east-1` | ✅ | ❌ |
| `MINIO_BRONZE_BUCKET` | `bronze` | ✅ | ❌ |

## 🔍 Vérification finale

Après avoir ajouté les 6 variables, tu devrais avoir **10 variables au total** :

```
✅ DAGSTER_PG_HOST = dagster_postgres
✅ DAGSTER_PG_PASSWORD = ******** (masqué)
✅ MINIO_BRONZE_BUCKET = bronze
✅ MINIO_ENDPOINT = http://minio:9000
✅ MINIO_PASS = ******** (masqué)
✅ MINIO_REGION = us-east-1
✅ MINIO_USER = admin
✅ PG_HOST = postgres
✅ PG_PASSWORD = ******** (masqué)
✅ POSTGIS_HOST = postgis
```

## 💡 Pourquoi ces variables ?

Ces variables permettent de :
- **Pointer vers des bases de données externes** (AWS RDS, Scaleway DB, etc.)
- **Utiliser S3 au lieu de MinIO local** (AWS S3, Scaleway Object Storage)
- **Configurer différents environnements** (dev/staging/prod) sans changer le code

### Exemples d'utilisation future

**Utiliser AWS S3 au lieu de MinIO local** :
```
MINIO_ENDPOINT = https://s3.eu-west-3.amazonaws.com
MINIO_USER = AKIAIOSFODNN7EXAMPLE
MINIO_PASS = wJalrXUtnFEMI/K7MDENG/...
MINIO_REGION = eu-west-3
MINIO_BRONZE_BUCKET = hubeau-bronze-prod
```

**Utiliser PostgreSQL managé (AWS RDS)** :
```
PG_HOST = hubeau-postgres.xyz.eu-west-3.rds.amazonaws.com
PG_PASSWORD = <rds-password>
```

Pour l'instant, on garde les valeurs par défaut (services Docker locaux).

---

## 🚀 Prêt à déployer !

Une fois les 6 variables ajoutées dans GitLab, tu peux push et le pipeline déploiera automatiquement.
