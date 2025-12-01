# Migration vers la nouvelle API Copernicus (Décembre 2024)

## Contexte

En décembre 2024, Copernicus Climate Data Store (CDS) a migré vers une nouvelle infrastructure API, rendant l'ancienne API obsolète.

## Changements effectués

### 1. Mise à jour de la bibliothèque cliente

**Avant :**
```python
# pyproject.toml
"cdsapi==0.7.2",
```

**Après :**
```python
# pyproject.toml
# ⚠️ IMPORTANT: Les DEUX packages sont nécessaires
"cads-api-client>=1.0.0",  # Nouveau client recommandé
"cdsapi>=0.7.7",           # Requis par cads-api-client.legacy_api_client
```

**Note :** Le nouveau `cads-api-client` nécessite l'ancien `cdsapi` pour son mode de compatibilité legacy. Les deux doivent être installés.

### 2. Mise à jour de l'URL de l'API

**Avant :**
```yaml
# configs/era5/era5_france_meteo.yml
credentials:
  cds_api_url: https://cds.climate.copernicus.eu/api/v2
  cds_api_key: <API_KEY>
```

**Après :**
```yaml
# configs/era5/era5_france_meteo.yml
credentials:
  cds_api_url: https://cds.climate.copernicus.eu/api
  cds_api_key: <API_KEY>  # Sans préfixe <UID>:
```

### 3. Mise à jour du code source

**Fichier :** `src/hubeau_pipeline/sources/era5_source.py`

Le code a été mis à jour pour supporter les deux API (nouvelle et ancienne) avec fallback automatique :

```python
# Support for both old and new CDS API clients
try:
    # Try new CADS API client first (recommended)
    from cads_api_client.legacy_api_client import LegacyApiClient as Client
    USING_NEW_API = True
except ImportError:
    # Fallback to old cdsapi (deprecated but still works for now)
    import cdsapi
    Client = cdsapi.Client
    USING_NEW_API = False
```

### 4. Passage à ERA5-Land + correction période

**Changements dans `configs/era5/era5_france_meteo.yml` :**

**Dataset :**
- **Avant :** `reanalysis-era5-single-levels` (résolution ~31km)
- **Après :** `reanalysis-era5-land` (résolution ~9km)
- **Raison :** Meilleure résolution spatiale pour variables de surface (précipitations, température, évapotranspiration)

**Période historique :**
- **Avant :** `start_year: 1940`
- **Après :** `start_year: 1950`
- **Raison :** ERA5-Land commence en 1950 (ERA5 standard commence en 1940)

**Format de requête (correction dépréciation) :**
- **Avant :** `'format': 'netcdf'`
- **Après :** `'data_format': 'netcdf'`
- **Raison :** La clé `'format'` est dépréciée dans la nouvelle API

## Format de la clé API

### Ancienne API (deprecated)
```
<UID>:<API_KEY>
```

### Nouvelle API
```
<API_KEY>
```

**⚠️ IMPORTANT :** Supprimez le préfixe `<UID>:` de votre clé API.

## Vérification de la migration

### 1. Vérifier la version de cads-api-client

```bash
docker compose exec dlt_worker pip show cads-api-client
```

**Attendu :** Version >= 1.0.0

### 2. Vérifier la configuration

```bash
cat configs/era5/era5_france_meteo.yml | grep cds_api_url
```

**Attendu :** `https://cds.climate.copernicus.eu/api`

### 3. Tester un téléchargement ERA5

Dans Dagster UI (http://localhost:8080) :
1. Aller dans **Assets**
2. Sélectionner `era5_france_meteo_raw`
3. Cliquer **Materialize**
4. Vérifier les logs pour voir : `Using NEW cads-api-client`

## Troubleshooting

### Erreur : 404 Not Found

**Symptôme :**
```
404 Client Error: Not Found for url: https://cds.climate.copernicus.eu/api/v2/...
```

**Solution :**
- Vérifier que `cds_api_url` est bien `https://cds.climate.copernicus.eu/api` (sans `/v2`)
- Rebuilder l'image Docker : `docker compose build dlt_worker`
- Redémarrer les services : `docker compose up -d --force-recreate dlt_worker`

### Erreur d'authentification

**Symptôme :**
```
401 Unauthorized
```

**Solution :**
- Vérifier que votre clé API ne contient PAS le préfixe `<UID>:`
- Format correct : `REDACTED`
- Format incorrect : `123456:REDACTED`

### Erreur : No module named 'cdsapi'

**Symptôme :**
```
ModuleNotFoundError: No module named 'cdsapi'
```

**Cause :**
Le package `cads-api-client` nécessite `cdsapi` pour son mode legacy, mais `cdsapi` n'est pas installé.

**Solution :**
```bash
# Vérifier que les DEUX packages sont dans pyproject.toml
cat pyproject.toml | grep -A2 "ERA5"

# Attendu :
# "cads-api-client>=1.0.0",
# "cdsapi>=0.7.7",

# Rebuilder
docker compose build dlt_worker
docker compose up -d --force-recreate dlt_worker

# Vérifier l'installation
docker compose exec dlt_worker pip show cdsapi cads-api-client
```

### Ancienne bibliothèque utilisée

**Symptôme :**
Dans les logs : `Using OLD cdsapi (deprecated)`

**Solution :**
```bash
# Rebuilder l'image
docker compose build dlt_worker

# Redémarrer
docker compose up -d --force-recreate dlt_worker

# Vérifier l'installation
docker compose exec dlt_worker pip show cads-api-client
```

### Erreur : MARS returned no data

**Symptôme :**
```
400 Client Error: Bad Request
MultiAdaptorNoDataError: MARS returned no data, please check your selection.
Request submitted to the MARS server:
[{'date': ['1940-01-01', '1940-01-02', ...
```

**Cause :**
Le dataset ERA5-Land ne contient des données qu'à partir de 1950 (pas 1940).

**Solution :**
```yaml
# configs/era5/era5_france_meteo.yml
extraction:
  time_range:
    start_year: 1950  # ERA5-Land commence en 1950
    end_year: null    # null = année courante
```

**Note :** Si vous avez besoin de données avant 1950, utilisez le dataset `reanalysis-era5-single-levels` qui commence en 1940 (mais avec une résolution plus faible : ~31km vs ~9km).

### Warning : 'format' is deprecated

**Symptôme :**
```
The 'format' key for requests is deprecated, please use 'data_format' instead.
```

**Solution :**
Mettre à jour `src/hubeau_pipeline/sources/era5_source.py` :

```python
# Avant
request_params = {
    'format': 'netcdf',
    ...
}

# Après
request_params = {
    'data_format': 'netcdf',
    ...
}
```

## Déploiement

### 1. Local (développement)

```bash
# Rebuilder l'image
docker compose build dlt_worker

# Redémarrer les services
docker compose up -d --force-recreate dlt_worker dagster_webserver dagster_daemon
```

### 2. Production (serveur)

```bash
# Se connecter au serveur
ssh ringuet@dib-2019006065

# Pull les derniers changements
cd /path/to/hubeau_data_integration
git pull

# Rebuilder et redémarrer
docker compose build dlt_worker
docker compose up -d --force-recreate dlt_worker dagster_webserver dagster_daemon
```

## Références

- [CADS API Documentation](https://cds.climate.copernicus.eu/how-to-api)
- [cads-api-client GitHub](https://github.com/ecmwf-projects/cads-api-client)
- [Migration Guide](https://cds.climate.copernicus.eu/how-to-api#migration-from-cdsapi)

## Date de migration

**Date :** 2024-12-01
**Auteur :** Nicolas Ringuet
**Status :** ✅ Complété
