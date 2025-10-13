# 🚀 Démarrage Rapide - Tests Locaux Sans Docker

Guide pratique pour tester le wrapper Hub'Eau en 2-5 minutes, **sans Docker**.

---

## 📦 Docker : Obligatoire ou Non ?

### ❌ Vous N'AVEZ PAS Besoin de Docker Pour :

- ✅ Tester les APIs Hub'Eau
- ✅ Extraire des données (piézométrie, hydrométrie, qualité, etc.)
- ✅ Exporter en CSV, Parquet
- ✅ Utiliser le wrapper Python `HubeauClient`
- ✅ Charger dans DuckDB local
- ✅ Exécuter le notebook Jupyter
- ✅ Utiliser le CLI

### ⚠️ Vous AVEZ Besoin de Docker Pour :

- Dagster (orchestration et scheduling)
- PostgreSQL/PostGIS (stockage relationnel)
- MinIO (stockage S3-compatible)
- Déploiement en production

**Conclusion** : Pour vos tests et développement initial, **Docker n'est pas nécessaire**.

---

## ⚡ Installation Rapide (2 minutes)

### Prérequis

- Python 3.11 ou supérieur
- pip
- Connexion internet (pour accéder aux APIs Hub'Eau)

### Installation

```bash
# 1. Cloner le dépôt (si pas déjà fait)
git clone <repo-url>
cd brgm

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. (Optionnel) Créer un environnement virtuel
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# 4. Installer les packages locaux
pip install -e .
```

**Durée** : ~2 minutes

---

## 🎯 Méthode 1 : Script Python Simple (RECOMMANDÉ pour débuter)

### Exécution

```bash
python test_local_simple.py
```

### Ce que fait le script

1. Teste 3 APIs Hub'Eau (piézométrie, température, hydrométrie)
2. Extrait 10 stations de chaque API
3. Exporte en CSV dans `data/local_tests/`
4. Affiche un résumé dans le terminal

### Résultat attendu

```
================================================================================
TEST LOCAL HUB'EAU - Sans Docker
================================================================================

[OK] Piezometry stations    : 10 records → piezometry_stations.csv
[OK] Temperature stations   : 10 records → temperature_stations.csv
[OK] Hydrometry stations    : 10 records → hydrometry_stations.csv

================================================================================
Tous les fichiers exportes dans: E:\brgm\data\local_tests
================================================================================
```

**Durée** : ~10-15 secondes

---

## 📓 Méthode 2 : Notebook Jupyter (RECOMMANDÉ pour exploration)

### Exécution

```bash
# Lancer Jupyter
jupyter notebook notebooks/test_hubeau_wrapper.ipynb

# OU avec JupyterLab
jupyter lab notebooks/test_hubeau_wrapper.ipynb
```

### Ce que fait le notebook

- Teste **8 APIs Hub'Eau** complètes
- Extrait données sur **7 derniers jours** pour chroniques
- Exporte CSV + Parquet
- Crée **visualisations** (cartes des stations)
- Teste pipeline **DuckDB**

### Exports générés

Tous dans `data/test_exports/` :
- 9 fichiers CSV (~50-500 KB chacun)
- 1 fichier Parquet
- 1 carte PNG (localisation stations)
- 1 base DuckDB locale

**Durée** : ~5-10 minutes (17 cellules)

---

## ⌨️ Méthode 3 : CLI (RECOMMANDÉ pour automatisation)

### Commandes disponibles

#### 1. Lister les APIs disponibles

```bash
python -m hubeau.cli list-apis
```

**Sortie** :
```
APIs Hub'Eau disponibles:
  1. ecoulement
  2. hydrobio
  3. hydrometry
  4. piezometry
  5. prelevements
  6. quality_groundwater
  7. quality_rivers
  8. temperature
```

#### 2. Lister les endpoints d'une API

```bash
python -m hubeau.cli list-endpoints piezometry
```

**Sortie** :
```
Endpoints pour 'piezometry':
  - stations
  - chroniques
  - chroniques_historical
```

#### 3. Tester la connectivité

```bash
python -m hubeau.cli test-connectivity
```

**Sortie** :
```
Test de connectivité des 23 endpoints...
[OK] piezometry/stations
[OK] temperature/stations
...
Taux de succès: 100% (23/23)
```

#### 4. Extraire des données

```bash
# Extraire 10 stations (affichage JSON)
python -m hubeau.cli get-data piezometry stations --limit 10

# Extraire et exporter en CSV
python -m hubeau.cli get-data piezometry stations --limit 100 --export csv --output ./data/

# Extraire avec paramètres
python -m hubeau.cli get-data ecoulement stations --limit 10 --params '{"code_departement": "75"}'
```

---

## 🐍 Méthode 4 : Code Python Direct

### Exemple 1 : Client Simple

```python
from hubeau import HubeauClient

# Initialiser le client
client = HubeauClient()

# Extraire des données
data = client.get_data(
    api="piezometry",
    endpoint="stations",
    limit=10
)

# Afficher
for record in data:
    print(record)
```

### Exemple 2 : Pipeline avec Export

```python
from hubeau import HubeauPipeline
import pandas as pd

# Créer pipeline DuckDB local
pipeline = HubeauPipeline(
    destination="duckdb",
    dataset_name="test_local"
)

# Charger des données
result = pipeline.load(
    api="piezometry",
    endpoint="stations",
    params={"size": 50}
)

print(f"Pipeline: {result}")
```

### Exemple 3 : Export CSV Personnalisé

```python
from hubeau import HubeauClient
import pandas as pd

client = HubeauClient()

# Extraire
data = list(client.get_data("temperature", "stations", limit=100))

# Convertir en DataFrame
df = pd.DataFrame(data)

# Exporter
df.to_csv("mes_stations_temperature.csv", index=False)
print(f"Exporté {len(df)} stations")
```

---

## 📊 Que Faire Avec les Données ?

### 1. Analyse avec Pandas

```python
import pandas as pd

df = pd.read_csv("data/local_tests/piezometry_stations.csv")

print(f"Nombre de stations: {len(df)}")
print(f"Colonnes: {list(df.columns)}")
print(df.describe())
```

### 2. Visualisation

```python
import matplotlib.pyplot as plt

# Carte des stations
plt.scatter(df['longitude'], df['latitude'])
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('Stations Piézométriques')
plt.show()
```

### 3. Requêtes DuckDB

```python
import duckdb

con = duckdb.connect("data/hubeau.duckdb")
result = con.execute("SELECT * FROM piezometry_stations LIMIT 10").df()
print(result)
```

---

## 🐛 Dépannage

### Erreur : ModuleNotFoundError: No module named 'hubeau'

**Solution** :
```bash
# Installer en mode éditable
pip install -e .

# OU ajouter src/ au path Python
import sys
sys.path.insert(0, 'src')
```

### Erreur : Timeout API

**Cause** : Connexion internet lente ou API Hub'Eau temporairement indisponible

**Solution** :
- Vérifier votre connexion internet
- Réessayer dans quelques minutes
- Tester avec un autre endpoint

### Erreur : UnicodeEncodeError (Windows)

**Cause** : Problème d'encodage console Windows

**Solution** :
```bash
# Avant d'exécuter, configurer UTF-8
chcp 65001
python test_local_simple.py
```

### Erreur : Permission denied (Linux/Mac)

**Solution** :
```bash
# Donner permissions d'exécution
chmod +x test_local_simple.py
```

---

## 🚀 Prochaines Étapes

### Tests Réussis ✅

Vous êtes prêt à :
1. **Développer** : Créer vos propres scripts d'extraction
2. **Intégrer** : Connecter à vos outils d'analyse
3. **Déployer** : Passer à Docker pour la production

**Guide Docker** : Voir [README.md](../README.md) section "Installation"

### Tests Échoués ❌

1. Vérifier les prérequis (Python 3.11+, pip, internet)
2. Consulter la section Dépannage ci-dessus
3. Vérifier les logs dans `logs/`
4. Ouvrir une issue sur GitLab

---

## 📚 Ressources

- [Documentation Hub'Eau](HUBEAU_USAGE.md)
- [Mapping des Endpoints](../API_ENDPOINTS_MAPPING.md)
- [APIs Hub'Eau Référence](APIS_HUBEAU_REFERENCE_COMPLETE.md)
- [Notebook de Test](../notebooks/README.md)

---

## 🤝 Support

- **Issues** : GitLab du projet
- **Documentation** : `docs/`
- **Exemples** : `notebooks/`