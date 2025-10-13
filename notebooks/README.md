# Notebooks de Test Hub'Eau

Ce répertoire contient des notebooks Jupyter pour tester et démontrer les fonctionnalités du wrapper Hub'Eau.

## 📓 Notebooks Disponibles

### test_hubeau_wrapper.ipynb

Notebook complet de test du wrapper Hub'Eau avec extraction de données réelles.

**Contenu** :
- Test de 8 APIs Hub'Eau
- Extraction de données sur courtes périodes
- Export CSV et Parquet
- Test du pipeline DuckDB
- Visualisations basiques (carte des stations)

**Données testées** :
- Piézométrie : Stations + Chroniques (7 derniers jours)
- Température : Stations
- Écoulement : Stations (département 75 - Paris)
- Hydrométrie : Stations (département 69 - Rhône)
- Qualité Nappes : Stations
- Qualité Rivières : Stations
- Hydrobiologie : Stations
- Prélèvements : Ouvrages

## 🚀 Prérequis

```bash
pip install jupyter pandas matplotlib
```

## ▶️ Exécution

### Méthode 1 : Jupyter Notebook

```bash
cd notebooks
jupyter notebook test_hubeau_wrapper.ipynb
```

### Méthode 2 : JupyterLab

```bash
cd notebooks
jupyter lab test_hubeau_wrapper.ipynb
```

### Méthode 3 : VS Code

Ouvrir le fichier `.ipynb` avec l'extension Jupyter de VS Code.

## 📁 Exports Générés

Tous les exports sont sauvegardés dans `../data/test_exports/` :

**CSV** :
- `piezometry_stations.csv`
- `piezometry_chroniques.csv`
- `temperature_stations.csv`
- `ecoulement_stations.csv`
- `hydrometry_stations.csv`
- `quality_groundwater_stations.csv`
- `quality_rivers_stations.csv`
- `hydrobio_stations.csv`
- `prelevements_ouvrages.csv`

**Parquet** :
- `piezometry_stations.parquet`

**Visualisations** :
- `map_piezometry_stations.png`

## 🔧 Utilisation Avancée

### Personnaliser les Périodes de Test

```python
# Dans le notebook, modifier les dates :
date_fin = datetime.now()
date_debut = date_fin - timedelta(days=30)  # 30 jours au lieu de 7
```

### Tester d'Autres Endpoints

```python
# Ajouter une nouvelle cellule :
data = client.get_data(
    api="ecoulement",
    endpoint="observations",
    params={"code_station": "A1234567"},
    limit=50
)
```

### Charger vers PostgreSQL

```python
# Si PostgreSQL est configuré :
pipeline = HubeauPipeline(
    destination="postgres",
    dataset_name="test_notebook"
)

result = pipeline.load(
    api="piezometry",
    endpoint="stations",
    params={"size": 100}
)
```

## 📊 Exemples de Résultats

Après exécution complète, vous aurez :
- **~9 fichiers CSV** (~50-500 KB chacun)
- **1 fichier Parquet** (plus compact)
- **1 carte PNG** des stations piézométriques
- **1 base DuckDB** locale avec données chargées

## 🐛 Dépannage

### Erreur d'Import

```python
# Si erreur "ModuleNotFoundError: No module named 'hubeau'"
# Vérifier que src/ est bien ajouté au path :
import sys
sys.path.insert(0, '../src')
```

### Timeout API

Si l'API ne répond pas :
- Vérifier la connectivité internet
- Essayer avec un autre endpoint
- Augmenter le timeout dans le client

### Problèmes d'Encodage

Sur Windows, si erreur d'encodage :
```python
# Forcer UTF-8 pour les exports CSV
df.to_csv(path, index=False, encoding='utf-8-sig')
```

## 📚 Ressources

- [Documentation Hub'Eau](docs/HUBEAU_USAGE.md)
- [Mapping des Endpoints](../API_ENDPOINTS_MAPPING.md)
- [Documentation APIs Hub'Eau](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)

## 🤝 Contribution

Pour ajouter un nouveau notebook :
1. Créer le fichier `.ipynb` dans ce répertoire
2. Documenter dans ce README
3. Ajouter les exports dans `.gitignore` si volumineux
