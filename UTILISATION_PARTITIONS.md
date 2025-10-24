# 🎯 Utilisation des Partitions Hub'Eau

## 📋 Qu'est-ce qu'une Partition ?

Les partitions permettent de **sélectionner visuellement** le mode d'ingestion directement dans Dagster UI, sans avoir à éditer du YAML.

---

## 🖥️ Interface Dagster avec Partitions

### **Nouvelle Interface (avec partitions)** :

```
┌──────────────────────────────────────────────────────────┐
│  piezometry_chroniques_csv                                │
├──────────────────────────────────────────────────────────┤
│  Partitions:  ▼ Static Partition                         │
│                                                            │
│  ┌──────┬──────────────┬──────┬──────┬──────┬──────┐    │
│  │ full │ incremental │ 2024 │ 2023 │ 2022 │ 2021 │    │
│  └──────┴──────────────┴──────┴──────┴──────┴──────┘    │
│                                                            │
│  [Materialize selected partition]                         │
└──────────────────────────────────────────────────────────┘
```

---

## 🎯 Comment Utiliser les Partitions

### **Méthode 1 : Cliquer sur la Partition** (Le plus simple)

1. Ouvrir Dagster UI
2. Aller dans **Assets**
3. Cliquer sur un asset partitionné (ex: `piezometry_chroniques_csv`)
4. Vous verrez les partitions disponibles : `full | incremental | 2024 | 2023 | 2022 | 2021 | 2020`
5. **Cliquer sur la partition désirée** (ex: "2023")
6. Cliquer **"Materialize selected partition"**

**C'est tout !** 🎉

---

## 📊 Partitions Disponibles

### **Partitions Statiques (Pré-configurées)** :

| Partition | Mode | Description |
|-----------|------|-------------|
| `full` | FULL | Tout l'historique disponible |
| `incremental` | INCREMENTAL | Derniers 2 jours (configurable) |
| `2024` | YEAR | Année 2024 uniquement |
| `2023` | YEAR | Année 2023 uniquement |
| `2022` | YEAR | Année 2022 uniquement |
| `2021` | YEAR | Année 2021 uniquement |
| `2020` | YEAR | Année 2020 uniquement |

---

## ➕ Ajouter une Année Personnalisée

### **Option 1 : Via API Python** (Recommandé)

```python
from dagster import DagsterInstance

# Se connecter à l'instance Dagster
instance = DagsterInstance.get()

# Ajouter l'année 2019
instance.add_dynamic_partitions(
    partitions_def_name="hubeau_time_partitions",
    partition_keys=["2019"]
)

# Ou ajouter plusieurs années d'un coup
instance.add_dynamic_partitions(
    partitions_def_name="hubeau_time_partitions",
    partition_keys=["2015", "2016", "2017", "2018", "2019"]
)
```

### **Option 2 : Via Dagster UI** (Si disponible dans votre version)

1. Aller dans **Deployment** → **Partitions**
2. Chercher `hubeau_time_partitions`
3. Cliquer **"Add partition"**
4. Entrer l'année (ex: `2019`)
5. Valider

### **Option 3 : Modifier le Code** (Moins flexible)

Éditer `csv_assets.py` ligne 42 et ajouter l'année :

```python
MODE_PARTITIONS = StaticPartitionsDefinition([
    "full",
    "incremental",
    "2024",
    "2023",
    "2022",
    "2021",
    "2020",
    "2019",  # ← Ajouter ici
])
```

Puis redéployer.

---

## 🔍 Vérifier la Partition Utilisée

Dans les logs Dagster, vous verrez :

```
📋 Partition sélectionnée: 2023
🚀 Ingestion: piezometry_chroniques
   Mode: year
   Année: 2023
```

---

## 📅 Cas d'Usage Recommandés

### **1. Première Installation (Base Vide)**

**Action** : Cliquer sur partition `full`

→ Charge tout l'historique disponible

---

### **2. Mise à Jour Quotidienne**

**Action** : Cliquer sur partition `incremental`

→ Charge seulement les derniers 2 jours

**💡 Conseil** : Créer un Schedule Dagster pour automatiser :
```python
@schedule(cron_schedule="0 2 * * *", job=...)
def daily_incremental():
    return RunRequest(partition_key="incremental")
```

---

### **3. Backfill d'une Année Spécifique**

**Action** : Cliquer sur partition `2022`

→ Charge uniquement l'année 2022

---

### **4. Backfill Multi-Années**

**Action** : Sélectionner plusieurs partitions (Maj+Clic)

Dagster UI permet de sélectionner `2020 | 2021 | 2022 | 2023`

→ Lancera 4 jobs en parallèle (un par année)

---

## ⚙️ Personnaliser incremental_days

Si vous voulez charger les **7 derniers jours** au lieu de 2 :

### **Option A : Via Launchpad** (Config YAML)

1. Cliquer sur partition `incremental`
2. Avant de lancer, cliquer **"Open Launchpad"**
3. Modifier :

```yaml
ops:
  piezometry_chroniques_csv:
    config:
      incremental_days: 7  # Au lieu de 2
```

### **Option B : Créer une Nouvelle Partition**

Éditer `csv_assets.py` ligne 42 :

```python
MODE_PARTITIONS = StaticPartitionsDefinition([
    "full",
    "incremental",      # 2 jours
    "incremental_7d",   # ← Ajouter 7 jours
    "2024",
    "2023",
    # ...
])
```

Puis dans la logique (ligne 120) :

```python
elif partition == "incremental":
    config.mode = "incremental"
    config.incremental_days = 2
elif partition == "incremental_7d":  # ← Ajouter ici
    config.mode = "incremental"
    config.incremental_days = 7
```

---

## 🎨 Visualisation de l'Historique des Partitions

Dagster UI montre :

```
Partition Status:
  ┌───────┬──────────────┬──────┬──────┬──────┬──────┐
  │ full  │ incremental  │ 2024 │ 2023 │ 2022 │ 2021 │
  ├───────┼──────────────┼──────┼──────┼──────┼──────┤
  │   ✅  │      ✅      │  ✅  │  ✅  │  ⚠️  │  ❌  │
  └───────┴──────────────┴──────┴──────┴──────┴──────┘

  ✅ = Réussi
  ⚠️ = Partiel/Erreur
  ❌ = Jamais lancé
```

**Utile pour voir** quelles années ont déjà été chargées !

---

## 🚫 Assets SANS Partitions

Les **référentiels** (stations, points) **n'ont PAS de partitions** car ils ne supportent pas les filtres temporels.

Exemples :
- `piezometry_stations_csv` → Pas de partitions (juste "Materialize")
- `quality_rivers_stations_csv` → Pas de partitions
- `ecoulement_stations_csv` → Pas de partitions

Pour ces assets, un simple clic sur **"Materialize"** charge toutes les données.

---

## 🔧 Troubleshooting

### **"Je ne vois pas les partitions dans l'UI"**

→ Vous êtes peut-être sur un asset **sans** partitions (référentiel)

→ Vérifiez que vous regardez bien un asset de type **chroniques/analyses/observations**

### **"La partition 2019 n'apparaît pas"**

→ Vérifiez que vous l'avez ajoutée (voir section "Ajouter une Année")

→ Rechargez la page Dagster UI

### **"Erreur : Partition invalide"**

→ Vous avez peut-être entré une chaîne non-numérique (ex: "twenty-twenty")

→ Les années doivent être au format `YYYY` (ex: `2019`)

---

## 📚 Références

- Partitions Dagster : https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions
- Dynamic Partitions : https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions#dynamic-partitions
- Backfills : https://docs.dagster.io/concepts/partitions-schedules-sensors/backfills

---

## 🎉 Avantages des Partitions

✅ **Visuel** : Clic sur un bouton au lieu de YAML
✅ **Historique** : Voir quelles partitions ont déjà été exécutées
✅ **Backfills** : Sélection multiple pour relancer plusieurs années
✅ **Automatisation** : Integration avec Schedules et Sensors
✅ **Flexible** : Possibilité d'ajouter des partitions dynamiquement
