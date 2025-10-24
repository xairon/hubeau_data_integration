# 🎯 Comment Choisir le Mode d'Ingestion (full/year/incremental)

## 📋 La Config Existe Déjà !

Le système **supporte déjà** les 3 modes :
- **FULL** : Tout l'historique
- **YEAR** : Une année spécifique (ex: 2024)
- **INCREMENTAL** : Derniers N jours (par défaut 2)

---

## 🖥️ Méthode 1 : Via Dagster UI Launchpad (Recommandé)

### **Étape 1 : Accéder au Launchpad**

1. Ouvrir Dagster UI : http://srv991054.hstgr.cloud:8080
2. Aller dans **Assets**
3. Sélectionner un asset (ex: `piezometry_chroniques_csv`)
4. Cliquer sur **"Materialize"**
5. En bas, cliquer sur **"Open Launchpad"** ou **"With Config"**

### **Étape 2 : Configurer le Mode**

Dans le Launchpad, vous verrez un éditeur YAML :

#### **Exemple 1 : Mode FULL (tout l'historique)**
```yaml
resources:
  io_manager: {}

ops:
  piezometry_chroniques_csv:
    config:
      mode: "full"
```

#### **Exemple 2 : Mode YEAR (année spécifique)**
```yaml
resources:
  io_manager: {}

ops:
  piezometry_chroniques_csv:
    config:
      mode: "year"
      year: 2024
```

#### **Exemple 3 : Mode INCREMENTAL (derniers jours)**
```yaml
resources:
  io_manager: {}

ops:
  piezometry_chroniques_csv:
    config:
      mode: "incremental"
      incremental_days: 7  # Derniers 7 jours
```

### **Étape 3 : Lancer**

Cliquer sur **"Launch Run"**

---

## 🔧 Méthode 2 : Via CLI Dagster

```bash
# Mode FULL
dagster asset materialize -m hubeau_pipeline \
  --select piezometry_chroniques_csv \
  --config '{"ops": {"piezometry_chroniques_csv": {"config": {"mode": "full"}}}}'

# Mode YEAR (2023)
dagster asset materialize -m hubeau_pipeline \
  --select piezometry_chroniques_csv \
  --config '{"ops": {"piezometry_chroniques_csv": {"config": {"mode": "year", "year": 2023}}}}'

# Mode INCREMENTAL (30 jours)
dagster asset materialize -m hubeau_pipeline \
  --select piezometry_chroniques_csv \
  --config '{"ops": {"piezometry_chroniques_csv": {"config": {"mode": "incremental", "incremental_days": 30}}}}'
```

---

## 📊 Comportement par Défaut

Si vous lancez un asset **sans config** (simple clic "Materialize"), les valeurs par défaut sont :

```python
mode = "full"           # Tout l'historique
year = None             # Pas utilisé
incremental_days = 2    # Pas utilisé en mode full
```

---

## ⚠️ Restrictions par Type de Ressource

### **Ressources avec filtre date** (chroniques, analyses, observations) :
✅ Supportent **FULL**, **YEAR**, **INCREMENTAL**

Exemples :
- `piezometry_chroniques_csv`
- `quality_rivers_analyses_csv`
- `temperature_chroniques_csv`
- `ecoulement_observations_csv`

### **Ressources référentielles** (stations, points) :
✅ Supportent **FULL uniquement** (pas de filtre date disponible)

Exemples :
- `piezometry_stations_csv`
- `quality_rivers_stations_csv`
- `ecoulement_stations_csv`

Si vous essayez YEAR ou INCREMENTAL sur ces ressources, le système passera automatiquement en mode FULL avec un warning.

---

## 🎯 Cas d'Usage Recommandés

### **Première Installation (Base vide)**
```yaml
mode: "full"  # Charger tout l'historique
```

### **Mise à Jour Quotidienne**
```yaml
mode: "incremental"
incremental_days: 1  # Juste hier + aujourd'hui
```

### **Recharger une Année Spécifique**
```yaml
mode: "year"
year: 2023  # Recharger toutes les données de 2023
```

### **Backfill Historique (plusieurs années)**
```bash
# Boucle pour backfill 2020-2024
for year in 2020 2021 2022 2023 2024; do
  dagster asset materialize -m hubeau_pipeline \
    --select piezometry_chroniques_csv \
    --config "{\"ops\": {\"piezometry_chroniques_csv\": {\"config\": {\"mode\": \"year\", \"year\": $year}}}}"
done
```

---

## 🔍 Vérifier le Mode Utilisé

Dans les logs Dagster, vous verrez :

```
🚀 Ingestion: piezometry_chroniques
   Mode: year
   Année: 2024
```

ou

```
🚀 Ingestion: piezometry_chroniques
   Mode: incremental
   Derniers 7 jours
```

---

## 📝 Exemple Complet : Backfill 2023

### Via Dagster UI :

1. Ouvrir `piezometry_chroniques_csv`
2. "Materialize" → "Open Launchpad"
3. Copier-coller :

```yaml
resources:
  io_manager: {}

ops:
  piezometry_chroniques_csv:
    config:
      mode: "year"
      year: 2023
```

4. "Launch Run"
5. Attendre la fin
6. Vérifier les logs : "Mode: year, Année: 2023"

---

## 🎉 Avantages de Cette Approche

✅ **Flexible** : Choisir le mode à chaque run
✅ **Pas de rebuild** : Changement de config sans redéploiement
✅ **Audit trail** : Config visible dans les logs Dagster
✅ **Automatisable** : Via API ou CLI pour scripts

---

## 🐛 Troubleshooting

### **"Je ne vois pas l'option de config dans l'UI"**

→ Assurez-vous d'utiliser **"Open Launchpad"** ou **"Materialize with config"**, pas juste "Materialize"

### **"Mode YEAR ne filtre rien"**

→ Vérifiez que l'asset supporte les filtres date (voir liste ci-dessus)

### **"Erreur: Mode YEAR necessite le parametre 'year'"**

→ Vous avez mis `mode: "year"` mais oublié de spécifier `year: 2024`

---

## 📚 Références

- Config Dagster : https://docs.dagster.io/concepts/configuration/config-schema
- Assets avec Config : https://docs.dagster.io/concepts/assets/software-defined-assets#assets-with-configuration
