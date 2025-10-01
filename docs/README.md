# Documentation du Projet BRGM Hub'Eau

**Structure documentaire organisée et à jour**

---

## 📚 Documentation par Thématique

### 🌊 Pipeline Hub'Eau (Ingestion Bronze)

**[HUBEAU_PIPELINE.md](HUBEAU_PIPELINE.md)** - Documentation complète Hub'Eau
- Architecture des partitions (quotidiennes/annuelles/non-partitionnées)
- Configuration des 8 APIs
- Optimisations critiques (sémaphore global, fenêtres temporelles)
- Jobs et schedules
- Mode d'emploi et troubleshooting

### 🏗️ Architecture Globale

**[ARCHITECTURE_MODERNE.md](ARCHITECTURE_MODERNE.md)** - Stack technique
- Infrastructure Docker (Dagster, MinIO, TimescaleDB, PostGIS, Neo4j)
- Choix technologiques justifiés
- Architecture Medallion (Bronze/Silver/Gold)

### 📊 Données

**[DATA_SOURCES_COMPLETE.md](DATA_SOURCES_COMPLETE.md)** - Sources de données
- Liste complète des APIs intégrées
- Fréquences de mise à jour
- Volumes de données

**[DATA_STORAGE_STRATEGY.md](DATA_STORAGE_STRATEGY.md)** - Stratégie de stockage
- Bronze : MinIO (JSON)
- Silver : TimescaleDB, PostGIS, Neo4j
- Gold : Knowledge Graph SOSA

### 🔮 Vision et Futur

**[SOSA_FUTURE_VISION.md](SOSA_FUTURE_VISION.md)** - Ontologie SOSA
- Modélisation sémantique
- Graphe de connaissances
- Standards W3C

### 🔍 Qualité

**[CODE_REVIEW.md](CODE_REVIEW.md)** - Code review
- Bonnes pratiques
- Points d'attention
- Recommandations

---

## 🗂️ Structure Recommandée

```
docs/
├── README.md                     ← Vous êtes ici (Index)
├── HUBEAU_PIPELINE.md           ← ⭐ DOC PRINCIPALE Hub'Eau
├── ARCHITECTURE_MODERNE.md       ← Stack technique
├── DATA_SOURCES_COMPLETE.md      ← Sources données
├── DATA_STORAGE_STRATEGY.md      ← Stratégie stockage
├── SOSA_FUTURE_VISION.md         ← Vision ontologie
└── CODE_REVIEW.md                ← Qualité code
```

---

## 🚀 Quick Start

**Nouveau sur le projet ?** Lire dans cet ordre :

1. **[README.md](../README.md)** (racine) - Vue d'ensemble et démarrage rapide
2. **[HUBEAU_PIPELINE.md](HUBEAU_PIPELINE.md)** - Pipeline Hub'Eau détaillé
3. **[ARCHITECTURE_MODERNE.md](ARCHITECTURE_MODERNE.md)** - Stack technique

**Utilisation quotidienne ?** 

→ **[HUBEAU_PIPELINE.md](HUBEAU_PIPELINE.md)** - Votre référence principale

---

## 🔄 Maintenance Documentation

**Règles** :
- ✅ **1 document par thématique** (pas de duplication)
- ✅ **Mise à jour immédiate** après changements architecture
- ✅ **Exemples concrets** et testés
- ✅ **Dates de dernière mise à jour** visibles

**Dernière refonte complète** : 1er octobre 2025

