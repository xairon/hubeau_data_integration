# 🧭 Guide de Navigation - Documentation JUNON Hub'Eau

Guide rapide pour trouver l'information dont vous avez besoin.

---

## 🎯 Questions Fréquentes → Documentation

### "C'est quoi ce projet ?"
👉 **[README.md](README.md)** - Contexte JUNON + installation rapide

### "C'est quoi JUNON ?"
👉 **[PROJET_JUNON_VISION.md](docs/PROJET_JUNON_VISION.md)** - Programme BRGM 12,3M€, jumeau numérique

### "Quelles données on intègre ?"
👉 **[APIS_HUBEAU_REFERENCE_COMPLETE.md](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)** - 8 APIs, 23 endpoints, 778 attributs

### "Comment je configure un endpoint ?"
👉 **[TUTORIEL_DLT.md](docs/TUTORIEL_DLT.md)** - Guide YAML, slicing, optimisations

### "Quelle est l'architecture technique ?"
👉 **[ARCHITECTURE_MODERNE.md](docs/ARCHITECTURE_MODERNE.md)** - Stack, état implémentation, roadmap

### "C'est quoi SANDRE, BDLISA ?"
👉 **[AUTRES_REFERENTIELS.md](docs/AUTRES_REFERENTIELS.md)** - 21 référentiels expliqués

### "Comment je déploie en production ?"
👉 **[scripts/README.md](scripts/README.md)** - GitLab CI/CD, configuration runner

### "Où trouver un schéma spécifique ?"
👉 **[APIS_HUBEAU_REFERENCE_COMPLETE.md](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)** - Table des matières détaillée

---

## 📂 Structure de Documentation

```
📁 Racine
├── README.md                    ⭐ POINT D'ENTRÉE - Contexte + Quick Start
├── NAVIGATION.md                🧭 Ce fichier - Guide de navigation
└── env.example                  🔐 Template variables (dev local)

📁 docs/
├── README.md                    📖 Index documentation
├── APIS_HUBEAU_REFERENCE_COMPLETE.md   📊 Référence APIs (778 attributs)
├── AUTRES_REFERENTIELS.md      🗂️ SANDRE, BDLISA, COG... (21 référentiels)
├── ARCHITECTURE_MODERNE.md     🏗️ Architecture technique
├── TUTORIEL_DLT.md             🔧 Guide pratique DLT
└── PROJET_JUNON_VISION.md      🎯 Vision jumeau numérique

📁 scripts/
└── README.md                    ⚙️ Déploiement GitLab CI/CD

📁 configs/hubeau/
└── *.yml                        ⚙️ Configurations DLT (24 fichiers)
```

---

## 🎨 Documents par Audience

### 👔 Management / Décideurs
```
1. PROJET_JUNON_VISION.md       (Vision, contexte, ROI)
2. README.md                    (Vue d'ensemble projet)
3. APIS_HUBEAU_REFERENCE_COMPLETE.md (Données intégrées)
```

### 👨‍🔬 Chercheurs / Data Scientists
```
1. APIS_HUBEAU_REFERENCE_COMPLETE.md (Schémas données complètes)
2. AUTRES_REFERENTIELS.md           (Enrichissement avec référentiels)
3. PROJET_JUNON_VISION.md           (Approche ontologique SOSA)
```

### 👨‍💻 Développeurs
```
1. README.md                    (Installation)
2. TUTORIEL_DLT.md              (Configuration)
3. ARCHITECTURE_MODERNE.md      (Choix techniques)
4. APIS_HUBEAU_REFERENCE_COMPLETE.md (Référence API)
```

### 🔧 DevOps / SRE
```
1. ARCHITECTURE_MODERNE.md      (Stack technique)
2. scripts/README.md            (Déploiement GitLab CI/CD)
3. README.md                    (Installation)
```

### 📊 Data Engineers
```
1. TUTORIEL_DLT.md              (Configuration pipelines)
2. APIS_HUBEAU_REFERENCE_COMPLETE.md (Schémas sources)
3. AUTRES_REFERENTIELS.md       (Référentiels à intégrer)
4. ARCHITECTURE_MODERNE.md      (Architecture données)
```

---

## 🔍 Index Rapide par Mots-Clés

### APIs & Endpoints
📄 **APIS_HUBEAU_REFERENCE_COMPLETE.md** - Sections par API (Hydrométrie, Piézométrie, Qualité, etc.)

### Configuration & YAML
📄 **TUTORIEL_DLT.md** - Section "Comprendre Chaque Section"

### Slicing & Optimisations
📄 **TUTORIEL_DLT.md** - Section "Modes de Slicing Expliqués"

### SANDRE, BDLISA, BSS, COG
📄 **AUTRES_REFERENTIELS.md** - Table des matières détaillée
📄 **APIS_HUBEAU_REFERENCE_COMPLETE.md** - Sections SANDRE et BDLISA

### Jumeau Numérique, SOSA, Ontologie
📄 **PROJET_JUNON_VISION.md** - Sections complètes

### Dagster, Jobs, Partitions
📄 **README.md** - Section "Jobs Dagster"
📄 **ARCHITECTURE_MODERNE.md** - Section "Orchestration Dagster"

### Docker, Déploiement, Production
📄 **scripts/README.md** - Scripts production
📄 **README.md** - Section "Installation Rapide"

### Erreurs, Debugging
📄 **TUTORIEL_DLT.md** - Section "Gestion des Erreurs Courantes"

### Roadmap, Silver/Gold
📄 **ARCHITECTURE_MODERNE.md** - Sections Silver/Gold Layer

---

## 📝 Changelog Documentation

### 2025-10-10 - Refonte Complète

**Ajouté** :
- ✅ APIS_HUBEAU_REFERENCE_COMPLETE.md (merge de 2 docs)
- ✅ AUTRES_REFERENTIELS.md (nouveau)
- ✅ docs/README.md (index)
- ✅ NAVIGATION.md (ce fichier)
- ✅ scripts/README.md (scripts production)

**Mis à jour** :
- ✅ README.md (contexte JUNON, architecture actuelle)
- ✅ TUTORIEL_DLT.md (modes réels, optimisations mémoire)
- ✅ ARCHITECTURE_MODERNE.md (état réel vs roadmap)
- ✅ PROJET_JUNON_VISION.md (infos officielles JUNON)

**Supprimé** :
- ❌ PROJET.md (redondant avec README)
- ❌ 21 scripts dev/test obsolètes
- ❌ 4 fichiers temporaires JSON/YAML
- ❌ 6 anciens docs (OPTIMISATION_MEMOIRE, INCREMENTAL_STRATEGY, etc.)

**Résultat** : Documentation **cohérente, à jour, 100% factuelle**

---

## 💡 Contribuer à la Documentation

### Règles de Rédaction

1. ✅ **Factuel uniquement** : Pas d'estimations, que des données vérifiables
2. ✅ **Sources citées** : Toujours lier vers doc officielle
3. ✅ **État clair** : Distinguer Implémenté ✅ / En cours 🚧 / Roadmap 📋
4. ✅ **Exemples concrets** : Code, SQL, YAML réels
5. ✅ **Dates de MAJ** : Mentionner la dernière mise à jour

### Processus de Mise à Jour

1. Identifier le document concerné (voir tableau ci-dessus)
2. Modifier avec sources vérifiables
3. Mettre à jour date MAJ
4. Commit avec message clair
5. Vérifier cohérence avec autres docs

---

## 🏆 Documentation de Qualité

Cette documentation est **complète, structurée et factuellement vérifiée** :

- ✅ **778 attributs** documentés avec descriptions
- ✅ **21 référentiels** expliqués avec exemples SQL
- ✅ **5 documents** spécialisés par thème
- ✅ **0 redondances** (chaque info à un seul endroit)
- ✅ **Sources officielles** citées (JUNON, Hub'Eau, W3C, etc.)
- ✅ **État réel** du projet (pas de "wishful thinking")

**La documentation est prête pour la production et la collaboration !** 📚✨

