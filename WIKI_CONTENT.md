# Contenu à Migrer vers Wiki GitLab

Ce fichier liste les documents volumineux qui devraient être dans le Wiki GitLab plutôt que dans le dépôt.

## 📚 Fichiers pour le Wiki

### 1. APIs Hub'Eau - Référence Complète (68K)
**Fichier** : `docs/APIS_HUBEAU_REFERENCE_COMPLETE.md`
**Page Wiki** : `APIs-HubEau-Reference`
**Contenu** : Schémas détaillés des 8 APIs Hub'Eau (778 attributs)

### 2. Schéma Base de Données (65K)
**Fichier** : `docs/SCHEMA_BDD_HUBEAU.md`
**Page Wiki** : `Database-Schema`
**Contenu** : Design complet BDD Silver/Gold, modèles de données

### 3. Autres Référentiels (40K)
**Fichier** : `docs/AUTRES_REFERENTIELS.md`
**Page Wiki** : `External-Referentials`
**Contenu** : SANDRE, BDLISA, COG, NQE, TAXREF

### 4. Observability (20K)
**Fichier** : `docs/OBSERVABILITY.md`
**Page Wiki** : `Monitoring-Observability`
**Contenu** : Setup Prometheus, Grafana, métriques avancées

## 🔧 Comment Migrer vers le Wiki

### Étape 1 : Créer les Pages Wiki

```bash
# Via GitLab UI
1. Aller sur : https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/wikis
2. Cliquer "New page"
3. Créer les 4 pages listées ci-dessus
```

### Étape 2 : Copier le Contenu

Pour chaque fichier :
1. Ouvrir le fichier `.md`
2. Copier tout le contenu
3. Coller dans la page Wiki correspondante
4. Sauvegarder

### Étape 3 : Mettre à Jour les Liens

Dans le dépôt, remplacer les liens par des liens Wiki :

**Avant** :
```markdown
[Référence API](docs/APIS_HUBEAU_REFERENCE_COMPLETE.md)
```

**Après** :
```markdown
[Référence API](https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/wikis/APIs-HubEau-Reference)
```

### Étape 4 : Supprimer du Dépôt

```bash
git rm docs/APIS_HUBEAU_REFERENCE_COMPLETE.md
git rm docs/SCHEMA_BDD_HUBEAU.md
git rm docs/AUTRES_REFERENTIELS.md
git rm docs/OBSERVABILITY.md
git commit -m "docs: move large reference docs to GitLab wiki"
```

## 📖 Structure Wiki Proposée

```
Home
├── Getting Started
│   ├── Quick Start
│   ├── Installation
│   └── First Run
│
├── Technical Documentation
│   ├── Architecture
│   ├── Database Schema
│   └── Monitoring & Observability
│
├── Data Reference
│   ├── APIs Hub'Eau Reference
│   ├── External Referentials
│   └── Data Quality
│
└── Advanced Topics
    ├── Custom DLT Pipelines
    ├── Scaling & Performance
    └── Troubleshooting
```

## ✅ Avantages du Wiki

1. **Versioning** : GitLab wiki a son propre Git
2. **Search** : Recherche intégrée GitLab
3. **TOC automatique** : Table des matières générée
4. **Collaboration** : Édition directe via UI
5. **Liens inter-pages** : Navigation facile
6. **Moins de bruit** : Dépôt plus léger, focus sur le code

## 📝 À Garder dans le Dépôt

- `README.md` - Vue d'ensemble
- `CONTRIBUTING.md` - Guide contribution
- `GITLAB_CI_SETUP.md` - Setup CI/CD
- `.env.template` - Template config
- `docs/QUICK_START_LOCAL.md` - Démarrage rapide
- `docs/TUTORIEL_DLT.md` - Tutoriel DLT
- `docs/ARCHITECTURE.md` - Architecture actuelle
- `docs/ENVIRONMENT_CONFIGURATION.md` - Config environnements
- `docs/PROJET_JUNON_VISION.md` - Vision projet

**Principe** : Garder l'essentiel opérationnel, déléguer la référence au Wiki

---

**Note** : Cette migration peut être faite progressivement, pas d'urgence !
