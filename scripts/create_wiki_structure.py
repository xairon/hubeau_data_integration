#!/usr/bin/env python3
"""
Script pour créer une structure de wiki GitLab standard avec:
- Page home (page d'accueil)
- _sidebar (navigation personnalisée)
- Structure hiérarchique organisée
"""

import requests
import urllib3
from pathlib import Path

# Désactiver les avertissements SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
GITLAB_URL = "https://scm.univ-tours.fr"
PROJECT_ID = 1219
TOKEN = "REDACTED"

HEADERS = {
    "PRIVATE-TOKEN": TOKEN,
    "Content-Type": "application/json"
}

# Structure du wiki
WIKI_STRUCTURE = {
    "home": {
        "title": "Hub'Eau Data Integration - Wiki",
        "content": """# Hub'Eau Data Integration - Documentation Wiki

Bienvenue sur le wiki du projet Hub'Eau Data Integration !

## 📚 Documentation Principale

Consultez le [README.md](https://scm.univ-tours.fr/ringuet/hubeau_data_integration) du dépôt pour démarrer rapidement.

## 📖 Guides Utilisateur

- **[Quick Start Local](quick-start/local)** - Démarrer le projet en local (Docker)
- **[Quick Start Production](quick-start/production)** - Déployer sur un VPS
- **[Configuration](guides/configuration)** - Configurer les environnements

## 🏗️ Architecture

- **[Architecture Globale](architecture/overview)** - Vue d'ensemble du système
- **[Stack Technique](architecture/stack)** - Technologies utilisées (Dagster, DLT, MinIO, PostgreSQL)
- **[Déploiement](architecture/deployment)** - Architecture de déploiement

## 📊 Documentation Technique de Référence

Documentation exhaustive des APIs Hub'Eau et schémas de données :

- **[APIs Hub'Eau - Référence Complète](APIs-Hub'Eau---Documentation-de-Référence-Complète)** - 8 APIs, 24 endpoints, 778 attributs
- **[Schéma Base de Données](Schéma-Base-de-Données-Hub'Eau)** - Modèle de données complet
- **[Autres Référentiels](Autres-Référentiels)** - SANDRE, BDLISA, etc.
- **[Observabilité & Monitoring](Observabilité-&-Monitoring)** - Métriques et monitoring

## 🔧 Développement

- **[Contributing](https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/blob/main/CONTRIBUTING.md)** - Guide de contribution
- **[Architecture Code](architecture/code)** - Structure du code Python
- **[Tests](development/tests)** - Guide de tests

## 🚀 CI/CD & GitLab

- **[Pipeline CI/CD](cicd/pipeline)** - Configuration du pipeline GitLab
- **[Variables GitLab](cicd/variables)** - Variables d'environnement à configurer

## 🆘 Support

- **[Troubleshooting](support/troubleshooting)** - Résolution de problèmes courants
- **[FAQ](support/faq)** - Questions fréquentes

---

**Projet** : Hub'Eau Data Integration
**Maintainers** : Nicolas Ringuet
**License** : MIT
**Repository** : [gitlab.com/ringuet/hubeau_data_integration](https://scm.univ-tours.fr/ringuet/hubeau_data_integration)
"""
    },

    "_sidebar": {
        "title": "_sidebar",
        "content": """## Navigation

### 📚 Guides
- [Quick Start Local](quick-start/local)
- [Quick Start Production](quick-start/production)
- [Configuration](guides/configuration)

### 🏗️ Architecture
- [Vue d'ensemble](architecture/overview)
- [Stack Technique](architecture/stack)
- [Déploiement](architecture/deployment)

### 📊 Référence Technique
- [APIs Hub'Eau](APIs-Hub'Eau---Documentation-de-Référence-Complète)
- [Schéma BDD](Schéma-Base-de-Données-Hub'Eau)
- [Autres Référentiels](Autres-Référentiels)
- [Observabilité](Observabilité-&-Monitoring)

### 🔧 Développement
- [Contributing](https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/blob/main/CONTRIBUTING.md)
- [Architecture Code](architecture/code)
- [Tests](development/tests)

### 🚀 CI/CD
- [Pipeline](cicd/pipeline)
- [Variables GitLab](cicd/variables)

### 🆘 Support
- [Troubleshooting](support/troubleshooting)
- [FAQ](support/faq)

---
[🏠 Retour à l'accueil](home)
"""
    },

    "quick-start/local": {
        "title": "Quick Start - Local Development",
        "content": """# Quick Start - Développement Local

Guide pour démarrer le projet Hub'Eau Data Integration en local avec Docker.

## Prérequis

- Docker Desktop installé
- Git
- 8 GB RAM minimum

## Installation

### 1. Cloner le dépôt

```bash
git clone https://scm.univ-tours.fr/ringuet/hubeau_data_integration.git
cd hubeau_data_integration
```

### 2. Créer le fichier .env

Copier le template et renseigner les mots de passe :

```bash
cp .env.template .env
# Éditer .env et renseigner les passwords
```

### 3. Démarrer les services

```bash
docker compose up -d
```

### 4. Vérifier que tout fonctionne

```bash
docker compose ps
```

Tous les services doivent être "healthy".

### 5. Accéder à l'interface

- **Dagster UI** : http://localhost:8080
- **MinIO Console** : http://localhost:9001

## Prochaines étapes

- Consultez le [README.md](https://scm.univ-tours.fr/ringuet/hubeau_data_integration) pour plus de détails
- Voir la [Configuration](../guides/configuration) pour personnaliser

[🏠 Retour à l'accueil](../home)
"""
    },

    "quick-start/production": {
        "title": "Quick Start - Production Deployment",
        "content": """# Quick Start - Déploiement Production

Guide pour déployer Hub'Eau Data Integration sur un VPS en production.

## Prérequis

- VPS avec Docker installé
- GitLab Runner configuré
- Accès SSH au serveur

## Déploiement Automatique (GitLab CI/CD)

Le projet utilise GitLab CI/CD pour le déploiement automatique.

### 1. Configurer les Variables GitLab

Dans GitLab : Settings → CI/CD → Variables

Variables requises :
- `DAGSTER_PG_HOST`
- `DAGSTER_PG_PASSWORD`
- `MINIO_ENDPOINT`
- `MINIO_USER`
- `MINIO_PASS`
- `MINIO_REGION`
- `MINIO_BRONZE_BUCKET`
- `PG_HOST`
- `PG_PASSWORD`
- `POSTGIS_HOST`

Voir [Variables GitLab](../cicd/variables) pour la liste complète.

### 2. Push sur main

```bash
git push origin main
```

Le pipeline GitLab se déclenche automatiquement :
1. Build des images Docker
2. Déploiement sur le VPS
3. Démarrage des services

### 3. Vérifier le déploiement

Accéder à l'URL de production (configurée dans le runner).

## Déploiement Manuel

Si vous préférez déployer manuellement :

```bash
# Sur le serveur
cd /srv/brgm
docker compose -f docker-compose.production.yml up -d
```

## Prochaines étapes

- Consultez [Architecture de déploiement](../architecture/deployment)
- Voir [Troubleshooting](../support/troubleshooting) en cas de problème

[🏠 Retour à l'accueil](../home)
"""
    },

}


def create_or_update_wiki_page(slug: str, title: str, content: str = None, file_path: str = None) -> bool:
    """Crée ou met à jour une page wiki."""

    # Si file_path fourni, lire le contenu depuis le fichier
    if file_path:
        file_full_path = Path(__file__).parent.parent / file_path
        if not file_full_path.exists():
            print(f"[ERROR] Fichier introuvable: {file_full_path}")
            return False
        with open(file_full_path, "r", encoding="utf-8") as f:
            content = f.read()

    if not content:
        print(f"[ERROR] Pas de contenu pour {slug}")
        return False

    # Essayer de créer la page
    url = f"{GITLAB_URL}/api/v4/projects/{PROJECT_ID}/wikis"
    data = {"title": title, "content": content}

    print(f"Creation/MAJ page: {slug}")

    try:
        response = requests.post(
            url,
            headers=HEADERS,
            json=data,
            verify=False,
            timeout=30
        )

        if response.status_code == 201:
            print(f"[OK] Page creee: {slug}")
            return True
        elif response.status_code == 409:
            # Page existe, mettre à jour
            print(f"[INFO] Page existe, mise a jour...")
            url_update = f"{GITLAB_URL}/api/v4/projects/{PROJECT_ID}/wikis/{slug}"
            response = requests.put(
                url_update,
                headers=HEADERS,
                json=data,
                verify=False,
                timeout=30
            )
            if response.status_code == 200:
                print(f"[OK] Page mise a jour: {slug}")
                return True
            else:
                print(f"[ERROR] Erreur MAJ {response.status_code}: {response.text}")
                return False
        else:
            print(f"[ERROR] Erreur {response.status_code}: {response.text}")
            return False

    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        return False


def main():
    """Fonction principale."""
    print("=" * 80)
    print("Creation structure wiki GitLab standard")
    print("=" * 80)
    print()

    success_count = 0
    total_count = len(WIKI_STRUCTURE)

    for slug, config in WIKI_STRUCTURE.items():
        title = config["title"]
        content = config.get("content")
        file_path = config.get("file")

        if create_or_update_wiki_page(slug, title, content, file_path):
            success_count += 1
        print()

    print("=" * 80)
    print(f"Resultat: {success_count}/{total_count} pages creees/mises a jour")
    print("=" * 80)
    print()
    print(f"Wiki disponible a: {GITLAB_URL}/ringuet/hubeau_data_integration/-/wikis/home")


if __name__ == "__main__":
    main()
