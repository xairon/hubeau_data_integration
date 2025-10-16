#!/usr/bin/env python3
"""
Script pour créer les pages wiki GitLab avec les grandes documentations de référence.
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

# Pages à créer
WIKI_PAGES = [
    {
        "file": "docs/APIS_HUBEAU_REFERENCE_COMPLETE.md",
        "title": "APIs Hub'Eau - Documentation de Référence Complète",
        "slug": "apis-hubeau-reference-complete"
    },
    {
        "file": "docs/SCHEMA_BDD_HUBEAU.md",
        "title": "Schéma Base de Données Hub'Eau",
        "slug": "schema-bdd-hubeau"
    },
    {
        "file": "docs/AUTRES_REFERENTIELS.md",
        "title": "Autres Référentiels",
        "slug": "autres-referentiels"
    },
    {
        "file": "docs/OBSERVABILITY.md",
        "title": "Observabilité & Monitoring",
        "slug": "observability"
    }
]


def create_wiki_page(file_path: str, title: str, slug: str) -> bool:
    """Crée une page wiki sur GitLab."""

    # Lire le contenu du fichier
    file_full_path = Path(__file__).parent.parent / file_path
    if not file_full_path.exists():
        print(f"[ERROR] Fichier introuvable: {file_full_path}")
        return False

    with open(file_full_path, "r", encoding="utf-8") as f:
        content = f.read()

    # API endpoint
    url = f"{GITLAB_URL}/api/v4/projects/{PROJECT_ID}/wikis"

    # Données
    data = {
        "title": title,
        "content": content
    }

    print(f"Creation page wiki: {title}")
    print(f"   Taille: {len(content)} caracteres")

    try:
        response = requests.post(
            url,
            headers=HEADERS,
            json=data,
            verify=False,  # Ignorer vérification SSL (certificat auto-signé)
            timeout=30
        )

        if response.status_code == 201:
            print(f"[OK] Page creee: {title}")
            wiki_url = response.json().get("web_url", "")
            print(f"   URL: {wiki_url}")
            return True
        elif response.status_code == 409:
            print(f"[WARN] Page existe deja: {title}")
            print(f"   Tentative de mise a jour...")
            return update_wiki_page(slug, content, title)
        else:
            print(f"[ERROR] Erreur {response.status_code}: {response.text}")
            return False

    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        return False


def update_wiki_page(slug: str, content: str, title: str) -> bool:
    """Met à jour une page wiki existante."""

    url = f"{GITLAB_URL}/api/v4/projects/{PROJECT_ID}/wikis/{slug}"

    data = {
        "title": title,
        "content": content
    }

    try:
        response = requests.put(
            url,
            headers=HEADERS,
            json=data,
            verify=False,
            timeout=30
        )

        if response.status_code == 200:
            print(f"[OK] Page mise a jour: {title}")
            return True
        else:
            print(f"[ERROR] Erreur mise a jour {response.status_code}: {response.text}")
            return False

    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        return False


def main():
    """Fonction principale."""
    print("=" * 80)
    print("Creation des pages wiki GitLab")
    print("=" * 80)
    print()

    success_count = 0
    total_count = len(WIKI_PAGES)

    for page in WIKI_PAGES:
        if create_wiki_page(page["file"], page["title"], page["slug"]):
            success_count += 1
        print()

    print("=" * 80)
    print(f"Resultat: {success_count}/{total_count} pages creees/mises a jour")
    print("=" * 80)
    print()
    print(f"Wiki disponible a: {GITLAB_URL}/ringuet/hubeau_data_integration/-/wikis/home")


if __name__ == "__main__":
    main()
