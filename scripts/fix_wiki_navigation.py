#!/usr/bin/env python3
"""Fix wiki navigation to use actual GitLab slugs."""
import requests
import urllib3

urllib3.disable_warnings()

URL = "https://scm.univ-tours.fr"
PID = 1219
TOKEN = "REDACTED"
H = {"PRIVATE-TOKEN": TOKEN, "Content-Type": "application/json"}

def update(slug, title, content):
    r = requests.put(f"{URL}/api/v4/projects/{PID}/wikis/{slug}",
                    headers=H, json={"title": title, "content": content},
                    verify=False, timeout=30)
    print(f"{slug}: {r.status_code}")

# HOME avec les VRAIS slugs GitLab
update("Home", "Home", """# Hub'Eau Data Integration - Wiki

Pipeline de données pour l'intégration des APIs Hub'Eau (8 APIs, 24 endpoints, 778 attributs).

## Démarrage Rapide
- [Quick Start Local](Quick-Start---Local)
- [Quick Start Production](Quick-Start---Production)

## Guides
- [Configuration](Guide---Configuration)
- [Environment Setup](Guide---Environment-Setup)

## Architecture
- [Overview](Architecture---Overview)
- [Stack Technique](Architecture---Stack)
- [Deployment](Architecture---Deployment)

## Référence (5500+ lignes)
- [APIs Hub'Eau](Reference---APIs-Hub'Eau)
- [Schéma BDD](Reference---Schema-BDD)
- [Autres Référentiels](Reference---Autres-Referentiels)
- [Observabilité](Reference---Observability)

## Développement
- [DLT Tutorial](Development---DLT-Tutorial)
- [Tests](Development---Tests)

## CI/CD
- [Pipeline](CI/CD---Pipeline)
- [Variables GitLab](CI/CD---Variables)

## Projet
- [Vision JUNON](Project---JUNON-Vision)

---
**Repository**: [gitlab.com/ringuet/hubeau_data_integration](https://scm.univ-tours.fr/ringuet/hubeau_data_integration)
""")

# SIDEBAR avec les VRAIS slugs
update("_sidebar", "_sidebar", """### Navigation

**Démarrage**
- [Quick Start Local](Quick-Start---Local)
- [Quick Start Production](Quick-Start---Production)

**Guides**
- [Configuration](Guide---Configuration)
- [Environment Setup](Guide---Environment-Setup)

**Architecture**
- [Overview](Architecture---Overview)
- [Stack](Architecture---Stack)
- [Deployment](Architecture---Deployment)

**Référence**
- [APIs Hub'Eau](Reference---APIs-Hub'Eau)
- [Schéma BDD](Reference---Schema-BDD)
- [Référentiels](Reference---Autres-Referentiels)
- [Observabilité](Reference---Observability)

**Dev**
- [DLT Tutorial](Development---DLT-Tutorial)
- [Tests](Development---Tests)

**CI/CD**
- [Pipeline](CI/CD---Pipeline)
- [Variables](CI/CD---Variables)

**Projet**
- [JUNON](Project---JUNON-Vision)

---
[🏠 Accueil](Home)
""")

print("\nNavigation mise a jour!")
print("Wiki: https://scm.univ-tours.fr/ringuet/hubeau_data_integration/-/wikis/Home")
