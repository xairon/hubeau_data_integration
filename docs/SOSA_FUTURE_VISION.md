# Vision SOSA & perspectives futures

Dernière mise à jour : 2024-09-30

Cette feuille de route décrit la trajectoire pour construire les couches analytiques Silver/Gold et structurer la connaissance Hub'Eau autour de l'ontologie SOSA/SSN.

---

## 1. Objectifs scientifiques

1. **Structurer les observations hydrologiques** dans un modèle commun (stations, phénomènes, procédures) basé sur SOSA.
2. **Relier les données multisources** (hydrométrie, qualité, hydrobiologie, prélèvements) pour analyser les interactions nappes–cours d'eau.
3. **Fournir des APIs internes** (GraphQL/REST) pour les chercheurs BRGM et partenaires académiques.

---

## 2. Couches analytiques prévues

| Couche | Description | État |
| --- | --- | --- |
| **Silver** | Normalisation dans TimescaleDB/PostGIS. Tables fact/dimension par API, index spatio-temporels, vues agrégées. | À spécifier (2024 Q4) |
| **Gold** | Knowledge graph Neo4j aligné SOSA : `ObservableProperty`, `Observation`, `FeatureOfInterest`, `SamplingFeature`. | Prototype (2025 Q1) |
| **APIs** | FastAPI exposant des endpoints agrégés (ex : séries temporelles harmonisées, alertes). | Conception (2025 Q2) |

---

## 3. Alignement SOSA

- **Observation** : chaque enregistrement Hub'Eau devient un nœud `Observation` avec propriétés : valeur, unité, timestamp, procédure.
- **Feature of Interest** : stations (`Station`, `Ouvrage`, `Point de prélèvement`) – alignement via codes BSS / code entité.
- **Procedure** : type de mesure (hydrométrie temps réel, prélèvement physico-chimique…).
- **ObservableProperty** : débit, niveau, concentration, indice biologique, volume prélevé…
- **Sensor/Platform** : métadonnées sur le dispositif de mesure (à enrichir via référentiels externes BRGM).

---

## 4. Roadmap

| Période | Jalons |
| --- | --- |
| 2024 Q4 | Modélisation Silver (TimescaleDB), scripts d'ETL Bronze -> Silver, premiers dashboards (Metabase). |
| 2025 Q1 | Modèle Neo4j SOSA minimal, import hydrométrie + piézométrie, API Cypher d'exploration. |
| 2025 Q2 | Intégration qualité eau + hydrobiologie dans Neo4j, API FastAPI / GraphQL, alertes multi-sources. |
| 2025 Q3 | Publication dataset open data, documentation scientifique (papier/revue). |

---

## 5. Collaborations & dépendances

- **Equipe Hydrosystèmes BRGM** : validation scientifique des indicateurs.
- **Equipe Systèmes d'information** : support déploiement TimescaleDB/Neo4j.
- **Partenaires académiques** : co-construction des cas d'usage (Université d'Orléans, CNRS…).
- **Hub'Eau** : veille sur les changements d'API, participation aux tests bêta.

---

## 6. Indicateurs de succès

- Temps moyen pour recharger une année complète de données < 2h.
- 100% des partitions Bronze couvertes par des tables Silver.
- 80% des stations alignées sur des entités SOSA (`FeatureOfInterest`).
- Publication d'au moins 2 analyses scientifiques/an reposant sur le pipeline.

---

Cette vision doit être revue trimestriellement. Toute décision structurante (nouvel entrepôt, changement d'ontologie) doit être consignée ici.
