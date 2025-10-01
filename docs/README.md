# Documentation Hub'Eau – BRGM

Cette arborescence regroupe la documentation scientifique et technique du pipeline. Chaque guide est versionné avec le code et doit être mis à jour lors de toute modification fonctionnelle ou d'architecture.

## 🧭 Plan de lecture

| Document | Contenu |
| --- | --- |
| [`ARCHITECTURE_MODERNE.md`](ARCHITECTURE_MODERNE.md) | Diagrammes logiques, topologies de déploiement, ressources Dagster et intégrations (MinIO, TimescaleDB, Neo4j). |
| [`HUBEAU_PIPELINE.md`](HUBEAU_PIPELINE.md) | Détails du client HTTP, du service d'ingestion, des assets Dagster et de la stratégie de partitionnement. |
| [`DATA_SOURCES_COMPLETE.md`](DATA_SOURCES_COMPLETE.md) | Fiches techniques par API Hub'Eau (endpoints, paramètres, fréquence, liens officiels). |
| [`DATA_STORAGE_STRATEGY.md`](DATA_STORAGE_STRATEGY.md) | Conventions Bronze (MinIO & fallback local), politiques de rétention, contrôles qualité. |
| [`CODE_REVIEW.md`](CODE_REVIEW.md) | Checklist scientifique/logicielle pour les PR et bonnes pratiques de validation. |
| [`SOSA_FUTURE_VISION.md`](SOSA_FUTURE_VISION.md) | Roadmap Silver/Gold, modélisation SOSA et articulation avec les projets de recherche BRGM. |

## 🔄 Gouvernance documentaire

- Chaque PR modifiant le comportement d'un asset ou d'une configuration API doit référencer la section documentation mise à jour.
- Les tableaux listant les endpoints incluent la date de dernière vérification. Mettre à jour cette date lors de la revue.
- Les décisions d'architecture sont récapitulées dans `ARCHITECTURE_MODERNE.md` et `HUBEAU_PIPELINE.md` avec le rationnel scientifique/technique.

## 🧰 Ressources externes

- [Portail Hub'Eau – documentation officielle](https://hubeau.eaufrance.fr/page/apis)
- [Dagster Documentation](https://docs.dagster.io/)
- [SOSA/SSN Ontology](https://www.w3.org/TR/vocab-ssn/)

La documentation doit rester synchrone avec les implémentations pour garantir la reproductibilité des travaux scientifiques.
