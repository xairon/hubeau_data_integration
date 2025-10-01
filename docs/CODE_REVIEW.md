# Code review & validation scientifique

Dernière mise à jour : 2024-09-30

Cette checklist garantit la qualité logicielle et la pertinence scientifique des contributions. Chaque PR doit cocher l'ensemble des points applicables avant d'être fusionnée.

---

## 1. Checklist générale

- [ ] Tests Pytest exécutés (`pytest`).
- [ ] Documentation mise à jour (README, docs/…).
- [ ] Description claire de la PR (impacts techniques & scientifiques).
- [ ] Ajout d'un changelog si nécessaire (roadmap interne).

## 2. Données & science

- [ ] Les sources Hub'Eau impactées sont documentées dans `DATA_SOURCES_COMPLETE.md` (paramètres, date de vérification).
- [ ] Les filtres spatiaux/temporaux ajoutés sont justifiés scientifiquement (commentaire dans le code + doc).
- [ ] Les métriques calculées ou transformations respectent les conventions BRGM (unités, codes station).
- [ ] Les partitions affectées ont été rejouées localement pour vérifier les volumes.

## 3. Architecture & performance

- [ ] Les limites Hub'Eau (requêtes simultanées, taille de page) sont respectées.
- [ ] Le fallback local MinIO fonctionne (tests ou exécution manuelle sans MinIO).
- [ ] Les erreurs sont correctement propagées (pas de swallow silencieux).
- [ ] Les assets sont tagués (`api=hubeau`) si nécessaire pour la gestion de concurrence.

## 4. Sécurité & conformité

- [ ] Aucun secret en clair dans le code ou la doc (utiliser `.env`).
- [ ] Les licences Hub'Eau/BRGM sont respectées (diffusion publique vérifiée si applicable).
- [ ] Les dépendances ajoutées sont compatibles avec l'environnement BRGM.

## 5. Validation finale

- [ ] Relecture croisée par un membre de l'équipe (si disponible) ou auto-relecture détaillée.
- [ ] Résumé envoyé sur le canal de suivi scientifique (notebook, rapport).

> En cas de doute, ajouter une section « Décision » dans la PR détaillant le rationnel scientifique/technique.
