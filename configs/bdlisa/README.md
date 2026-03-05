# Config TME (Référentiel des Entités Hydrogéologiques)

Configuration pour l'ingestion du référentiel TME (Tableau Multi-Échelles).

- **bdlisa_entites.yml** : Configuration pour le téléchargement du fichier TME depuis le ZIP national ou URL custom.
- Voir le [Guide d'onboarding](../../docs/ONBOARDING.md) pour l'intégration dans le pipeline.

## Source des données

Le pipeline charge le fichier TME.csv depuis :
1. Fichier local `TME.csv` (prioritaire)
2. ZIP national BDLISA (fallback)
3. URL custom configurée dans `bdlisa_entites.yml`

## Note

BDLISA complet (géométries) et les nomenclatures Sandre ont été retirés du pipeline.  
Seul le référentiel TME de base est intégré.
