# Intégration SANDRE & BD-LISA

## Vue d'ensemble

Cette intégration ajoute les référentiels SANDRE et les données spatiales BD-LISA au pipeline Hub'Eau existant.

## Architecture

### Nouvelles sources de données

#### SANDRE (14 référentiels)
- **API**: `https://api.sandre.eaufrance.fr/referentiels/v1/`
- **Format**: CSV/JSON
- **Schéma PostgreSQL**: `sandre`

Tables créées:
- `sandre_parametres` - Paramètres physico-chimiques (~6300 lignes)
- `sandre_unites` - Unités de mesure (~700 lignes)
- `sandre_methodes` - Méthodes d'analyse (~2000 lignes)
- `sandre_supports` - Supports de prélèvement (~30 lignes)
- `sandre_fractions` - Fractions analysées (~10 lignes)
- `sandre_communes` - Communes françaises (~35000 lignes)
- `sandre_departements` - Départements (~101 lignes)
- `sandre_regions` - Régions (~18 lignes)
- `sandre_intervenants` - Organismes/laboratoires (~50000 lignes)
- `sandre_masses_eau_sout` - Masses d'eau souterraines (~650 lignes)
- `sandre_masses_eau_surf` - Masses d'eau surface (~11000 lignes)
- `sandre_cours_eau` - Cours d'eau (~120000 lignes)
- `sandre_bassins` - Bassins versants (~150 lignes)
- `sandre_milieux` - Milieux de prélèvement (~15 lignes)

#### BD-LISA (3 niveaux)
- **Services**: WFS/WMS
- **Format**: GeoJSON avec géométries
- **Schéma PostgreSQL**: `bdlisa`

Tables créées:
- `bdlisa_entites_nv1` - Niveau national (~200 polygones)
- `bdlisa_entites_nv2` - Niveau régional (~500 polygones)
- `bdlisa_entites_nv3` - Niveau local (~4000 polygones)

Chaque table contient:
- `code` - Identifiant unique
- `libelle` - Nom de l'entité
- `niveau` - Niveau (1/2/3)
- `theme` - Thème géologique
- `nature` - Nature (aquifère/imperméable)
- `milieu` - Milieu (poreux/fissuré/karstique)
- `etat` - État (libre/captif)
- `geometry_wkt` - Géométrie en WKT
- `geom` - Géométrie PostGIS (SRID 2154)

## Installation

### 1. Installer les dépendances spatiales

```bash
pip install -r requirements_reference.txt
```

### 2. Activer PostGIS

```sql
-- Se connecter à la base
psql -h localhost -U postgres -d postgres

-- Créer l'extension PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
```

## Utilisation

### Jobs Dagster

Trois nouveaux jobs sont disponibles dans l'interface Dagster:

1. **`sandre_full_load`** - Charge tous les référentiels SANDRE
   - Fréquence recommandée: Hebdomadaire
   - Durée estimée: 5-10 minutes

2. **`bdlisa_spatial_load`** - Charge les entités BD-LISA avec géométries
   - Fréquence recommandée: Mensuelle
   - Durée estimée: 2-5 minutes

3. **`reference_data_full_load`** - Charge SANDRE + BD-LISA en une fois
   - Pour initialisation ou mise à jour complète

### Lancer les jobs

#### Via l'interface Dagster (recommandé)
1. Ouvrir http://localhost:3000
2. Aller dans "Jobs"
3. Sélectionner le job souhaité
4. Cliquer sur "Launch Run"

#### Via CLI
```bash
# Charger SANDRE
dagster job execute -m hubeau_pipeline -j sandre_full_load

# Charger BD-LISA
dagster job execute -m hubeau_pipeline -j bdlisa_spatial_load

# Tout charger
dagster job execute -m hubeau_pipeline -j reference_data_full_load
```

## Requêtes SQL utiles

### Vérifier le chargement

```sql
-- Compter les référentiels SANDRE
SELECT
    'parametres' as table_name, COUNT(*) as count FROM sandre.sandre_parametres
UNION ALL
SELECT 'communes', COUNT(*) FROM sandre.sandre_communes
UNION ALL
SELECT 'intervenants', COUNT(*) FROM sandre.sandre_intervenants;

-- Vérifier BD-LISA avec géométries
SELECT
    niveau,
    COUNT(*) as total,
    COUNT(geom) as with_geometry
FROM bdlisa.bdlisa_entites_nv3
GROUP BY niveau;
```

### Enrichir les données Hub'Eau

```sql
-- Ajouter les libellés des paramètres aux analyses
SELECT
    a.*,
    p.NomParametre,
    p.LbCourtParametre
FROM hubeau.quality_rivers_analyses_raw a
LEFT JOIN sandre.sandre_parametres p
    ON a.code_parametre = p.CdParametre
LIMIT 10;

-- Trouver dans quelle entité BD-LISA se trouve une station
SELECT
    s.code_bss,
    s.libelle_pe,
    b.libelle as aquifere,
    b.nature,
    b.milieu,
    b.etat
FROM hubeau.piezometry_stations_raw s
JOIN bdlisa.bdlisa_entites_nv3 b
    ON ST_Contains(
        b.geom,
        ST_SetSRID(ST_MakePoint(s.x, s.y), 2154)
    )
WHERE s.x IS NOT NULL
LIMIT 10;
```

## Troubleshooting

### Erreur "could not convert geometry"
- Installer shapely: `pip install shapely`

### Erreur PostGIS
- Vérifier l'installation: `SELECT PostGIS_Version();`
- Créer l'extension: `CREATE EXTENSION postgis;`

### WFS timeout
- Les services BD-LISA peuvent être lents
- Le code a un fallback automatique
- Réessayer plus tard si nécessaire

### Tables vides
- Vérifier la connexion internet
- Vérifier les logs Dagster pour les erreurs
- L'API SANDRE a une limite de pagination (size=10000)

## Maintenance

### Mise à jour des référentiels
- SANDRE change peu (mise à jour hebdomadaire suffisante)
- BD-LISA version 3 est stable (mise à jour mensuelle)

### Monitoring
- Vérifier le nombre de lignes après chaque chargement
- Les assets `bdlisa_stats` donnent des statistiques automatiques

## Ressources

- [API SANDRE](https://api.sandre.eaufrance.fr/referentiels/v1/)
- [Site BD-LISA](https://bdlisa.eaufrance.fr/)
- [Documentation PostGIS](https://postgis.net/documentation/)