# Scripts SQL - Optimisation PostgreSQL

Ce dossier contient les scripts SQL pour optimiser la base de données PostgreSQL Hub'Eau.

## 📋 Scripts Disponibles

| Script | Description | Durée estimée | Ordre |
|--------|-------------|---------------|-------|
| `01_create_indexes.sql` | Crée index optimisés pour performance requêtes | 5-15 min | 1er |
| `02_create_foreign_keys.sql` | Ajoute contraintes d'intégrité référentielle | 2-5 min | 2ème |
| `03_add_postgis_geometry.sql` | Ajoute colonnes géométriques PostGIS | 3-10 min | 3ème |

## 🚀 Exécution

### Option 1: Via psql (ligne de commande)

```bash
# Se connecter au serveur PostgreSQL
psql -U postgres -h localhost -d postgres

# Exécuter les scripts dans l'ordre
\i 01_create_indexes.sql
\i 02_create_foreign_keys.sql
\i 03_add_postgis_geometry.sql
```

### Option 2: Via Adminer (Web UI)

1. Accéder à Adminer: http://localhost:8081
2. Se connecter:
   - Server: `postgres`
   - Username: `postgres`
   - Password: `votre_mot_de_passe`
   - Database: `postgres`
3. Onglet "SQL command"
4. Copier-coller le contenu d'un script
5. Cliquer "Execute"

### Option 3: Via PgAdmin (Web UI)

1. Accéder à PgAdmin: http://localhost:5050
2. Se connecter et ouvrir "Query Tool"
3. Copier-coller le contenu d'un script
4. Exécuter (F5)

### Option 4: Via Docker

```bash
# Copier scripts dans container
docker cp 01_create_indexes.sql postgres:/tmp/

# Exécuter depuis container
docker exec -it postgres psql -U postgres -d postgres -f /tmp/01_create_indexes.sql
```

## ⚠️ Prérequis et Recommandations

### Avant d'exécuter les scripts

1. **Sauvegarde**: Faire un backup de la base de données
   ```bash
   docker exec postgres pg_dump -U postgres postgres > backup_$(date +%Y%m%d).sql
   ```

2. **Vérifier espace disque**: Les index peuvent doubler la taille de la DB
   ```sql
   SELECT pg_size_pretty(pg_database_size('postgres'));
   ```

3. **Planifier hors heures de pointe**: Les scripts peuvent ralentir les requêtes

### Script 01: Indexes

**Pourquoi?**
- Accélère les requêtes de filtrage (WHERE, JOIN) de 10x à 1000x
- Optimise les tri (ORDER BY)
- Améliore performance Dagster UI (historique runs)

**Impact:**
- ✅ Performance requêtes: 🚀🚀🚀
- ⚠️ Taille DB: +20-50%
- ⚠️ Durée INSERT: +5-10% (négligeable)

**Notes:**
- Utilise `CONCURRENTLY` pour éviter lock des tables
- Peut être exécuté sur base en production
- Créer d'abord les index les plus critiques si manque d'espace

### Script 02: Foreign Keys

**Pourquoi?**
- Garantit l'intégrité référentielle (pas de chroniques sans station)
- Suppression en cascade automatique
- Détecte erreurs d'insertion

**Impact:**
- ✅ Intégrité données: 🔒
- ⚠️ Performance INSERT: -10-20%
- ⚠️ Peut échouer si données orphelines existantes

**ATTENTION:**
- Vérifier orphelins avant d'exécuter (script fait la vérification)
- Option 1: Supprimer orphelins (perte de données)
- Option 2: Charger stations manquantes d'abord
- Option 3: Ne pas créer FK pour cette table

**Recommandation:**
- Exécuter APRÈS ingestion initiale des stations
- Tester d'abord sur table de dev

### Script 03: PostGIS Geometry

**Pourquoi?**
- Requêtes spatiales 10-100x plus rapides
- Support distance, buffer, intersection, etc.
- Export facile vers QGIS, GeoJSON

**Impact:**
- ✅ Performance géo: 🚀🚀🚀
- ⚠️ Taille DB: +5-10%
- ✅ Nouvelles capacités: cartes, analyses spatiales

**Prérequis:**
- Extension PostGIS installée (le script la crée automatiquement)

**Features ajoutées:**
- Colonne `geom` sur toutes les tables stations
- Index spatiaux (GIST)
- Triggers mise à jour auto (si lat/lon changent)
- Contraintes validation géométrie

## 📊 Vérification Post-Exécution

### Vérifier les index créés

```sql
SET search_path TO hubeau;

SELECT
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_indexes
JOIN pg_class ON indexrelid = oid
WHERE schemaname = 'hubeau'
ORDER BY pg_relation_size(indexrelid) DESC;
```

### Vérifier les foreign keys

```sql
SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table,
    ccu.column_name AS foreign_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'hubeau';
```

### Vérifier les géométries PostGIS

```sql
SELECT
    table_name,
    column_name,
    COUNT(*) FILTER (WHERE geom IS NOT NULL) AS has_geom,
    COUNT(*) AS total_rows
FROM information_schema.columns c
JOIN hubeau.piezometry_stations USING (table_name)  -- Exemple
WHERE table_schema = 'hubeau'
  AND column_name = 'geom'
GROUP BY table_name, column_name;
```

## 🔧 Rollback (Annulation)

Si besoin d'annuler les modifications:

### Supprimer les index

```sql
-- Liste des index à supprimer
SELECT 'DROP INDEX IF EXISTS hubeau.' || indexname || ';'
FROM pg_indexes
WHERE schemaname = 'hubeau'
  AND indexname LIKE 'idx_%';

-- Copier-coller les DROP générés et exécuter
```

### Supprimer les foreign keys

```sql
-- Liste des FK à supprimer
SELECT 'ALTER TABLE hubeau.' || table_name || ' DROP CONSTRAINT ' || constraint_name || ';'
FROM information_schema.table_constraints
WHERE constraint_type = 'FOREIGN KEY'
  AND table_schema = 'hubeau';

-- Copier-coller les ALTER générés et exécuter
```

### Supprimer les colonnes géométriques

```sql
-- Supprimer colonne geom de toutes les tables stations
DO $$
DECLARE
    table_rec RECORD;
BEGIN
    FOR table_rec IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'hubeau'
          AND (table_name LIKE '%_stations' OR table_name LIKE '%_sites')
    LOOP
        EXECUTE format('ALTER TABLE hubeau.%I DROP COLUMN IF EXISTS geom;', table_rec.table_name);
    END LOOP;
END;
$$;
```

## 💡 Exemples de Requêtes Optimisées

### Avec index (rapide):

```sql
-- Chercher chroniques piézométrie pour une station sur période
SELECT *
FROM hubeau.piezometry_chroniques
WHERE code_bss = 'BSS001ABCD'
  AND timestamp_mesure BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY timestamp_mesure DESC;
-- Utilise: idx_piezo_chroniques_bss_date
```

### Avec PostGIS (rapide):

```sql
-- Stations dans rayon 10km de Tours (47.39, 0.69)
SELECT
    code_station,
    libelle_station,
    ST_Distance(geom::geography, ST_MakePoint(0.69, 47.39)::geography) / 1000 AS distance_km
FROM hubeau.piezometry_stations
WHERE ST_DWithin(geom::geography, ST_MakePoint(0.69, 47.39)::geography, 10000)
ORDER BY distance_km;
-- Utilise: idx_piezo_stations_geom
```

### Avec foreign keys (cohérence):

```sql
-- Cette requête échouera si station n'existe pas
INSERT INTO hubeau.piezometry_chroniques (code_bss, timestamp_mesure, niveau_nappe_ngf)
VALUES ('STATION_INEXISTANTE', NOW(), 100.5);
-- Erreur: FK constraint violation
```

## 📚 Ressources

- **PostgreSQL Index**: https://www.postgresql.org/docs/current/indexes.html
- **PostGIS Documentation**: https://postgis.net/documentation/
- **Foreign Keys**: https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK

## ❓ FAQ

**Q: Les scripts sont-ils idempotents?**
A: Oui, tous utilisent `IF NOT EXISTS` - on peut les relancer sans erreur.

**Q: Peut-on exécuter sur base en production?**
A: Oui pour script 01 (CONCURRENTLY). Prudence pour scripts 02-03 (tester d'abord).

**Q: Combien d'espace disque faut-il?**
A: Compter ~50-70% de taille actuelle en plus (index + geom).

**Q: Les index ralentissent les INSERT?**
A: Oui (~5-10%) mais gain en requêtes bien supérieur (10-1000x).

**Q: PostGIS est obligatoire?**
A: Non, optionnel. Nécessaire uniquement pour requêtes géospatiales.
