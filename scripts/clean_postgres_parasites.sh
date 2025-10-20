#!/bin/bash
# Script pour nettoyer les tables parasites DLT dans PostgreSQL
# Supprime les tables avec __geometry__coordinates et autres tables DLT parasites

set -e

echo "🧹 NETTOYAGE DES TABLES PARASITES DLT"
echo "======================================"
echo ""
echo "⚠️  ATTENTION: Ce script va supprimer:"
echo "   - Toutes les tables avec '__geometry__coordinates'"
echo "   - Toutes les tables avec '__codes_' et '__libelles_'"
echo "   - Les schémas parasites comme 'hubeau_staging'"
echo "   - Les tables DLT système dans le mauvais schéma"
echo ""
read -p "Êtes-vous sûr? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Annulé."
    exit 1
fi

echo ""
echo "🔍 Détection de l'environnement..."

# Détection de l'environnement
if [ -f /.dockerenv ]; then
    # Dans Docker
    PSQL="psql -U postgres -d postgres"
    echo "📍 Mode: Conteneur Docker"
else
    # Local ou via docker exec
    if docker ps | grep -q "postgres\|brgm-postgres"; then
        PSQL="docker exec -i postgres psql -U postgres -d postgres 2>/dev/null || docker exec -i brgm-postgres psql -U postgres -d postgres"
        echo "📍 Mode: Docker exec (local)"
    else
        echo "❌ Aucun conteneur PostgreSQL trouvé"
        echo "Veuillez démarrer le conteneur PostgreSQL avant d'exécuter ce script."
        exit 1
    fi
fi

echo ""
echo "🗑️  Suppression des tables parasites..."

# Supprimer les tables avec __geometry__coordinates
echo "   - Tables __geometry__coordinates..."
$PSQL -c "
DO \$\$ 
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE tablename LIKE '%__geometry__coordinates%'
        AND schemaname = 'hubeau'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Supprimé: %.%', r.schemaname, r.tablename;
    END LOOP;
END \$\$;
"

# Supprimer les tables avec __codes_ et __libelles_
echo "   - Tables __codes_ et __libelles_..."
$PSQL -c "
DO \$\$ 
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE (tablename LIKE '%__codes_%' OR tablename LIKE '%__libelles_%')
        AND schemaname = 'hubeau'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Supprimé: %.%', r.schemaname, r.tablename;
    END LOOP;
END \$\$;
"

# Supprimer les schémas parasites
echo "   - Schémas parasites..."
$PSQL -c "DROP SCHEMA IF EXISTS hubeau_staging CASCADE;"
$PSQL -c "DROP SCHEMA IF EXISTS staging CASCADE;"

# Nettoyer les tables DLT système dans le mauvais schéma
echo "   - Tables DLT système parasites..."
$PSQL -c "
DO \$\$ 
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE tablename LIKE '_dlt_%'
        AND schemaname = 'hubeau'
    ) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Supprimé: %.%', r.schemaname, r.tablename;
    END LOOP;
END \$\$;
"

echo ""
echo "✅ Nettoyage terminé!"
echo ""
echo "📋 Vérification des tables restantes:"
$PSQL -c "
SELECT schemaname, tablename 
FROM pg_tables 
WHERE schemaname = 'hubeau' 
ORDER BY tablename;
"

echo ""
echo "🎯 Prochaines étapes:"
echo "   1. Redémarrer les services Docker"
echo "   2. Relancer les jobs Dagster"
echo "   3. DLT créera maintenant des tables propres"
echo ""
