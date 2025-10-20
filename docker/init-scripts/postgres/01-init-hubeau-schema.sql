-- ==========================================
-- HUB'EAU SCHEMA INITIALIZATION
-- ==========================================
-- Ce script est OPTIONNEL car DLT crée automatiquement
-- le schéma au premier pipeline.run()
--
-- Il est fourni pour être explicite et permettre
-- de définir des permissions custom si nécessaire.
-- ==========================================

-- Créer le schéma Hub'Eau
CREATE SCHEMA IF NOT EXISTS hubeau;

-- Permissions pour l'utilisateur postgres
GRANT ALL ON SCHEMA hubeau TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA hubeau TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA hubeau TO postgres;

-- Permissions par défaut pour les futures tables
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
    GRANT ALL ON TABLES TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau
    GRANT ALL ON SEQUENCES TO postgres;

-- Activer l'extension PostGIS si besoin (pour colonnes géométrie)
CREATE EXTENSION IF NOT EXISTS postgis;

SELECT 'Schema hubeau initialized (DLT will create tables automatically)' AS status;
