-- Nettoyage des schémas créés par erreur par dbt (préfixe public_)
-- À exécuter APRÈS validation du nouveau pipeline

-- Suppression des schémas "sales"
DROP SCHEMA IF EXISTS public_hubeau CASCADE;
DROP SCHEMA IF EXISTS public_staging CASCADE;
DROP SCHEMA IF EXISTS public_intermediate CASCADE;

-- Note: Le schéma "public" lui-même n'est pas supprimé car c'est le schéma par défaut de Postgres.
-- Les schémas propres sont maintenant :
-- - staging (géré par DLT)
-- - hubeau (géré par dbt marts + intermediate)
