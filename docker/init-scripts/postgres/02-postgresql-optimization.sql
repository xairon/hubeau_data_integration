-- Configuration PostgreSQL optimisée pour éviter les problèmes de mémoire
-- et les modes recovery répétitifs
-- Ce script sera exécuté après la création du schéma

-- ═══════════════════════════════════════════════════════════
-- MÉMOIRE ET PERFORMANCE
-- ═══════════════════════════════════════════════════════════

-- Limite la mémoire partagée pour éviter les OOM
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET work_mem = '4MB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';

-- ═══════════════════════════════════════════════════════════
-- WAL (Write-Ahead Logging) - CRITIQUE pour éviter les corruptions
-- ═══════════════════════════════════════════════════════════

-- Réduire la taille des segments WAL pour éviter les corruptions
ALTER SYSTEM SET max_wal_size = '1GB';
ALTER SYSTEM SET min_wal_size = '80MB';

-- Configuration WAL pour la robustesse
ALTER SYSTEM SET wal_level = 'replica';
ALTER SYSTEM SET wal_compression = 'on';
ALTER SYSTEM SET wal_buffers = '16MB';

-- ═══════════════════════════════════════════════════════════
-- CHECKPOINTS - Éviter les problèmes de récupération
-- ═══════════════════════════════════════════════════════════

-- Checkpoints plus fréquents mais moins coûteux
ALTER SYSTEM SET checkpoint_timeout = '5min';
ALTER SYSTEM SET checkpoint_completion_target = '0.7';
ALTER SYSTEM SET checkpoint_flush_after = '256kB';

-- ═══════════════════════════════════════════════════════════
-- CONNEXIONS ET TIMEOUTS
-- ═══════════════════════════════════════════════════════════

ALTER SYSTEM SET max_connections = '100';
ALTER SYSTEM SET statement_timeout = '30min';
ALTER SYSTEM SET idle_in_transaction_session_timeout = '10min';

-- ═══════════════════════════════════════════════════════════
-- LOGGING - Pour diagnostiquer les problèmes
-- ═══════════════════════════════════════════════════════════

ALTER SYSTEM SET log_min_duration_statement = '1000';
ALTER SYSTEM SET log_checkpoints = 'on';
ALTER SYSTEM SET log_connections = 'on';
ALTER SYSTEM SET log_disconnections = 'on';
ALTER SYSTEM SET log_lock_waits = 'on';

-- ═══════════════════════════════════════════════════════════
-- AUTOVACUUM - Maintenance automatique
-- ═══════════════════════════════════════════════════════════

ALTER SYSTEM SET autovacuum = 'on';
ALTER SYSTEM SET autovacuum_max_workers = '3';
ALTER SYSTEM SET autovacuum_naptime = '1min';

-- Recharger la configuration
SELECT pg_reload_conf();
