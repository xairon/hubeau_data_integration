@echo off
REM ============================================================================
REM Script d'initialisation des volumes Docker externes (Windows)
REM DOIT être exécuté AVANT le premier "docker compose up"
REM ============================================================================
REM Protection maximale contre suppression accidentelle des données
REM Les volumes externes ne sont PAS supprimés par "docker compose down -v"
REM ============================================================================

echo.
echo 🔧 Initialisation des volumes Docker externes pour Hub'Eau Pipeline...
echo.

REM Fonction pour créer un volume s'il n'existe pas
set "volume_name=brgm_postgres_data"
set "description=Base de données PostgreSQL/TimescaleDB"
call :create_volume

set "volume_name=brgm_dagster_pg_data"
set "description=Métadonnées Dagster"
call :create_volume

echo.
echo ✅ Tous les volumes sont prêts!
echo.
echo 📋 Volumes créés:
docker volume ls | findstr "brgm_"
echo.
echo 🚀 Vous pouvez maintenant lancer: docker compose up -d
echo.
echo ⚠️  IMPORTANT - Protection des données:
echo    - Les volumes NE SERONT PAS supprimés par 'docker compose down -v'
echo    - Pour supprimer les volumes: docker volume rm brgm_postgres_data brgm_dagster_pg_data
echo    - Backup recommandé: docker exec brgm-postgres pg_dump -U postgres postgres ^> backup.sql
echo.
goto :eof

:create_volume
docker volume inspect %volume_name% >nul 2>&1
if %errorlevel% equ 0 (
    echo ⚠️  Volume %volume_name% existe déjà (aucune action^)
) else (
    echo 📦 Création du volume: %volume_name% (%description%^)...
    docker volume create %volume_name%
    if %errorlevel% equ 0 (
        echo ✅ Volume %volume_name% créé
    ) else (
        echo ❌ Erreur lors de la création du volume %volume_name%
    )
)
goto :eof
