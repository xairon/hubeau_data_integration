@echo off
echo ============================================
echo   ARRET ENVIRONNEMENT LOCAL HUB'EAU
echo ============================================
echo.

echo [1/2] Arret des containers...
docker-compose -f docker-compose.local.yml down

echo.
echo [2/2] Nettoyage optionnel...
choice /C YN /M "Voulez-vous supprimer les volumes de donnees"
if %errorlevel%==1 (
    echo Suppression des volumes...
    docker volume rm hubeau_postgres_data_local hubeau_dagster_storage_local 2>nul
    echo Volumes supprimes.
) else (
    echo Volumes conserves.
)

echo.
echo ============================================
echo   ENVIRONNEMENT ARRETE
echo ============================================
echo.
pause