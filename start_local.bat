@echo off
echo ============================================
echo   DEMARRAGE ENVIRONNEMENT LOCAL HUB'EAU
echo ============================================
echo.

REM Verifier que Docker est lance
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Docker n'est pas lance !
    echo Veuillez demarrer Docker Desktop puis relancer ce script.
    pause
    exit /b 1
)

echo [1/6] Arret des anciens containers...
docker-compose -f docker-compose.local.yml down

echo.
echo [2/6] Nettoyage des volumes si demande...
choice /C YN /M "Voulez-vous supprimer les donnees existantes (reset complet)"
if %errorlevel%==1 (
    echo Suppression des volumes...
    docker volume rm hubeau_postgres_data_local hubeau_dagster_storage_local 2>nul
)

echo.
echo [3/6] Build des images Docker...
docker-compose -f docker-compose.local.yml build

echo.
echo [4/6] Demarrage de PostgreSQL...
docker-compose -f docker-compose.local.yml up -d postgres
timeout /t 10 /nobreak >nul

echo.
echo [5/6] Demarrage des services Dagster...
docker-compose -f docker-compose.local.yml up -d

echo.
echo [6/6] Attente du demarrage complet...
timeout /t 15 /nobreak >nul

echo.
echo ============================================
echo   ENVIRONNEMENT LOCAL DEMARRE !
echo ============================================
echo.
echo URLs disponibles:
echo   - Dagster UI:    http://localhost:3001
echo   - PostgreSQL:    localhost:5433
echo.
echo Commandes utiles:
echo   - Voir les logs:     docker-compose -f docker-compose.local.yml logs -f
echo   - Arreter tout:      docker-compose -f docker-compose.local.yml down
echo   - Redemarrer:        docker-compose -f docker-compose.local.yml restart
echo.
echo Pour acceder a Dagster UI, ouvrez: http://localhost:3001
echo.
pause