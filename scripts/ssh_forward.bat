@echo off
REM SSH Port Forwarding Script - Hub'Eau Data Integration (Windows)
REM Convention: Local ports = Server ports with prefix '1'
REM
REM Usage:
REM   scripts\ssh_forward.bat
REM
REM Then access:
REM   - Dagster UI:  http://localhost:18080
REM   - Adminer:     http://localhost:18081
REM   - Portainer:   http://localhost:19000
REM   - PostgreSQL:  localhost:15432

set SERVER=dib-2019006065
set USER=ringuet

echo.
echo ====================================================
echo   SSH Port Forwarding - Hub'Eau Data Integration
echo ====================================================
echo.
echo Server: %SERVER%
echo User:   %USER%
echo.
echo Port mappings (Local -^> Remote):
echo    18080 -^> 8080  (Dagster UI)
echo    18081 -^> 8081  (Adminer - PostgreSQL Web UI)
echo    19000 -^> 9000  (Portainer - Docker Management)
echo    15432 -^> 5432  (PostgreSQL Direct Connection)
echo.
echo Access URLs:
echo    Dagster:    http://localhost:18080
echo    Adminer:    http://localhost:18081
echo    Portainer:  http://localhost:19000
echo.
echo PostgreSQL credentials:
echo    Host:     localhost:15432
echo    User:     postgres
echo    Password: REDACTED
echo    Database: postgres
echo.
echo Press Ctrl+C to stop forwarding
echo ====================================================
echo.

ssh -L 18080:localhost:8080 -L 18081:localhost:8081 -L 19000:localhost:9000 -L 15432:localhost:5432 %USER%@%SERVER%
