@echo off
REM ============================================================================
REM ERA5 Time Series Extraction - Windows Wrapper
REM ============================================================================
REM Extracts ERA5 NetCDF data from PostgreSQL bytea to normalized time series
REM
REM Usage:
REM   scripts\run_era5_extraction.bat [OPTIONS]
REM
REM Examples:
REM   REM Extract all files
REM   scripts\run_era5_extraction.bat
REM
REM   REM Extract specific file
REM   scripts\run_era5_extraction.bat --file-id era5_france_2020_2021
REM ============================================================================

echo.
echo ==============================================
echo   ERA5 Time Series Extraction
echo ==============================================
echo.

REM Check if Docker is running
docker ps >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running
    echo.
    echo Please start Docker Desktop and try again.
    exit /b 1
)

REM Check if postgres container is running
docker ps --format "{{.Names}}" | findstr /C:"brgm-postgres" >nul
if errorlevel 1 (
    echo ERROR: PostgreSQL container is not running
    echo.
    echo Start PostgreSQL with:
    echo   docker compose up -d postgres
    exit /b 1
)

echo [OK] Docker and PostgreSQL are running
echo.
echo Running extraction inside dlt_worker container...
echo.
echo This will:
echo   1. Read NetCDF files from staging.era5_france_meteo_raw (bytea)
echo   2. Extract time series data (time, lat, lon, temp, precip, evap)
echo   3. Insert into staging.era5_france_timeseries (normalized table)
echo.
echo Expected:
echo   - ~38 files to process
echo   - ~7.3M rows per file
echo   - ~277M total rows
echo   - ~1-2 minutes per file
echo   - ~1 hour total runtime
echo.
echo WARNING: This is a long-running operation. Press Ctrl+C to cancel.
echo.
timeout /t 3 >nul

REM Run extraction script inside dlt_worker container
docker exec -it brgm-dlt-worker python /app/scripts/extract_era5_timeseries.py %*

echo.
echo ==============================================
echo [OK] Extraction Complete
echo ==============================================
echo.
echo New table: staging.era5_france_timeseries
echo.
echo Now you can browse this table in Adminer!
echo.
