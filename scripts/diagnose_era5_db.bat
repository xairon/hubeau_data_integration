@echo off
REM ============================================================================
REM Diagnose ERA5 tables on a remote Postgres (no server install; Docker only)
REM ============================================================================
REM Usage:
REM   scripts\diagnose_era5_db.bat localhost 5432 postgres %PG_PASSWORD% postgres
REM
REM Notes:
REM - This runs a one-shot postgres client container to execute SQL.
REM - Works if Docker Desktop is running on your machine.

set HOST=%1
set PORT=%2
set USERNAME=%3
set PASSWORD=%4
set DBNAME=%5

if "%HOST%"=="" (
  echo Usage: scripts\diagnose_era5_db.bat HOST PORT USER PASSWORD DB
  exit /b 1
)

echo.
echo == Indexes on staging.era5_france_timeseries ==
docker run --rm -e PGPASSWORD=%PASSWORD% postgres:16 ^
  psql -h %HOST% -p %PORT% -U %USERNAME% -d %DBNAME% ^
  -c "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='staging' AND tablename='era5_france_timeseries' ORDER BY indexname;"

echo.
echo == Does 2006 exist in staging.era5_france_timeseries? ==
docker run --rm -e PGPASSWORD=%PASSWORD% postgres:16 ^
  psql -h %HOST% -p %PORT% -U %USERNAME% -d %DBNAME% ^
  -c "SELECT EXISTS (SELECT 1 FROM staging.era5_france_timeseries WHERE time >= '2006-01-01' AND time < '2007-01-01' LIMIT 1) AS has_2006;"

echo.
echo == Raw chunk covering 2006 (staging.era5_france_meteo_raw) ==
docker run --rm -e PGPASSWORD=%PASSWORD% postgres:16 ^
  psql -h %HOST% -p %PORT% -U %USERNAME% -d %DBNAME% ^
  -c "SELECT file_id, start_year, end_year, file_size_mb, download_timestamp FROM staging.era5_france_meteo_raw WHERE start_year <= 2006 AND end_year >= 2006 ORDER BY start_year;"

echo.
echo == If chunk exists, was it extracted? (counts by source_file_id) ==
docker run --rm -e PGPASSWORD=%PASSWORD% postgres:16 ^
  psql -h %HOST% -p %PORT% -U %USERNAME% -d %DBNAME% ^
  -c "SELECT source_file_id, COUNT(*)::bigint AS rows, MIN(time) AS min_time, MAX(time) AS max_time FROM staging.era5_france_timeseries WHERE source_file_id IN (SELECT file_id FROM staging.era5_france_meteo_raw WHERE start_year <= 2006 AND end_year >= 2006) GROUP BY source_file_id ORDER BY source_file_id;"

echo.
echo ✅ Done.

