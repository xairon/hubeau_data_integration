# PostgreSQL User Management - Migration Guide

## Overview

This guide explains how to enable **automatic PostgreSQL user creation** on your Hub'Eau pipeline installation.

**New feature**: The system now automatically creates 2 users at container startup:
1. **Main user** (read/write) - Configured via `PG_USER`/`PG_PASSWORD`
2. **Read-only user** (read-only) - Configured via `PG_READONLY_USER`/`PG_READONLY_PASSWORD`

---

## For New Installations

If you're setting up Hub'Eau for the first time, **no migration needed**! Just follow the standard installation:

```bash
# Clone repository
git clone <repo-url>
cd brgm

# Copy and configure .env
cp .env.example .env
nano .env  # Set PG_PASSWORD and PG_READONLY_PASSWORD

# Start services
docker compose up -d
```

The users will be created automatically on first PostgreSQL startup.

---

## For Existing Installations

If you already have a running Hub'Eau pipeline with data, follow these steps:

### Option A: Keep Existing Data + Add Read-Only User (RECOMMENDED)

This option **preserves all your data** and just adds the read-only user.

#### Step 1: Update `.env` File

```bash
# Edit your .env file
nano .env
```

Add these lines:
```env
# PostgreSQL Read-Only User (for Adminer, BI tools, etc.)
PG_READONLY_USER=readonly
PG_READONLY_PASSWORD=your_secure_readonly_password_here
```

#### Step 2: Update Docker Compose Configuration

```bash
# Pull latest changes (includes docker-compose.yml updates)
git pull origin main
```

Or manually update `docker-compose.yml` - the postgres section should include:
```yaml
postgres:
  environment:
    POSTGRES_PASSWORD: ${PG_PASSWORD:-REDACTED}
    POSTGRES_DB: ${PG_DB:-postgres}
    POSTGRES_USER: ${PG_USER:-postgres}
    PGOPTIONS: "-c hubeau.readonly_user=${PG_READONLY_USER:-readonly} -c hubeau.readonly_password=${PG_READONLY_PASSWORD:-readonly_default_pass_2024}"
```

#### Step 3: Create Read-Only User Manually

Since init scripts only run on **first startup**, we need to create the user manually:

```bash
# Connect to PostgreSQL as superuser
docker exec -it brgm-postgres psql -U postgres -d postgres
```

In the PostgreSQL prompt, run:
```sql
-- Create read-only user
CREATE USER readonly WITH PASSWORD 'your_secure_readonly_password_here';

-- Grant connect permission
GRANT CONNECT ON DATABASE postgres TO readonly;

-- Grant read-only permissions on public schema
GRANT USAGE ON SCHEMA public TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;

-- Grant read-only permissions on hubeau schema
GRANT USAGE ON SCHEMA hubeau TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA hubeau TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau GRANT SELECT ON TABLES TO readonly;

-- Grant read-only permissions on staging schema (DLT)
GRANT USAGE ON SCHEMA staging TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO readonly;

-- Verify user creation
\du

-- Exit
\q
```

#### Step 4: Test Read-Only User

```bash
# Test connection
docker exec -it brgm-postgres psql -U readonly -d postgres

# Should succeed (SELECT)
SELECT COUNT(*) FROM staging.piezometry_chroniques_raw;

# Should fail (INSERT)
INSERT INTO staging.piezometry_chroniques_raw DEFAULT VALUES;
-- Expected: ERROR: permission denied

# Exit
\q
```

✅ **Done!** Your data is preserved and read-only user is now available.

---

### Option B: Fresh Start (DELETES ALL DATA)

⚠️ **WARNING**: This option **DELETES ALL POSTGRESQL DATA**. Only use if:
- You can re-import/re-run your pipelines
- You have backups
- You're on a development environment

#### Step 1: Backup Data (Optional but Recommended)

```bash
# Backup entire database
docker exec -it brgm-postgres pg_dump -U postgres -d postgres -F c -f /tmp/backup.dump

# Copy backup to host
docker cp brgm-postgres:/tmp/backup.dump ./postgres_backup_$(date +%Y%m%d).dump
```

#### Step 2: Update `.env` File

```bash
nano .env
```

Add:
```env
PG_READONLY_USER=readonly
PG_READONLY_PASSWORD=your_secure_readonly_password_here
```

#### Step 3: Rebuild PostgreSQL

```bash
# Stop all services
docker compose down

# Delete PostgreSQL volume (⚠️ DELETES DATA)
docker volume rm brgm_postgres_data

# Optional: Also delete Dagster metadata (run history)
docker volume rm brgm_dagster_pg_data

# Pull latest changes
git pull origin main

# Rebuild and start
docker compose up -d
```

#### Step 4: Verify Users

```bash
# Check users were created
docker exec -it brgm-postgres psql -U postgres -d postgres -c "\du"
```

Expected output:
```
           List of roles
 Role name |  Attributes   | Member of
-----------+---------------+-----------
 postgres  | Superuser     | {}
 readonly  |               | {}
```

#### Step 5: Restore Data (If Backed Up)

```bash
# Copy backup into container
docker cp ./postgres_backup_20250104.dump brgm-postgres:/tmp/backup.dump

# Restore
docker exec -it brgm-postgres pg_restore -U postgres -d postgres -F c /tmp/backup.dump
```

✅ **Done!** Fresh PostgreSQL with automatic user management.

---

## Verification Checklist

After migration, verify everything works:

### 1. Check Users Exist

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "\du"
```

Should show both `postgres` and `readonly` users.

### 2. Test Main User (Read/Write)

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
CREATE TABLE staging.test_table (id INT);
INSERT INTO staging.test_table VALUES (1);
SELECT * FROM staging.test_table;
DROP TABLE staging.test_table;
"
```

Should succeed without errors.

### 3. Test Read-Only User (Read-Only)

```bash
# Should succeed
docker exec -it brgm-postgres psql -U readonly -d postgres -c "SELECT 1;"

# Should fail
docker exec -it brgm-postgres psql -U readonly -d postgres -c "CREATE TABLE staging.test_fail (id INT);"
```

Expected error: `permission denied to create table`

### 4. Test Adminer Access

- Open http://localhost:8081
- System: PostgreSQL
- Server: `postgres`
- Username: `readonly`
- Password: Your `PG_READONLY_PASSWORD`
- Database: `postgres`

Should connect successfully and show all tables (read-only access).

### 5. Test DLT Pipeline

```bash
# Restart worker to pick up latest code
docker compose restart dlt_worker

# Check Dagster UI
open http://localhost:8080

# Materialize an asset (e.g., piezometry_stations_raw)
```

Should run successfully and store data in PostgreSQL.

---

## Rollback (If Something Goes Wrong)

### Rollback Option A: Remove Read-Only User

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "DROP USER IF EXISTS readonly;"
```

### Rollback Option B: Restore From Backup

```bash
# Stop services
docker compose down

# Delete volume
docker volume rm brgm_postgres_data

# Recreate volume
docker volume create brgm_postgres_data

# Start PostgreSQL
docker compose up -d postgres

# Wait for healthy
docker compose ps

# Restore backup
docker cp ./postgres_backup_20250104.dump brgm-postgres:/tmp/backup.dump
docker exec -it brgm-postgres pg_restore -U postgres -d postgres -F c /tmp/backup.dump
```

---

## Troubleshooting

### Problem: "User already exists" Error

**Cause**: You tried Option B but the volume wasn't deleted.

**Solution**:
```bash
docker compose down
docker volume ls | grep postgres  # Check volume exists
docker volume rm brgm_postgres_data  # Force delete
docker compose up -d postgres
```

### Problem: Read-Only User Can't See Tables

**Cause**: Tables were created before user existed (no default privileges applied).

**Solution**: Re-grant permissions manually:
```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA hubeau TO readonly;
"
```

### Problem: "Permission Denied" When Using Main User

**Cause**: Wrong user or password in `.env`.

**Solution**: Check your `.env` file:
```bash
cat .env | grep PG_USER
cat .env | grep PG_PASSWORD
```

Ensure they match the PostgreSQL container environment.

---

## Best Practices

### Security

1. **Use strong passwords** (min 16 chars, mixed case, symbols)
2. **Different passwords** for main user vs read-only user
3. **Rotate passwords** every 90 days in production
4. **Use read-only user** for all BI tools, dashboards, Adminer

### Operations

1. **Document credentials** in your team's password manager (1Password, LastPass, etc.)
2. **Test backups regularly** (`pg_dump` monthly)
3. **Monitor user activity** (enable PostgreSQL audit logs in production)
4. **Principle of least privilege** (use read-only user whenever possible)

---

## FAQ

### Q: Can I change the read-only username?

**A**: Yes! Set `PG_READONLY_USER=myreaduser` in `.env` before first startup.

### Q: Can I have multiple read-only users?

**A**: Yes! Create additional users manually:
```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
CREATE USER analyst WITH PASSWORD 'secure_password';
GRANT USAGE ON SCHEMA staging TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO analyst;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO analyst;
"
```

### Q: What if I forget the read-only password?

**A**: Reset it as superuser:
```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
ALTER USER readonly WITH PASSWORD 'new_secure_password';
"
```

### Q: Does this work with PostgreSQL 15/17?

**A**: Yes! The init scripts are compatible with PostgreSQL 12+. Just change the image in `docker-compose.yml`.

---

## Summary

| Migration Path | Data Preserved | Downtime | Complexity |
|---------------|----------------|----------|------------|
| **Option A** (Manual user creation) | ✅ Yes | None | Low |
| **Option B** (Fresh start) | ❌ No (requires backup/restore) | ~5 min | Medium |

**Recommendation**: Use **Option A** for production, **Option B** for development.

---

## Support

If you encounter issues:

1. Check logs: `docker logs brgm-postgres`
2. Verify configuration: `docker compose config | grep -A 10 postgres`
3. Review init script logs: `docker exec -it brgm-postgres cat /var/log/postgresql/*.log`
4. Open an issue on GitHub with:
   - Error message
   - Docker logs
   - PostgreSQL version
   - Migration path chosen (A or B)

---

**Last Updated**: 2025-01-04
**Maintained By**: Hub'Eau Pipeline Team
