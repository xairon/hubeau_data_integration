# PostgreSQL Initialization Scripts

## Overview

This directory contains SQL scripts that run **automatically** when the PostgreSQL container starts for the **first time** (empty `postgres_data` volume).

Scripts are executed in **alphabetical order** by the PostgreSQL Docker entrypoint.

## Script Execution Order

1. **`00_create_users.sql`** - Creates PostgreSQL users
2. **`01_init_minimal.sql`** - Creates schemas and sets permissions
3. **`02-postgresql-optimization.sql`** - Performance tuning
4. **`99-verify-initialization.sql`** - Verification checks

---

## User Management

### Automatic User Creation

The system automatically creates **2 users** at container initialization:

#### 1. Main User (Read/Write)
- **Username**: Configured via `PG_USER` env var (default: `postgres`)
- **Password**: Configured via `PG_PASSWORD` env var
- **Permissions**: Full access (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP)
- **Schemas**: `public`, `hubeau`, `staging`
- **Use case**: DLT pipeline, data loading, maintenance

#### 2. Read-Only User
- **Username**: Configured via `PG_READONLY_USER` env var (default: `readonly`)
- **Password**: Configured via `PG_READONLY_PASSWORD` env var
- **Permissions**: Read-only access (SELECT only)
- **Schemas**: `public`, `hubeau`, `staging`
- **Use case**: Adminer, BI tools, data visualization, reporting

---

## Configuration

### 1. Create/Update `.env` file

```bash
# Copy template
cp .env.example .env

# Edit with your credentials
nano .env
```

### 2. Set User Credentials

```env
# Main user (read/write)
PG_USER=postgres
PG_PASSWORD=your_secure_main_password

# Read-only user
PG_READONLY_USER=readonly
PG_READONLY_PASSWORD=your_secure_readonly_password
```

### 3. Start PostgreSQL Container

```bash
# First startup (empty volume) - scripts will run automatically
docker compose up -d postgres
```

---

## Verification

### Check Users Were Created

```bash
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

### Test Main User Permissions

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "SELECT current_user, has_schema_privilege('staging', 'CREATE');"
```

Expected: `has_schema_privilege = t` (true)

### Test Read-Only User Permissions

```bash
# Should succeed (SELECT)
docker exec -it brgm-postgres psql -U readonly -d postgres -c "SELECT COUNT(*) FROM staging.piezometry_chroniques_raw;"

# Should fail (INSERT)
docker exec -it brgm-postgres psql -U readonly -d postgres -c "INSERT INTO staging.piezometry_chroniques_raw DEFAULT VALUES;"
```

Expected error: `permission denied for table piezometry_chroniques_raw`

---

## Connecting with Adminer

### Main User (Read/Write)
- **URL**: http://localhost:8081
- **System**: PostgreSQL
- **Server**: `postgres`
- **Username**: `postgres` (or your `PG_USER`)
- **Password**: Your `PG_PASSWORD`
- **Database**: `postgres`

### Read-Only User (Recommended for Viewing)
- **URL**: http://localhost:8081
- **System**: PostgreSQL
- **Server**: `postgres`
- **Username**: `readonly` (or your `PG_READONLY_USER`)
- **Password**: Your `PG_READONLY_PASSWORD`
- **Database**: `postgres`

---

## Troubleshooting

### Users Not Created

**Problem**: Users not created after container startup

**Solution**: Scripts only run on **first startup** (empty volume). To re-run:

```bash
# Stop container
docker compose down

# Delete PostgreSQL volume (⚠️ DELETES ALL DATA)
docker volume rm brgm_postgres_data

# Restart (scripts will run)
docker compose up -d postgres
```

### Forgot Read-Only Password

**Solution**: Recreate user manually

```bash
# Connect as superuser
docker exec -it brgm-postgres psql -U postgres -d postgres

# Drop and recreate user
DROP USER IF EXISTS readonly;
CREATE USER readonly WITH PASSWORD 'new_secure_password';

-- Grant permissions
GRANT USAGE ON SCHEMA staging TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO readonly;

GRANT USAGE ON SCHEMA hubeau TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA hubeau TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA hubeau GRANT SELECT ON TABLES TO readonly;
```

### Read-Only User Can't See Tables

**Problem**: User exists but `SELECT * FROM staging.table` returns "permission denied"

**Solution**: Re-grant permissions after DLT creates new tables

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA hubeau TO readonly;
"
```

**Note**: The `ALTER DEFAULT PRIVILEGES` statements ensure new tables get permissions automatically, but existing tables need explicit grants.

---

## Security Best Practices

### Password Strength

✅ **Good passwords**:
- Min 16 characters
- Mix of uppercase, lowercase, numbers, symbols
- No dictionary words
- Example: `Kp9#mL2$xR5@vN8!`

❌ **Bad passwords**:
- `password`, `123456`, `admin`
- Short passwords (< 12 chars)
- Personal information (birthdays, names)

### Production Recommendations

1. **Use secrets management**: AWS Secrets Manager, HashiCorp Vault, Azure Key Vault
2. **Rotate passwords regularly**: Every 90 days minimum
3. **Enable TLS**: Force encrypted connections only
4. **Restrict network access**: Firewall rules, VPC isolation
5. **Audit logs**: Enable PostgreSQL audit extension (pgAudit)
6. **Principle of least privilege**: Use read-only user for BI tools

### Development vs Production

| Environment | Main User | Read-Only User | TLS | Network |
|-------------|-----------|----------------|-----|---------|
| **Development** | Simple password OK | Simple password OK | Optional | localhost:5432 |
| **Production** | Strong password + rotation | Strong password + rotation | **Required** | Internal VPC only |

---

## Architecture

### User Permissions Matrix

| Schema | Main User (postgres) | Read-Only User (readonly) |
|--------|---------------------|---------------------------|
| `public` | Full (CREATE, DROP, SELECT, INSERT, UPDATE, DELETE) | Read-only (SELECT) |
| `hubeau` | Full (CREATE, DROP, SELECT, INSERT, UPDATE, DELETE) | Read-only (SELECT) |
| `staging` | Full (CREATE, DROP, SELECT, INSERT, UPDATE, DELETE) | Read-only (SELECT) |

### Why 2 Users?

**Separation of concerns**:
- **Main user**: For data pipelines that need to write data
- **Read-only user**: For dashboards/tools that should only read data

**Security benefits**:
- Accidental data deletion prevented
- Audit trail (separate users in logs)
- Principle of least privilege

---

## References

- [PostgreSQL User Management](https://www.postgresql.org/docs/current/user-manag.html)
- [PostgreSQL Default Privileges](https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html)
- [Docker PostgreSQL Image](https://hub.docker.com/_/postgres)
- [PostGIS Documentation](https://postgis.net/documentation/)

---

**Last Updated**: 2025-01-04
**Maintained By**: Hub'Eau Pipeline Team
