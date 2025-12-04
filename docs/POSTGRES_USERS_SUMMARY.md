# PostgreSQL User Management - Quick Reference

## Summary

The Hub'Eau pipeline now supports **automatic creation of 2 PostgreSQL users**:

1. **Main user** (`postgres` by default) - Read/Write access for DLT pipeline
2. **Read-only user** (`readonly` by default) - Read-only access for Adminer, BI tools

---

## Quick Setup (New Installation)

```bash
# 1. Configure credentials in .env
cp .env.example .env
nano .env  # Set PG_READONLY_USER and PG_READONLY_PASSWORD

# 2. Start PostgreSQL
docker compose up -d postgres

# 3. Verify users created
docker exec -it brgm-postgres psql -U postgres -d postgres -c "\du"
```

---

## Quick Setup (Existing Installation)

### Option A: Keep Data + Add Read-Only User

```bash
# 1. Update .env
nano .env  # Add PG_READONLY_USER and PG_READONLY_PASSWORD

# 2. Run helper script
bash scripts/create_readonly_user.sh

# 3. Verify
docker exec -it brgm-postgres psql -U readonly -d postgres -c "SELECT 1;"
```

### Option B: Fresh Start (Deletes Data)

```bash
# 1. Backup (optional)
docker exec -it brgm-postgres pg_dump -U postgres -d postgres -F c -f /tmp/backup.dump
docker cp brgm-postgres:/tmp/backup.dump ./postgres_backup.dump

# 2. Update .env
nano .env  # Add PG_READONLY_USER and PG_READONLY_PASSWORD

# 3. Rebuild
docker compose down
docker volume rm brgm_postgres_data
docker compose up -d postgres

# 4. Restore (optional)
docker cp ./postgres_backup.dump brgm-postgres:/tmp/backup.dump
docker exec -it brgm-postgres pg_restore -U postgres -d postgres -F c /tmp/backup.dump
```

---

## Configuration

### .env Variables

```env
# Main user (read/write)
PG_USER=postgres
PG_PASSWORD=your_secure_main_password

# Read-only user
PG_READONLY_USER=readonly
PG_READONLY_PASSWORD=your_secure_readonly_password
```

### Default Values (if not set)

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_USER` | `postgres` | Main user (read/write) |
| `PG_PASSWORD` | `REDACTED` | Main user password |
| `PG_READONLY_USER` | `readonly` | Read-only user |
| `PG_READONLY_PASSWORD` | `readonly_default_pass_2024` | Read-only password |

---

## User Permissions

### Main User (postgres)

| Action | public | hubeau | staging |
|--------|--------|--------|---------|
| SELECT | ✅ | ✅ | ✅ |
| INSERT | ✅ | ✅ | ✅ |
| UPDATE | ✅ | ✅ | ✅ |
| DELETE | ✅ | ✅ | ✅ |
| CREATE | ✅ | ✅ | ✅ |
| DROP | ✅ | ✅ | ✅ |

**Use case**: DLT pipeline, data loading, maintenance, schema changes

### Read-Only User (readonly)

| Action | public | hubeau | staging |
|--------|--------|--------|---------|
| SELECT | ✅ | ✅ | ✅ |
| INSERT | ❌ | ❌ | ❌ |
| UPDATE | ❌ | ❌ | ❌ |
| DELETE | ❌ | ❌ | ❌ |
| CREATE | ❌ | ❌ | ❌ |
| DROP | ❌ | ❌ | ❌ |

**Use case**: Adminer, BI tools (Tableau, Power BI), data visualization, reporting

---

## Connection Strings

### Main User (Read/Write)

```bash
# Docker exec
docker exec -it brgm-postgres psql -U postgres -d postgres

# Connection string
postgresql://postgres:<password>@localhost:5432/postgres

# Adminer
http://localhost:8081
  Server: postgres
  Username: postgres
  Password: <your password>
  Database: postgres
```

### Read-Only User (Read-Only)

```bash
# Docker exec
docker exec -it brgm-postgres psql -U readonly -d postgres

# Connection string
postgresql://readonly:<password>@localhost:5432/postgres

# Adminer
http://localhost:8081
  Server: postgres
  Username: readonly
  Password: <your password>
  Database: postgres
```

---

## Common Commands

### Check Users Exist

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "\du"
```

### Test Main User

```bash
# Should succeed
docker exec -it brgm-postgres psql -U postgres -d postgres -c "SELECT 1;"
docker exec -it brgm-postgres psql -U postgres -d postgres -c "CREATE TABLE staging.test (id INT);"
```

### Test Read-Only User

```bash
# Should succeed
docker exec -it brgm-postgres psql -U readonly -d postgres -c "SELECT 1;"
docker exec -it brgm-postgres psql -U readonly -d postgres -c "SELECT COUNT(*) FROM staging.piezometry_chroniques_raw;"

# Should fail (permission denied)
docker exec -it brgm-postgres psql -U readonly -d postgres -c "CREATE TABLE staging.test (id INT);"
```

### Reset Read-Only Password

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "ALTER USER readonly WITH PASSWORD 'new_password';"
```

### Grant Permissions to Existing Tables

```bash
# If tables were created before read-only user existed
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA hubeau TO readonly;
"
```

---

## Troubleshooting

### Users Not Created

**Problem**: `\du` shows only `postgres` user

**Solution**: Scripts only run on first startup (empty volume)

```bash
# Option 1: Run manual script
bash scripts/create_readonly_user.sh

# Option 2: Rebuild (deletes data)
docker compose down
docker volume rm brgm_postgres_data
docker compose up -d postgres
```

### Can't Connect with Read-Only User

**Problem**: `psql: FATAL: password authentication failed`

**Solution**: Check `.env` file

```bash
cat .env | grep PG_READONLY
# Should show: PG_READONLY_USER=readonly
#              PG_READONLY_PASSWORD=<your password>

# Reset password
docker exec -it brgm-postgres psql -U postgres -d postgres -c "ALTER USER readonly WITH PASSWORD 'new_password';"
```

### Read-Only User Can't See Tables

**Problem**: `SELECT * FROM staging.table` returns "permission denied"

**Solution**: Re-grant permissions

```bash
docker exec -it brgm-postgres psql -U postgres -d postgres -c "
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA hubeau TO readonly;
"
```

---

## Security Best Practices

### Password Strength

✅ **Good**: `Kp9#mL2$xR5@vN8!` (16+ chars, mixed case, symbols)
❌ **Bad**: `password`, `123456`, `readonly`

### Production Checklist

- [ ] Use strong passwords (16+ chars)
- [ ] Different passwords for main vs read-only user
- [ ] Store passwords in secrets manager (Vault, AWS Secrets Manager)
- [ ] Enable TLS/SSL for PostgreSQL connections
- [ ] Rotate passwords every 90 days
- [ ] Use read-only user for all BI tools
- [ ] Enable PostgreSQL audit logs (pgAudit)
- [ ] Restrict network access (VPC, firewall rules)

---

## Files Created

| File | Description |
|------|-------------|
| `docker/init-scripts/postgres/00_create_users.sql` | Automatic user creation (runs on first startup) |
| `docker/init-scripts/postgres/01_init_minimal.sql` | Schema creation + permissions |
| `docker/init-scripts/postgres/README.md` | Init scripts documentation |
| `scripts/create_readonly_user.sh` | Helper script for existing installations |
| `docs/POSTGRES_USER_MIGRATION.md` | Detailed migration guide |
| `docs/POSTGRES_USERS_SUMMARY.md` | This file (quick reference) |

---

## Reference Documentation

- [PostgreSQL User Management](https://www.postgresql.org/docs/current/user-manag.html)
- [Docker PostgreSQL Image](https://hub.docker.com/_/postgres)
- [DLT Best Practices](./DLT_BEST_PRACTICES.md)
- [Migration Guide](./POSTGRES_USER_MIGRATION.md)

---

**Last Updated**: 2025-01-04
**Maintained By**: Hub'Eau Pipeline Team
