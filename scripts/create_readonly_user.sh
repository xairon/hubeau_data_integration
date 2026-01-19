#!/bin/bash
# ============================================================================
# Hub'Eau Pipeline - Create Read-Only PostgreSQL User
# ============================================================================
# This script creates a read-only PostgreSQL user on an existing database
# Use this for existing installations (migration Option A)
#
# Usage:
#   bash scripts/create_readonly_user.sh [username] [password]
#
# Examples:
#   bash scripts/create_readonly_user.sh                    # Uses .env defaults
#   bash scripts/create_readonly_user.sh readonly mypass123 # Custom credentials
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# Configuration
# ============================================================================

# Load .env file if exists
if [ -f .env ]; then
    echo -e "${GREEN}✓${NC} Found .env file, loading configuration..."
    export $(grep -v '^#' .env | xargs)
else
    echo -e "${YELLOW}⚠${NC}  No .env file found, using defaults"
fi

# Get username and password from arguments or .env
READONLY_USER="${1:-${PG_READONLY_USER:-readonly}}"
READONLY_PASSWORD="${2:-${PG_READONLY_PASSWORD:-}}"

# Container name
CONTAINER_NAME="${POSTGRES_CONTAINER:-brgm-postgres}"

# PostgreSQL superuser
PG_SUPERUSER="${PG_USER:-postgres}"
PG_DB="${PG_DB:-postgres}"

# ============================================================================
# Validation
# ============================================================================

echo ""
echo "=============================================="
echo "  PostgreSQL Read-Only User Creation"
echo "=============================================="
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${RED}✗${NC} Container ${CONTAINER_NAME} is not running"
    echo ""
    echo "Start PostgreSQL with:"
    echo "  docker compose up -d postgres"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓${NC} Container ${CONTAINER_NAME} is running"

# Check if password is set
if [ -z "$READONLY_PASSWORD" ]; then
    echo -e "${RED}✗${NC} Password not set"
    echo ""
    echo "Usage:"
    echo "  bash scripts/create_readonly_user.sh <username> <password>"
    echo ""
    echo "Or set PG_READONLY_PASSWORD in .env file"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓${NC} Configuration valid"
echo ""
echo "  Username: ${READONLY_USER}"
echo "  Password: $(echo "$READONLY_PASSWORD" | sed 's/./*/g')"
echo "  Container: ${CONTAINER_NAME}"
echo ""

# ============================================================================
# Confirmation
# ============================================================================

read -p "Create read-only user '${READONLY_USER}'? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}⚠${NC}  Aborted by user"
    exit 0
fi

# ============================================================================
# User Creation
# ============================================================================

echo ""
echo "Creating read-only user..."
echo ""

# SQL script to create user and grant permissions
SQL_SCRIPT=$(cat <<EOF
-- Check if user already exists
DO \$\$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${READONLY_USER}') THEN
        RAISE NOTICE '⚠️  User ${READONLY_USER} already exists, updating password...';
        EXECUTE format('ALTER USER %I WITH PASSWORD %L', '${READONLY_USER}', '${READONLY_PASSWORD}');
    ELSE
        RAISE NOTICE '✅ Creating user ${READONLY_USER}...';
        EXECUTE format('CREATE USER %I WITH PASSWORD %L', '${READONLY_USER}', '${READONLY_PASSWORD}');
    END IF;
END
\$\$;

-- Grant connect permission
GRANT CONNECT ON DATABASE ${PG_DB} TO ${READONLY_USER};

-- Grant read-only permissions on public schema
GRANT USAGE ON SCHEMA public TO ${READONLY_USER};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${READONLY_USER};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${READONLY_USER};

-- Grant read-only permissions on silver schema (if exists)
DO \$\$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'silver') THEN
        GRANT USAGE ON SCHEMA silver TO ${READONLY_USER};
        GRANT SELECT ON ALL TABLES IN SCHEMA silver TO ${READONLY_USER};
        ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO ${READONLY_USER};
        RAISE NOTICE '✅ Permissions granted on schema: silver';
    END IF;
END
\$\$;

-- Grant read-only permissions on gold schema (if exists)
DO \$\$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'gold') THEN
        GRANT USAGE ON SCHEMA gold TO ${READONLY_USER};
        GRANT SELECT ON ALL TABLES IN SCHEMA gold TO ${READONLY_USER};
        ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO ${READONLY_USER};
        RAISE NOTICE '✅ Permissions granted on schema: gold';
    END IF;
END
\$\$;

-- Grant read-only permissions on bronze schema (DLT default)
DO \$\$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        GRANT USAGE ON SCHEMA bronze TO ${READONLY_USER};
        GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO ${READONLY_USER};
        ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO ${READONLY_USER};
        RAISE NOTICE '✅ Permissions granted on schema: bronze';
    END IF;
END
\$\$;

-- Summary
DO \$\$
DECLARE
    table_count INT;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema IN ('public', 'bronze', 'silver', 'gold');

    RAISE NOTICE '================================================';
    RAISE NOTICE '✅ Read-only user created successfully';
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Username: ${READONLY_USER}';
    RAISE NOTICE 'Schemas: public, bronze, silver, gold';
    RAISE NOTICE 'Tables accessible: %', table_count;
    RAISE NOTICE 'Permissions: SELECT only (read-only)';
    RAISE NOTICE '================================================';
END
\$\$;
EOF
)

# Execute SQL script
if docker exec -i ${CONTAINER_NAME} psql -U ${PG_SUPERUSER} -d ${PG_DB} <<< "$SQL_SCRIPT"; then
    echo ""
    echo -e "${GREEN}✓${NC} User created successfully"
else
    echo ""
    echo -e "${RED}✗${NC} Failed to create user"
    exit 1
fi

# ============================================================================
# Verification
# ============================================================================

echo ""
echo "Verifying user..."
echo ""

# Test connection
if docker exec ${CONTAINER_NAME} psql -U ${READONLY_USER} -d ${PG_DB} -c "SELECT 1;" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} User can connect to database"
else
    echo -e "${RED}✗${NC} User cannot connect to database"
    exit 1
fi

# Test SELECT permission
if docker exec ${CONTAINER_NAME} psql -U ${READONLY_USER} -d ${PG_DB} -c "SELECT 1 AS test;" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} User can SELECT data"
else
    echo -e "${RED}✗${NC} User cannot SELECT data"
    exit 1
fi

# Test that CREATE is denied
if docker exec ${CONTAINER_NAME} psql -U ${READONLY_USER} -d ${PG_DB} -c "CREATE TABLE test_table (id INT);" > /dev/null 2>&1; then
    echo -e "${RED}✗${NC} User can CREATE tables (should be read-only!)"
    exit 1
else
    echo -e "${GREEN}✓${NC} User cannot CREATE tables (read-only confirmed)"
fi

# ============================================================================
# Summary
# ============================================================================

echo ""
echo "=============================================="
echo "  ✅ Read-Only User Setup Complete"
echo "=============================================="
echo ""
echo "Connection details:"
echo ""
echo "  Username:  ${READONLY_USER}"
echo "  Password:  $(echo "$READONLY_PASSWORD" | sed 's/./*/g')"
echo "  Database:  ${PG_DB}"
echo "  Host:      localhost (or postgres inside Docker network)"
echo "  Port:      5432"
echo ""
echo "Adminer (Web UI):"
echo ""
echo "  URL:       http://localhost:8081"
echo "  System:    PostgreSQL"
echo "  Server:    postgres"
echo "  Username:  ${READONLY_USER}"
echo "  Password:  <your password>"
echo "  Database:  ${PG_DB}"
echo ""
echo "Test connection:"
echo ""
echo "  docker exec -it ${CONTAINER_NAME} psql -U ${READONLY_USER} -d ${PG_DB}"
echo ""
echo "=============================================="
echo ""
