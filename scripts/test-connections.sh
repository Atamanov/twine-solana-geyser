#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "Testing Database Connections"
echo "============================"

# Test variables
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="twine_solana_db"
DB_USER="geyser_writer"
DB_PASS="geyser_writer_password"

# Test 1: Container running
echo -n "1. Docker container running: "
if docker ps | grep -q twine-timescaledb; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC} - Container not running"
    exit 1
fi

# Test 2: Port accessible
echo -n "2. Port 5432 accessible: "
if (echo > /dev/tcp/localhost/5432) &>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC} - Port not accessible"
    exit 1
fi

# Test 3: Database ready
echo -n "3. Database ready: "
if docker exec twine-timescaledb pg_isready -U $DB_USER -d $DB_NAME &>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC} - Database not ready"
    exit 1
fi

# Test 4: Can connect with geyser_writer
echo -n "4. Can connect as geyser_writer: "
if PGPASSWORD=$DB_PASS psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT 1;" &>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC} - Cannot connect"
    
    # Try from within container
    echo "   Trying from within container..."
    if docker exec twine-timescaledb psql -U $DB_USER -d $DB_NAME -c "SELECT 1;" &>/dev/null; then
        echo -e "   ${YELLOW}Container connection works - host connection issue${NC}"
    else
        echo -e "   ${RED}Container connection also fails${NC}"
    fi
fi

# Test 5: Check user permissions
echo -n "5. User has proper permissions: "
RESULT=$(docker exec twine-timescaledb psql -U $DB_USER -d $DB_NAME -t -c "SELECT rolsuper FROM pg_roles WHERE rolname = '$DB_USER';" 2>/dev/null | tr -d ' ')
if [ "$RESULT" = "t" ]; then
    echo -e "${GREEN}PASS${NC} (superuser)"
else
    echo -e "${YELLOW}WARNING${NC} - Not a superuser"
fi

# Test 6: Check tables exist
echo -n "6. Required tables exist: "
TABLE_COUNT=$(docker exec twine-timescaledb psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('slots', 'account_changes', 'monitored_accounts', 'account_change_stats');" 2>/dev/null | tr -d ' ')
if [ "$TABLE_COUNT" = "4" ]; then
    echo -e "${GREEN}PASS${NC} (4/4 tables)"
else
    echo -e "${RED}FAIL${NC} - Only $TABLE_COUNT/4 tables found"
    
    # List what tables exist
    echo "   Existing tables:"
    docker exec twine-timescaledb psql -U $DB_USER -d $DB_NAME -c "\dt" 2>/dev/null | grep -E "slots|account_changes|monitored_accounts|account_change_stats"
fi

# Test 7: Check TimescaleDB extension
echo -n "7. TimescaleDB extension: "
EXT=$(docker exec twine-timescaledb psql -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb';" 2>/dev/null | tr -d ' ')
if [ "$EXT" = "1" ]; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL${NC} - TimescaleDB not installed"
fi

# Test 8: Test Grafana connection
echo -n "8. Grafana datasource test: "
if curl -s -u admin:admin http://localhost:3000/api/datasources/name/TimescaleDB &>/dev/null; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${YELLOW}WARNING${NC} - Grafana not accessible or datasource not configured"
fi

echo ""
echo "Connection test complete!"