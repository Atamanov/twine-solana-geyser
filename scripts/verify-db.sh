#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "Verifying Twine Geyser Database Setup"
echo "===================================="

# Check if container is running
if ! docker ps | grep -q twine-timescaledb; then
    echo -e "${RED}[ERROR]${NC} Database container is not running"
    exit 1
fi

echo -e "${GREEN}[OK]${NC} Database container is running"

# Check database connection
if docker exec twine-timescaledb pg_isready -U geyser_writer -d twine_solana_db &>/dev/null; then
    echo -e "${GREEN}[OK]${NC} Database is accepting connections"
else
    echo -e "${RED}[ERROR]${NC} Database is not ready"
    exit 1
fi

# List all tables
echo ""
echo "Database Tables:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "\dt"

# Check specific tables
echo ""
echo "Checking required tables..."

TABLES=("slots" "account_changes" "proof_requests" "geyser_stats" "monitored_accounts" "account_change_stats")

for table in "${TABLES[@]}"; do
    COUNT=$(docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '$table';")
    COUNT=$(echo $COUNT | tr -d ' ')
    
    if [ "$COUNT" = "1" ]; then
        echo -e "${GREEN}[OK]${NC} Table '$table' exists"
    else
        echo -e "${RED}[MISSING]${NC} Table '$table' not found"
    fi
done

# Check TimescaleDB hypertables
echo ""
echo "TimescaleDB Hypertables:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "SELECT hypertable_name FROM timescaledb_information.hypertables;"

# Check users
echo ""
echo "Database Users:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "\du"

echo ""
echo "Verification complete!"