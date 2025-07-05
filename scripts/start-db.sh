#!/bin/bash

# Start TimescaleDB with Docker Compose

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "Starting TimescaleDB for Twine Solana Geyser Plugin..."

# Check if .env exists, if not copy from example
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo "Creating .env from .env.example..."
    cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
    echo "NOTE: Using default passwords. For production, update .env with secure passwords!"
fi

# Create necessary directories
mkdir -p config/grafana/provisioning/{dashboards,datasources}
mkdir -p config/prometheus

# Start only the database by default
docker-compose up -d timescaledb

echo "Waiting for TimescaleDB to be ready..."
sleep 5

# Check if database is ready
until docker-compose exec -T timescaledb pg_isready -U postgres -d twine_solana_db > /dev/null 2>&1; do
    echo "Waiting for database..."
    sleep 2
done

echo "TimescaleDB is ready!"
echo ""
echo "Connection details:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: twine_solana_db"
echo "  User: geyser_writer"
echo "  Password: geyser_writer_password"
echo ""
echo "To start with monitoring stack (Grafana, Prometheus, PgAdmin):"
echo "  docker-compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f timescaledb"
echo ""
echo "To stop:"
echo "  docker-compose down"
echo ""
echo "To stop and remove data:"
echo "  docker-compose down -v"