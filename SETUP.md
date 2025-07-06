# Twine Solana Geyser Plugin - Setup Guide

## Quick Start

### Complete Fresh Setup

```bash
# 1. Clean everything (WARNING: Deletes all data)
./setup.sh clean

# 2. Start database
./setup.sh up

# 3. Build the plugin
cargo build --release
# or
./setup.sh build

# 4. (Optional) Start monitoring
./setup.sh monitoring

# 5. Check status
./setup.sh status
```

That's it! The database will be automatically initialized with:
- TimescaleDB extension enabled
- All required tables created
- `geyser_writer` user with correct permissions
- Compression and performance optimizations applied

## Database Connection

After running `make up`, connect with:
- Host: `localhost`
- Port: `5432`
- Database: `twine_solana_db`
- User: `geyser_writer`
- Password: `geyser_writer_password`

## Monitoring

After running `make monitoring`:
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

## Commands

### Using the setup script:
```bash
./setup.sh up         # Start database
./setup.sh down       # Stop all services
./setup.sh clean      # Remove everything (data included)
./setup.sh logs       # View database logs
./setup.sh build      # Build the plugin
./setup.sh monitoring # Start Grafana and Prometheus
./setup.sh status     # Show service status
```


## Remote Server Setup

On your remote server:

```bash
# Copy updated files
cd ~/alex/twine-solana-geyser

# Clean and start fresh
./setup.sh clean
./setup.sh up

# The plugin binary location
target/release/libtwine_solana_geyser.so
```

## Troubleshooting

### Database connection issues
```bash
# Check if database is running
docker ps | grep twine-timescaledb

# Check logs
make logs

# Test connection
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "SELECT 1"
```

### Metrics not showing in Grafana
- Ensure the Geyser plugin is running and exposing metrics on port 9091
- Check Prometheus targets at http://localhost:9090/targets
- The dashboard uses only Prometheus metrics, not database queries