# Twine Solana Geyser Plugin

A high-performance Geyser plugin for Solana with synchronized airlock pattern and TimescaleDB backend.

## Features

- **Synchronized Airlock Pattern**: Thread-safe data collection with minimal contention
- **High-performance data ingestion**: Lock-free data structures and atomic operations
- **Conditional storage**: Only stores slots containing monitored account changes
- **TimescaleDB backend**: Automatic partitioning, compression, and time-series optimization
- **Proof scheduling**: Configurable debouncing for proof generation requests
- **Comprehensive monitoring**: Atomic performance counters with periodic logging
- **Zero-copy design**: Direct ownership transfer for optimal performance
- **Enhanced notifications**: Support for LtHash and bank hash components

## Architecture

The plugin uses a multi-stage architecture:

1. **Airlock System**: Lock-free ingestion with atomic reference counting
2. **Conditional Storage**: Only persists slots with monitored account changes
3. **Worker Pool**: Async database operations with connection pooling
4. **Proof Scheduler**: Debounced scheduling service for proof requests

## Quick Start

### 1. Start the Database

```bash
./scripts/start-db.sh
```

This starts a high-performance TimescaleDB instance with:
- Optimized PostgreSQL configuration
- Schema initialization with hypertables
- Compression policies
- Performance indexes

### 2. Configure the Plugin

```bash
cp config.json.example config.json
```

Edit `config.json` (default passwords are aligned across all config files):
```json
{
  "libpath": "/path/to/twine_geyser_plugin.so",
  "monitored_accounts": [
    "11111111111111111111111111111111",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
  ],
  "max_slots_tracked": 100,
  "enable_lthash_notifications": true,
  "db_host": "localhost",
  "db_port": 5432,
  "db_user": "geyser_writer",
  "db_password": "geyser_writer_password",
  "db_name": "twine_solana_db",
  "num_worker_threads": 4,
  "db_connections_per_worker": 2,
  "max_queue_size": 10000,
  "batch_size": 1000,
  "batch_timeout_ms": 100,
  "proof_scheduling_slot_interval": 10,
  "metrics_port": 9091,
  "network_mode": "mainnet"
}
```

**Network Mode Options:**
- `"mainnet"`: Connect to mainnet-beta RPC for network slot monitoring
- `"devnet"`: Connect to devnet RPC for network slot monitoring

### 3. Build the Plugin

```bash
cargo build --release
```

### 4. Add to Validator

Add to your validator configuration:

```yaml
geyser_plugins:
  - name: twine_geyser_plugin
    libpath: /path/to/target/release/libtwine_solana_geyser.so
    config: /path/to/config.json
```

## Monitoring

### Plugin Metrics

The plugin exposes Prometheus metrics on port 9091 (configurable):

**Core Metrics:**
- `twine_geyser_total_updates`: Total account updates processed
- `twine_geyser_sealed_slots`: Total sealed slots
- `twine_geyser_active_slots`: Currently active slots
- `twine_geyser_monitored_account_changes`: Total monitored account changes
- `twine_geyser_proof_requests_generated`: Total proof requests generated
- `twine_geyser_db_writes`: Total database writes

**Queue Metrics:**
- `twine_geyser_queue_depth`: Current database queue depth
- `twine_geyser_queue_capacity`: Maximum queue capacity
- `twine_geyser_worker_pool_size`: Number of worker threads

**Chain Synchronization:**
- `twine_geyser_validator_slot`: Current slot being processed by validator
- `twine_geyser_network_slot`: Latest slot on the network (devnet/mainnet)

**Memory Metrics:**
- `twine_geyser_memory_rss_bytes`: Resident set size in bytes
- `twine_geyser_memory_virtual_bytes`: Virtual memory size in bytes

### Plugin Statistics

The plugin logs comprehensive statistics every 100 rooted slots to the log file.

### Monitoring Stack

Start the full monitoring stack:
```bash
docker-compose --profile monitoring up -d
```

This includes:
- **TimescaleDB**: High-performance time-series database
- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization dashboards
- **PgAdmin**: Database administration

Access:
- **Grafana**: http://localhost:3000 (credentials from .env)
  - Pre-configured dashboard: "Twine Geyser Monitoring"
  - Real-time metrics from both Prometheus and TimescaleDB
- **Prometheus**: http://localhost:9090
  - Scrapes metrics from the Geyser plugin
- **PgAdmin**: http://localhost:5050 (credentials from .env)

## Performance Tuning

### Database
- Uses TimescaleDB hypertables for automatic partitioning
- Compression reduces storage by 90-95% after 7 days
- Optimized indexes for common query patterns
- Connection pooling with deadpool-postgres

### Plugin
- Lock-free data structures (SegQueue, DashMap)
- Atomic operations for thread safety
- Zero-copy ownership transfer
- Batch database operations

## Documentation

- [Database Setup Guide](docs/DATABASE.md) - Detailed TimescaleDB configuration
- [Architecture](geyser_db.md) - Design decisions and implementation details

## Development

### Requirements
- Rust 1.70+
- Docker & Docker Compose
- PostgreSQL client tools (optional)

### Testing
```bash
cargo test
```

### Building with Features
```bash
# The plugin requires enhanced notifications from agave
cargo build --release
```

## License

[Your License Here]