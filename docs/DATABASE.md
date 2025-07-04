# TimescaleDB Setup for Twine Geyser Plugin

This document describes the high-performance TimescaleDB setup for the Twine Solana Geyser Plugin.

## Quick Start

1. (Optional) Configure passwords:
   ```bash
   cp .env.example .env
   # Edit .env to set custom passwords
   ```

2. Start the database:
   ```bash
   ./scripts/start-db.sh
   ```

3. The database will be automatically initialized with:
   - TimescaleDB extension
   - Optimized schema with hypertables
   - Proper indexes for high-performance queries
   - Compression policies for older data
   - User credentials for the Geyser plugin

## Architecture

### Why TimescaleDB?

TimescaleDB is chosen for this project because:
- **Time-series optimization**: Slots are naturally time-ordered data
- **Automatic partitioning**: Hypertables automatically partition data by slot
- **Compression**: Older data can be automatically compressed (up to 95% compression ratio)
- **Parallel queries**: Native support for parallel query execution
- **Continuous aggregates**: Pre-computed views for common queries
- **Data retention**: Automatic cleanup of old data

### Schema Design

1. **`slots` table**: Stores all slot information
   - Hypertable partitioned by slot number
   - Compressed after 7 days
   - Indexes on bank_hash, parent_bank_hash, and rooted_at

2. **`account_changes` table**: Stores account state transitions
   - Hypertable partitioned by slot number
   - Composite primary key (slot, account_pubkey, write_version)
   - Optimized indexes for account history queries
   - Compressed after 7 days

3. **`proof_requests` table**: Work queue for proof generation
   - Regular table (not a hypertable)
   - Status-based workflow management
   - Indexes for efficient job polling

4. **`geyser_stats` table**: Plugin performance metrics
   - Time-series data for monitoring
   - Can be used with Grafana for visualization

### Performance Optimizations

1. **Hypertables**: Automatic partitioning by slot improves query performance and maintenance
2. **Compression**: Reduces storage by 90-95% for older data
3. **Indexes**: Carefully chosen for common query patterns
4. **Parallel execution**: Configured for up to 12 parallel workers
5. **Connection pooling**: Uses deadpool-postgres in the plugin
6. **Batch operations**: Plugin uses COPY commands for bulk inserts

## Configuration

### PostgreSQL Tuning

The `config/postgresql.conf` file contains optimized settings for:
- 16GB+ RAM systems
- NVMe SSD storage
- High-throughput write workloads
- Parallel query execution

Key settings:
- `shared_buffers = 4GB` (25% of RAM)
- `effective_cache_size = 12GB` (75% of RAM)
- `max_wal_size = 4GB`
- `work_mem = 256MB`
- `max_parallel_workers = 12`

### Docker Resource Limits

The Docker Compose file sets:
- CPU: 8 cores limit, 4 cores reserved
- Memory: 16GB limit, 8GB reserved
- Shared memory: 1GB

Adjust these based on your hardware.

## Monitoring

### With Docker Compose Profiles

1. Start with monitoring stack:
   ```bash
   docker-compose --profile monitoring up -d
   ```

2. Access services:
   - **Grafana**: http://localhost:3000 (admin/admin_password from .env)
     - Pre-configured "Twine Geyser Monitoring" dashboard
     - Real-time plugin metrics from Prometheus
     - Database statistics from TimescaleDB
   - **Prometheus**: http://localhost:9090
     - Collects metrics from Geyser plugin (port 9091)
     - PostgreSQL exporter metrics
   - **PgAdmin**: http://localhost:5050 (admin@twine.com/admin_password from .env)
     - Database administration interface

### Key Metrics to Monitor

1. **Database metrics** (via postgres-exporter):
   - Connection count
   - Transaction rate
   - Cache hit ratio
   - Replication lag
   - Table bloat

2. **TimescaleDB specific**:
   - Chunk count per hypertable
   - Compression ratio
   - Continuous aggregate lag

3. **Geyser plugin metrics** (from geyser_stats table):
   - Total updates processed
   - Slots processed per second
   - Account changes per second
   - Queue depth
   - Worker pool utilization

## Maintenance

### Compression

Compression is automatically applied to chunks older than 7 days. To manually compress:

```sql
SELECT compress_chunk(c) 
FROM show_chunks('account_changes', older_than => INTERVAL '1 day') c;
```

### Retention

To enable automatic data retention (e.g., keep only 30 days):

```sql
SELECT add_retention_policy('account_changes', INTERVAL '30 days');
SELECT add_retention_policy('slots', INTERVAL '30 days');
```

### Continuous Aggregates

The schema includes an example continuous aggregate for hourly slot statistics. To refresh manually:

```sql
CALL refresh_continuous_aggregate('slot_stats_hourly', NULL, NULL);
```

### Backup

For production use, set up regular backups:

```bash
# Backup
docker-compose exec -T timescaledb pg_dump -U postgres twine_solana_db | gzip > backup.sql.gz

# Restore
gunzip -c backup.sql.gz | docker-compose exec -T timescaledb psql -U postgres twine_solana_db
```

## Troubleshooting

### Check database logs
```bash
docker-compose logs -f timescaledb
```

### Connect to database
```bash
docker-compose exec timescaledb psql -U postgres -d twine_solana_db
```

### Check table sizes
```sql
SELECT hypertable_name, 
       pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name))) as size,
       pg_size_pretty(compression_chunk_size(format('%I.%I', hypertable_schema, hypertable_name))) as compressed_size
FROM timescaledb_information.hypertables;
```

### Check chunk information
```sql
SELECT * FROM timescaledb_information.chunks 
WHERE hypertable_name = 'account_changes' 
ORDER BY range_start DESC 
LIMIT 10;
```

## Production Considerations

1. **Hardware**: Use NVMe SSDs for best performance
2. **Replication**: Set up streaming replication for HA
3. **Monitoring**: Use Prometheus + Grafana for production monitoring
4. **Backups**: Implement regular backup strategy
5. **Security**: 
   - Change default passwords
   - Use SSL connections
   - Implement network isolation
6. **Scaling**: Consider using Citus for horizontal scaling if needed