#!/bin/bash

echo "=== Checking Database Health ==="

# Check Docker volume disk usage
echo -e "\n1. Docker Volume Size:"
docker system df -v | grep timescale_data

# Check disk usage inside container
echo -e "\n2. Database Disk Usage:"
docker exec twine-timescaledb df -h /var/lib/postgresql/data

# Check database sizes
echo -e "\n3. Database Sizes:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    pg_database.datname as database,
    pg_size_pretty(pg_database_size(pg_database.datname)) as size
FROM pg_database
ORDER BY pg_database_size(pg_database.datname) DESC;"

# Check table sizes
echo -e "\n4. Table Sizes (Top 10):"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    schemaname || '.' || tablename AS table,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) AS indexes_size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;"

# Check hypertable chunk information
echo -e "\n5. Hypertable Chunks:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    hypertable_name,
    COUNT(*) as num_chunks,
    pg_size_pretty(SUM(total_bytes)) as total_size,
    pg_size_pretty(SUM(total_bytes - index_bytes)) as data_size,
    pg_size_pretty(SUM(index_bytes)) as index_size
FROM timescaledb_information.chunks
GROUP BY hypertable_name
ORDER BY SUM(total_bytes) DESC;"

# Check compression status
echo -e "\n6. Compression Status:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    hypertable_name,
    COUNT(*) FILTER (WHERE is_compressed) as compressed_chunks,
    COUNT(*) FILTER (WHERE NOT is_compressed) as uncompressed_chunks,
    pg_size_pretty(SUM(CASE WHEN is_compressed THEN total_bytes ELSE 0 END)) as compressed_size,
    pg_size_pretty(SUM(CASE WHEN NOT is_compressed THEN total_bytes ELSE 0 END)) as uncompressed_size
FROM timescaledb_information.chunks
GROUP BY hypertable_name;"

# Check row counts
echo -e "\n7. Row Counts:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    'slots' as table_name, 
    COUNT(*) as row_count,
    MIN(slot) as min_slot,
    MAX(slot) as max_slot
FROM slots
UNION ALL
SELECT 
    'account_changes' as table_name, 
    COUNT(*) as row_count,
    MIN(slot) as min_slot,
    MAX(slot) as max_slot
FROM account_changes
UNION ALL
SELECT 
    'vote_transactions' as table_name, 
    COUNT(*) as row_count,
    MIN(slot) as min_slot,
    MAX(slot) as max_slot
FROM vote_transactions;"

# Check for bloat
echo -e "\n8. Table Bloat Check:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    schemaname || '.' || tablename AS table,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
    n_dead_tup AS dead_tuples,
    n_live_tup AS live_tuples,
    ROUND(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_percent
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY n_dead_tup DESC
LIMIT 10;"

# Check WAL size
echo -e "\n9. WAL Size:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) as total_wal_generated,
    pg_size_pretty(sum(size)) as wal_directory_size
FROM pg_ls_waldir();"

# Check for long-running queries
echo -e "\n10. Long Running Queries:"
docker exec twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
SELECT 
    pid,
    now() - pg_stat_activity.query_start AS duration,
    query,
    state
FROM pg_stat_activity
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes'
AND query NOT LIKE '%pg_stat_activity%'
ORDER BY duration DESC;"