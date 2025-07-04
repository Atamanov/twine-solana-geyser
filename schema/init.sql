-- Create the database if it doesn't exist
-- This is usually handled by Docker, but included for completeness
-- CREATE DATABASE twine_solana_db;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create main slots table
CREATE TABLE IF NOT EXISTS slots (
    slot BIGINT PRIMARY KEY,
    bank_hash VARCHAR(44) NOT NULL,
    parent_bank_hash VARCHAR(44) NOT NULL,
    signature_count BIGINT NOT NULL,
    last_blockhash VARCHAR(44) NOT NULL,
    -- The cumulative LtHash after all changes in this slot are applied
    cumulative_lthash BYTEA NOT NULL,
    -- The LtHash of only the changes within this slot
    delta_lthash BYTEA NOT NULL,
    -- Additional hash components for auditing
    accounts_delta_hash VARCHAR(44), -- NULL after LtHash fork
    accounts_lthash_checksum VARCHAR(44), -- NULL before LtHash fork
    epoch_accounts_hash VARCHAR(44),
    -- Timestamp when the slot was rooted and this record was committed
    rooted_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert slots table to TimescaleDB hypertable
SELECT create_hypertable('slots', 'slot', 
    chunk_time_interval => 100000,
    if_not_exists => TRUE
);

-- Create account_changes table partitioned by slot
CREATE TABLE IF NOT EXISTS account_changes (
    slot BIGINT NOT NULL,
    account_pubkey VARCHAR(44) NOT NULL,
    write_version BIGINT NOT NULL,
    -- Old Account State
    old_lamports BIGINT NOT NULL,
    old_owner VARCHAR(44) NOT NULL,
    old_executable BOOLEAN NOT NULL,
    old_rent_epoch BIGINT NOT NULL,
    old_data BYTEA,
    old_lthash BYTEA NOT NULL,
    -- New Account State
    new_lamports BIGINT NOT NULL,
    new_owner VARCHAR(44) NOT NULL,
    new_executable BOOLEAN NOT NULL,
    new_rent_epoch BIGINT NOT NULL,
    new_data BYTEA,
    new_lthash BYTEA NOT NULL,
    -- Constraints and Indexing
    PRIMARY KEY (slot, account_pubkey, write_version)
);

-- Convert account_changes to TimescaleDB hypertable
SELECT create_hypertable('account_changes', 'slot',
    chunk_time_interval => 100000,
    if_not_exists => TRUE
);

-- Create proof_requests table
CREATE TABLE IF NOT EXISTS proof_requests (
    request_id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    slot BIGINT NOT NULL,
    account_pubkey VARCHAR(44) NOT NULL,
    -- Status to track the lifecycle of the request by the external prover
    status VARCHAR(20) DEFAULT 'pending' NOT NULL CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    -- Additional fields for processing
    assigned_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    retry_count INT DEFAULT 0
);

-- Create indexes for optimal performance
-- Slots table indexes
CREATE INDEX IF NOT EXISTS idx_slots_rooted_at ON slots (rooted_at DESC);
CREATE INDEX IF NOT EXISTS idx_slots_bank_hash ON slots (bank_hash);
CREATE INDEX IF NOT EXISTS idx_slots_parent_bank_hash ON slots (parent_bank_hash);

-- Account changes indexes - optimized for common queries
CREATE INDEX IF NOT EXISTS idx_account_changes_pubkey_slot ON account_changes (account_pubkey, slot DESC);
CREATE INDEX IF NOT EXISTS idx_account_changes_owner ON account_changes (new_owner, slot DESC);
CREATE INDEX IF NOT EXISTS idx_account_changes_slot_write_version ON account_changes (slot, write_version);

-- Proof requests indexes
CREATE INDEX IF NOT EXISTS idx_proof_requests_status ON proof_requests (status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_proof_requests_created ON proof_requests (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_proof_requests_account_slot ON proof_requests (account_pubkey, slot DESC);

-- Create compression policy for older data (optional, can be adjusted)
-- Compress chunks older than 7 days
SELECT add_compression_policy('slots', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('account_changes', INTERVAL '7 days', if_not_exists => TRUE);

-- Create retention policies (optional, adjust as needed)
-- Drop chunks older than 30 days
-- SELECT add_retention_policy('slots', INTERVAL '30 days', if_not_exists => TRUE);
-- SELECT add_retention_policy('account_changes', INTERVAL '30 days', if_not_exists => TRUE);

-- Create continuous aggregates for common queries (optional)
-- Example: Hourly slot statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS slot_stats_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket(INTERVAL '1 hour', to_timestamp(slot * 0.4)) AS hour,
    COUNT(*) as slot_count,
    SUM(signature_count) as total_signatures,
    AVG(signature_count) as avg_signatures_per_slot
FROM slots
GROUP BY hour
WITH NO DATA;

-- Refresh the continuous aggregate
SELECT add_continuous_aggregate_policy('slot_stats_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE
);

-- Create function for efficient batch inserts
CREATE OR REPLACE FUNCTION insert_account_changes_batch(
    p_changes JSONB
) RETURNS VOID AS $$
BEGIN
    INSERT INTO account_changes
    SELECT 
        (item->>'slot')::BIGINT,
        item->>'account_pubkey',
        (item->>'write_version')::BIGINT,
        (item->>'old_lamports')::BIGINT,
        item->>'old_owner',
        (item->>'old_executable')::BOOLEAN,
        (item->>'old_rent_epoch')::BIGINT,
        decode(item->>'old_data', 'base64'),
        decode(item->>'old_lthash', 'base64'),
        (item->>'new_lamports')::BIGINT,
        item->>'new_owner',
        (item->>'new_executable')::BOOLEAN,
        (item->>'new_rent_epoch')::BIGINT,
        decode(item->>'new_data', 'base64'),
        decode(item->>'new_lthash', 'base64')
    FROM jsonb_array_elements(p_changes) AS item
    ON CONFLICT (slot, account_pubkey, write_version) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Create database user for the geyser plugin
-- Note: Password should match the one in config.json.example and docker-compose.yml
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'geyser_writer') THEN
        CREATE USER geyser_writer WITH PASSWORD 'geyser_writer_password';
    END IF;
END $$;

-- Grant necessary permissions
GRANT CONNECT ON DATABASE twine_solana_db TO geyser_writer;
GRANT USAGE ON SCHEMA public TO geyser_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO geyser_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO geyser_writer;

-- Performance tuning recommendations (to be set in postgresql.conf or via ALTER SYSTEM)
-- These are suggestions and should be adjusted based on your hardware
/*
ALTER SYSTEM SET shared_buffers = '8GB';
ALTER SYSTEM SET effective_cache_size = '24GB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET min_wal_size = '1GB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;
ALTER SYSTEM SET max_worker_processes = 16;
ALTER SYSTEM SET max_parallel_workers_per_gather = 8;
ALTER SYSTEM SET max_parallel_workers = 16;
ALTER SYSTEM SET timescaledb.max_background_workers = 8;
*/

-- Create statistics tracking table for monitoring
CREATE TABLE IF NOT EXISTS geyser_stats (
    id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ DEFAULT NOW(),
    total_updates BIGINT,
    sealed_slots BIGINT,
    active_slots BIGINT,
    monitored_account_changes BIGINT,
    slots_with_monitored_accounts BIGINT,
    proof_requests_generated BIGINT,
    db_writes BIGINT
);

-- Index for stats queries
CREATE INDEX IF NOT EXISTS idx_geyser_stats_recorded_at ON geyser_stats (recorded_at DESC);