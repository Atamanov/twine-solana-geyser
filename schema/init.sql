-- Create the database if it doesn't exist
-- This is usually handled by Docker, but included for completeness
-- CREATE DATABASE twine_solana_db;

-- Note: When using geyser_writer as the main database user (POSTGRES_USER),
-- these user creation and grant statements are not needed.
-- They are kept here for reference if using a different setup.

-- Skip user creation if we're already running as geyser_writer
DO
$do$
BEGIN
   -- Only try to create geyser_writer if we're running as a superuser
   IF current_user != 'geyser_writer' AND EXISTS (
      SELECT 1 FROM pg_roles WHERE rolname = current_user AND rolsuper = true
   ) THEN
      IF NOT EXISTS (
         SELECT FROM pg_catalog.pg_roles
         WHERE  rolname = 'geyser_writer') THEN

         CREATE ROLE geyser_writer LOGIN PASSWORD 'geyser_writer_password';
      END IF;
      
      -- Grant privileges to the new user
      GRANT CONNECT ON DATABASE twine_solana_db TO geyser_writer;
      GRANT USAGE ON SCHEMA public TO geyser_writer;
      GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO geyser_writer;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO geyser_writer;
      GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO geyser_writer;
      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO geyser_writer;
   END IF;
END
$do$;

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
    -- Slot status tracking
    status VARCHAR(20) DEFAULT 'first_shred_received' NOT NULL CHECK (status IN ('first_shred_received', 'completed', 'processed', 'confirmed', 'rooted')),
    -- Timestamps for status changes
    first_shred_received_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    confirmed_at TIMESTAMPTZ,
    -- Timestamp when the slot was rooted and this record was committed
    rooted_at TIMESTAMPTZ DEFAULT NOW(),
    -- Block metadata fields
    blockhash VARCHAR(44),
    parent_slot BIGINT,
    executed_transaction_count BIGINT,
    entry_count BIGINT,
    block_metadata_updated_at TIMESTAMPTZ
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
CREATE INDEX IF NOT EXISTS idx_slots_status ON slots (status);
CREATE INDEX IF NOT EXISTS idx_slots_processed_at ON slots (processed_at DESC) WHERE processed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_slots_confirmed_at ON slots (confirmed_at DESC) WHERE confirmed_at IS NOT NULL;

-- Account changes indexes - optimized for common queries
CREATE INDEX IF NOT EXISTS idx_account_changes_pubkey_slot ON account_changes (account_pubkey, slot DESC);
CREATE INDEX IF NOT EXISTS idx_account_changes_owner ON account_changes (new_owner, slot DESC);
CREATE INDEX IF NOT EXISTS idx_account_changes_slot_write_version ON account_changes (slot, write_version);

-- Proof requests indexes
CREATE INDEX IF NOT EXISTS idx_proof_requests_status ON proof_requests (status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_proof_requests_created ON proof_requests (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_proof_requests_account_slot ON proof_requests (account_pubkey, slot DESC);

-- Enable compression on hypertables first
ALTER TABLE slots SET (timescaledb.compress, timescaledb.compress_segmentby = 'slot');
ALTER TABLE account_changes SET (timescaledb.compress, timescaledb.compress_segmentby = 'account_pubkey', timescaledb.compress_orderby = 'slot DESC');

-- Create compression policy for older data (optional, can be adjusted)
-- Compress chunks older than 7 days
SELECT add_compression_policy('slots', INTERVAL '7 days');
SELECT add_compression_policy('account_changes', INTERVAL '7 days');

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
    schedule_interval => INTERVAL '30 minutes'
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

-- Create table for monitored accounts configuration
CREATE TABLE IF NOT EXISTS monitored_accounts (
    account_pubkey VARCHAR(88) PRIMARY KEY,
    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_slot BIGINT,
    last_seen_at TIMESTAMPTZ,
    total_changes_tracked BIGINT DEFAULT 0,
    total_data_bytes BIGINT DEFAULT 0,
    active BOOLEAN DEFAULT true,
    metadata JSONB
);

CREATE INDEX idx_monitored_accounts_active ON monitored_accounts(active);
CREATE INDEX idx_monitored_accounts_last_seen ON monitored_accounts(last_seen_at);

-- Create table for account change statistics
CREATE TABLE IF NOT EXISTS account_change_stats (
    account_pubkey VARCHAR(88) NOT NULL,
    slot BIGINT NOT NULL,
    change_count INTEGER NOT NULL DEFAULT 1,
    old_data_size BIGINT NOT NULL,
    new_data_size BIGINT NOT NULL,
    total_data_size BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_pubkey, slot)
);

CREATE INDEX idx_account_change_stats_slot ON account_change_stats(slot);
CREATE INDEX idx_account_change_stats_pubkey ON account_change_stats(account_pubkey);
CREATE INDEX idx_account_change_stats_created ON account_change_stats(created_at DESC);

-- Convert to hypertable for time-series efficiency
SELECT create_hypertable('account_change_stats', 'created_at', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE);

-- Set up retention policy for 7 days on account change stats
SELECT add_retention_policy('account_change_stats', INTERVAL '7 days');

-- Create table for vote transactions
CREATE TABLE IF NOT EXISTS vote_transactions (
    slot BIGINT NOT NULL,
    voter_pubkey VARCHAR(88) NOT NULL,
    vote_signature VARCHAR(88) NOT NULL,
    vote_transaction BYTEA NOT NULL,  -- Serialized transaction
    transaction_meta JSONB,           -- Transaction metadata (compute units, etc)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (slot, voter_pubkey, vote_signature)
);

-- Convert to hypertable for time-series efficiency
SELECT create_hypertable('vote_transactions', 'slot', 
    chunk_time_interval => 100000,
    if_not_exists => TRUE);

-- Create indexes for vote transactions
CREATE INDEX idx_vote_transactions_voter ON vote_transactions(voter_pubkey, slot DESC);
CREATE INDEX idx_vote_transactions_signature ON vote_transactions(vote_signature);
CREATE INDEX idx_vote_transactions_created ON vote_transactions(created_at DESC);

-- Create table for epoch validator stakes
CREATE TABLE IF NOT EXISTS epoch_stakes (
    epoch BIGINT NOT NULL,
    validator_pubkey VARCHAR(88) NOT NULL,
    stake_amount BIGINT NOT NULL,
    stake_percentage NUMERIC(5,2),  -- Percentage of total stake
    total_epoch_stake BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (epoch, validator_pubkey)
);

-- Create indexes for epoch stakes
CREATE INDEX idx_epoch_stakes_validator ON epoch_stakes(validator_pubkey, epoch DESC);
CREATE INDEX idx_epoch_stakes_amount ON epoch_stakes(stake_amount DESC);

-- Add vote transaction columns to slots table
ALTER TABLE slots ADD COLUMN IF NOT EXISTS vote_count INTEGER DEFAULT 0;
ALTER TABLE slots ADD COLUMN IF NOT EXISTS vote_transactions JSONB;

-- Create table for stake account states
CREATE TABLE IF NOT EXISTS stake_account_states (
    slot BIGINT NOT NULL,
    stake_pubkey VARCHAR(88) NOT NULL,
    voter_pubkey VARCHAR(88),
    stake_amount BIGINT NOT NULL,
    activation_epoch BIGINT,
    deactivation_epoch BIGINT,
    credits_observed BIGINT,
    rent_exempt_reserve BIGINT,
    staker VARCHAR(88),
    withdrawer VARCHAR(88),
    state_type VARCHAR(20) NOT NULL, -- 'uninitialized', 'initialized', 'stake', 'rewards_pool'
    lthash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (slot, stake_pubkey)
);

-- Convert to hypertable
SELECT create_hypertable('stake_account_states', 'slot', 
    chunk_time_interval => 100000,
    if_not_exists => TRUE);

-- Create indexes
CREATE INDEX idx_stake_states_voter ON stake_account_states(voter_pubkey, slot DESC);
CREATE INDEX idx_stake_states_epoch ON stake_account_states(activation_epoch);
CREATE INDEX idx_stake_states_created ON stake_account_states(created_at DESC);

-- Create table for epoch validator sets (computed at epoch boundaries)
CREATE TABLE IF NOT EXISTS epoch_validator_sets (
    epoch BIGINT NOT NULL,
    epoch_start_slot BIGINT NOT NULL,
    epoch_end_slot BIGINT NOT NULL,
    validator_pubkey VARCHAR(88) NOT NULL,
    total_stake BIGINT NOT NULL,
    stake_percentage NUMERIC(5,2),
    total_epoch_stake BIGINT NOT NULL,
    computed_at_slot BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (epoch, validator_pubkey)
);

-- Create indexes
CREATE INDEX idx_epoch_validator_sets_validator ON epoch_validator_sets(validator_pubkey, epoch DESC);
CREATE INDEX idx_epoch_validator_sets_stake ON epoch_validator_sets(total_stake DESC);

-- Create view for recent votes with stake information
CREATE OR REPLACE VIEW recent_votes_with_stakes AS
WITH latest_epoch AS (
    SELECT MAX(epoch) as current_epoch FROM epoch_validator_sets
),
latest_votes AS (
    SELECT DISTINCT ON (voter_pubkey)
        slot,
        voter_pubkey,
        vote_signature,
        created_at
    FROM vote_transactions
    WHERE created_at > NOW() - INTERVAL '1 hour'
    ORDER BY voter_pubkey, slot DESC
)
SELECT 
    lv.slot,
    lv.voter_pubkey,
    lv.vote_signature,
    lv.created_at,
    COALESCE(evs.total_stake, 0) as validator_stake,
    COALESCE(evs.stake_percentage, 0) as stake_percentage,
    evs.epoch,
    CASE 
        WHEN evs.validator_pubkey IS NOT NULL THEN 'active'
        ELSE 'inactive'
    END as validator_status
FROM latest_votes lv
CROSS JOIN latest_epoch le
LEFT JOIN epoch_validator_sets evs ON lv.voter_pubkey = evs.validator_pubkey 
    AND evs.epoch = le.current_epoch
ORDER BY lv.slot DESC;

-- Create materialized view for vote participation stats
CREATE MATERIALIZED VIEW IF NOT EXISTS vote_participation_stats AS
WITH time_windows AS (
    SELECT 
        '5 minutes' as "window",
        NOW() - INTERVAL '5 minutes' as start_time
    UNION ALL
    SELECT 
        '30 minutes' as "window",
        NOW() - INTERVAL '30 minutes' as start_time
    UNION ALL
    SELECT 
        '1 hour' as "window",
        NOW() - INTERVAL '1 hour' as start_time
),
latest_epoch_data AS (
    SELECT 
        epoch,
        validator_pubkey,
        total_stake,
        stake_percentage,
        total_epoch_stake
    FROM epoch_validator_sets
    WHERE epoch = (SELECT MAX(epoch) FROM epoch_validator_sets)
),
vote_counts AS (
    SELECT 
        tw."window",
        vt.voter_pubkey,
        COUNT(*) as vote_count,
        MAX(vt.slot) as last_vote_slot
    FROM time_windows tw
    CROSS JOIN vote_transactions vt
    WHERE vt.created_at >= tw.start_time
    GROUP BY tw."window", vt.voter_pubkey
)
SELECT 
    vc."window",
    COUNT(DISTINCT vc.voter_pubkey) as voting_validators,
    COUNT(DISTINCT led.validator_pubkey) as total_validators,
    SUM(CASE WHEN vc.voter_pubkey IS NOT NULL THEN led.total_stake ELSE 0 END) as voting_stake,
    MAX(led.total_epoch_stake) as total_epoch_stake,
    ROUND(
        100.0 * SUM(CASE WHEN vc.voter_pubkey IS NOT NULL THEN led.total_stake ELSE 0 END) / 
        NULLIF(MAX(led.total_epoch_stake), 0), 
        2
    ) as voting_stake_percentage,
    NOW() as last_updated
FROM vote_counts vc
RIGHT JOIN latest_epoch_data led ON vc.voter_pubkey = led.validator_pubkey
GROUP BY vc."window"
ORDER BY vc."window";

-- Create index for materialized view refresh
CREATE INDEX idx_vote_participation_stats_window ON vote_participation_stats("window");

-- Refresh policy for materialized view
CREATE OR REPLACE FUNCTION refresh_vote_participation_stats()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY vote_participation_stats;
END;
$$ LANGUAGE plpgsql;

-- Create a scheduled job to refresh the materialized view every minute
-- Note: Requires pg_cron extension - uncomment if available
-- CREATE EXTENSION IF NOT EXISTS pg_cron;
-- SELECT cron.schedule('refresh-vote-stats', '* * * * *', 'SELECT refresh_vote_participation_stats();');

-- Alternative: Create a trigger to refresh on new vote transactions
CREATE OR REPLACE FUNCTION trigger_refresh_vote_stats()
RETURNS trigger AS $$
BEGIN
    -- Only refresh if enough time has passed since last refresh
    IF NOT EXISTS (
        SELECT 1 FROM vote_participation_stats 
        WHERE last_updated > NOW() - INTERVAL '30 seconds'
    ) THEN
        PERFORM refresh_vote_participation_stats();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on vote_transactions table (optional - may impact performance)
-- CREATE TRIGGER refresh_vote_stats_trigger
-- AFTER INSERT ON vote_transactions
-- FOR EACH STATEMENT
-- EXECUTE FUNCTION trigger_refresh_vote_stats();

-- Alternative simpler view for recent votes if the above has issues
CREATE OR REPLACE VIEW recent_votes_simple AS
SELECT 
    vt.slot,
    vt.voter_pubkey,
    vt.vote_signature,
    vt.created_at,
    evs.total_stake as validator_stake,
    evs.stake_percentage,
    evs.epoch,
    CASE 
        WHEN evs.validator_pubkey IS NOT NULL THEN 'active'
        ELSE 'inactive'
    END as validator_status
FROM vote_transactions vt
LEFT JOIN epoch_validator_sets evs ON vt.voter_pubkey = evs.validator_pubkey
    AND evs.epoch = (SELECT MAX(epoch) FROM epoch_validator_sets)
WHERE vt.created_at > NOW() - INTERVAL '1 hour'
ORDER BY vt.slot DESC
LIMIT 100;