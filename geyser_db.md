# Plan: High-Performance Geyser Plugin Database Sink

This document outlines a plan to build a robust, high-performance system for consuming and storing the enhanced Geyser plugin notifications in a database.

## 1. Executive Summary

The primary challenge is handling the immense data volume from the Solana mainnet. The architecture must be highly parallel and asynchronous, avoiding backpressure on the Agave validator's core threads at all costs.

This plan details a **stateful, conditional storage and proof scheduling engine** built around a **Synchronized Airlock** pattern. Its three primary functions are:
1.  **Full History of Slot Hashes**: Persist the `BankHashComponentsInfo` for every rooted slot to maintain a complete audit trail of the chain's state progression.
2.  **Conditional History of Account Changes**: Store the complete set of account changes for a slot **if and only if** at least one account from a pre-configured "monitored list" was modified in that slot.
3.  **Intelligent Proof Scheduling**: For any monitored account that has been updated, wait for a configurable number of slots to pass, then create a single proof request for the account's *most recent* state within that window.

The proposed solution involves:
-   **Synchronized Airlock Ingestion:** Using atomic counters and lock-free queues to safely ingest concurrent data updates before a slot is rooted, eliminating race conditions and lock contention.
-   **Stateful Filtering & Scheduling Logic:** A "frontend" component that decides which data to persist and when to schedule a proof.
-   **External Prover Decoupling:** Using the database as a durable work queue for a separate, offline proof generation service.
-   **Parallel Worker Pool:** A dedicated, asynchronous worker pool for writing data to the database in large, efficient batches.

This plan explicitly avoids message brokers like Kafka to provide a self-contained solution within the plugin's process space.

## 2. Database Schema Design

The schema is updated to replace the proofs table with a `proof_requests` table, which will act as a work queue for the external prover.

```sql
-- Table to store per-slot information
CREATE TABLE slots (
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

-- Main table for account changes, partitioned by slot
CREATE TABLE account_changes (
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

-- Index for querying an account's history
CREATE INDEX idx_account_changes_on_pubkey_slot ON account_changes (account_pubkey, slot DESC);

-- NEW table to schedule proof generation for an external prover
CREATE TABLE proof_requests (
    request_id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    slot BIGINT NOT NULL,
    account_pubkey VARCHAR(44) NOT NULL,
    -- Status to track the lifecycle of the request by the external prover
    -- e.g., 'pending', 'in_progress', 'completed', 'failed'
    status VARCHAR(20) DEFAULT 'pending' NOT NULL
);

CREATE INDEX idx_proof_requests_on_status ON proof_requests (status);
```
* **Partitioning:** The `account_changes` table **must** be partitioned (e.g., by slot range) to keep indexes manageable and allow for efficient archival or deletion of old data.

## 3. Plugin Architecture

The plugin's internal logic is redesigned around the **Synchronized Airlock** pattern, creating a clear separation between data ingestion, filtering/scheduling, and database writing.

### 3.1. The Synchronized Airlock: Core Data Structures

This pattern is the core of the ingestion mechanism, ensuring thread-safe data capture without locks on the hot path.

-   **`AirlockSlotData` struct:** A container for each slot currently being processed.
    ```rust
    struct AirlockSlotData {
        // Tracks how many validator threads are currently writing to this slot's buffer.
        writer_count: AtomicU32,
        // A lock-free queue for all account changes in this slot.
        buffer: SegQueue<OwnedAccountChange>,
        // Tracks if a monitored account was seen in this slot.
        contains_monitored_change: AtomicBool,
    }
    ```
-   **Main In-memory State:**
    -   `airlock: DashMap<Slot, Arc<AirlockSlotData>>`: The primary buffer for all in-flight slot data.
    -   `monitored_accounts: HashSet<Pubkey>`: A fast-lookup set of pubkeys loaded from config.
    -   `pending_proofs_for_scheduling: DashMap<Pubkey, Slot>`: The debouncing map for proof scheduling.
    -   `db_writer_queue: MPSC-Channel`: The lock-free queue that feeds the database worker pool.

### 3.2. Conditional Storage and Proof Scheduling Logic

#### Geyser Callback Logic (On-chain thread)
1.  **On `notify_account_change`:** (High-throughput, lock-free)
    -   Get or insert the `Arc<AirlockSlotData>` for the current slot from the `airlock`.
    -   **Atomically increment `writer_count`**.
    -   Push the `OwnedAccountChange` onto the lock-free `buffer`.
    -   If `change.pubkey` is in `monitored_accounts`:
        -   Set `contains_monitored_change` to `true` (this is an atomic bool, so it's safe).
        -   Update `pending_proofs_for_scheduling`, overwriting any previous slot for that pubkey.
    -   **Atomically decrement `writer_count`**.

2.  **On `update_slot_status(..., Rooted)`:** (Sealing the Airlock)
    -   **Always** push the `BankHashComponentsInfo` and slot `LtHash` for the rooted slot into the `db_writer_queue`.
    -   Attempt to remove the `AirlockSlotData` for the rooted slot from the main `airlock` map. If it's not present, it means no accounts were touched in that slot, so we're done.
    -   **Synchronization Point**: Briefly spin-wait until the slot's `writer_count` is zero. This guarantees that any lagging threads have finished writing to the buffer.
    -   **Conditional Account Storage**: If `contains_monitored_change` is `true`, drain the `buffer` and push all its `OwnedAccountChange` items into the `db_writer_queue`.
    -   **Note:** Proofs are no longer scheduled in this callback. The `pending_proofs_for_scheduling` map is handled by the independent service.

### 3.3. Proof Scheduling Service (Independent Thread)
This service runs independently inside the plugin to decouple scheduling from the real-time Geyser callbacks.

-   **Mechanism:** A dedicated thread is spawned at plugin startup. It tracks the last slot it processed (`last_proof_scheduling_slot`) and wakes up periodically when `current_rooted_slot > last_proof_scheduling_slot + proof_scheduling_slot_interval`.
-   **Workflow:**
    1.  The service wakes up when the slot condition is met.
    2.  It performs an atomic "drain" of the `pending_proofs_for_scheduling` map: it takes all the current entries for processing and clears the map, so new updates can start accumulating immediately.
    3.  For each `(pubkey, slot)` in the drained snapshot, it creates a `ProofRequest` object.
    4.  It pushes all these new `ProofRequest` objects into the `db_writer_queue`.
    5.  It updates its internal `last_proof_scheduling_slot` counter to the `current_rooted_slot`.

### 3.4. External Prover (Out of Scope)

The plan still assumes an external service is responsible for proof generation by polling the `proof_requests` table. This architecture remains unchanged.

### 3.5. Backend: Worker Pool and Batch Processing

The backend architecture remains as previously described: a pool of worker threads consumes from the `db_writer_queue` and performs high-throughput, batched `COPY` or `INSERT` operations to the database via a connection pool. This design is robust and well-suited to handle the mixed workload of `slots`, `account_changes`, and `proof_requests`.

## 4. Error Handling & Resilience

*   **Database Errors:** If a batch write fails, it must not be silently dropped. The entire batch should be retried with an exponential backoff strategy. If retries fail repeatedly, the data for that batch should be logged to a local "dead-letter" file for manual inspection and replay.
*   **Poison Pills:** A "poison pill" (a special message to gracefully shut down) should be sent to the worker threads when the plugin's `on_unload` method is called, ensuring all buffered data is flushed to the database before the validator exits.

## 5. Configuration

The plugin's configuration file is updated:
*   `database_connection_string`
*   `num_worker_threads`, `db_connections_per_worker`
*   `max_queue_size`, `batch_size`, `batch_timeout_ms`
*   `monitored_accounts: Vec<String>` (An array of base58-encoded pubkeys)
*   **NEW:** `proof_scheduling_slot_interval: u64` (e.g., a value of `10` would trigger a scheduling run every 10 rooted slots, which is approx. 4 seconds on mainnet).

This plan provides a blueprint for a highly concurrent, resilient, and performant database sink capable of handling the full firehose of Solana mainnet data. 