# Twine Solana Geyser Plugin

A high-performance Geyser plugin implementing the Synchronized Airlock pattern with atomic reference counting for deterministic data capture.

## Features

- **Synchronized Airlock Pattern**: Thread-safe data collection with minimal contention
- **Atomic Reference Counting**: Ensures no data races during slot sealing
- **Object Pooling**: Efficient memory management with reusable account info objects
- **Lock-free Queues**: High-throughput data ingestion using crossbeam's SegQueue
- **Deterministic Processing**: Guarantees all account updates are captured when slots are rooted

## Architecture

The plugin uses a multi-stage airlock system:

1. **Stage 1 - High-throughput ingestion**: Lock-free queue for account updates
2. **Stage 2 - Synchronization**: Atomic counters track active writers
3. **Stage 3 - Sealing**: When a slot is rooted, data is atomically sealed and processed

## Building

```bash
cargo build --release --features enhanced-geyser-bank-data
```

## Configuration

Create a JSON configuration file:

```json
{
  "monitored_accounts": [
    "11111111111111111111111111111111",
    "Config1111111111111111111111111111111111111"
  ],
  "max_slots_tracked": 100,
  "enable_lthash_notifications": true
}
```

## Usage

Add to your validator's Geyser plugin configuration:

```bash
solana-validator --geyser-plugin-config path/to/config.json
```

## Performance

- Minimal lock contention on the hot path
- Object pooling reduces allocation overhead
- Lock-free queues enable high-throughput data ingestion
- Atomic operations ensure thread safety without blocking