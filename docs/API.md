# Twine Geyser Plugin REST API

The Twine Geyser Plugin provides a REST API for managing monitored accounts at runtime.

## Configuration

Add the `api_port` configuration to your plugin config:

```json
{
  "api_port": 8080,
  // ... other config options
}
```

## Endpoints

### Health Check

```bash
GET /api/health
```

Example:
```bash
curl http://localhost:8080/api/health
```

Response:
```json
{
  "success": true,
  "message": "Twine Geyser Plugin API is running"
}
```

### Get Monitored Accounts

```bash
GET /api/monitored-accounts
```

Example:
```bash
curl http://localhost:8080/api/monitored-accounts
```

Response:
```json
{
  "accounts": [
    {
      "pubkey": "11111111111111111111111111111111",
      "added_at": "2024-01-20T10:30:00Z"
    },
    {
      "pubkey": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "added_at": "2024-01-20T10:30:00Z"
    }
  ],
  "count": 2
}
```

### Add Monitored Account

```bash
POST /api/monitored-accounts
Content-Type: application/json

{
  "pubkey": "YOUR_ACCOUNT_PUBKEY"
}
```

Example:
```bash
curl -X POST http://localhost:8080/api/monitored-accounts \
  -H "Content-Type: application/json" \
  -d '{"pubkey": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}'
```

Response:
```json
{
  "success": true,
  "message": "Account EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v added to monitoring"
}
```

### Remove Monitored Account

```bash
DELETE /api/monitored-accounts
Content-Type: application/json

{
  "pubkey": "YOUR_ACCOUNT_PUBKEY"
}
```

Example:
```bash
curl -X DELETE http://localhost:8080/api/monitored-accounts \
  -H "Content-Type: application/json" \
  -d '{"pubkey": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}'
```

Response:
```json
{
  "success": true,
  "message": "Account EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v removed from monitoring"
}
```

## Database Integration

All monitored accounts are stored in the `monitored_accounts` table with the following information:
- `account_pubkey`: The account public key
- `added_at`: When the account was added
- `last_seen_slot`: Last slot where the account had changes
- `last_seen_at`: Last time the account had changes
- `total_changes_tracked`: Total number of changes tracked
- `total_data_bytes`: Total data bytes tracked
- `active`: Whether the account is actively monitored

## Notes

- Accounts added via the API are immediately active
- Accounts from the config file are automatically synced to the database on startup
- The plugin syncs with the database on startup to load all active accounts
- Account statistics are updated in real-time as changes are processed