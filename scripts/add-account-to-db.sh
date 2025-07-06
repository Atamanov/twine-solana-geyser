#!/bin/bash

# Script to add account directly to database
# Usage: ./add-account-to-db.sh <pubkey>

if [ -z "$1" ]; then
    echo "Usage: $0 <account_pubkey>"
    exit 1
fi

PUBKEY=$1

echo "Adding account $PUBKEY to monitored_accounts table..."

docker exec -it twine-timescaledb psql -U geyser_writer -d twine_solana_db -c "
INSERT INTO monitored_accounts (account_pubkey) 
VALUES ('$PUBKEY') 
ON CONFLICT (account_pubkey) 
DO UPDATE SET active = true
RETURNING *;"

if [ $? -eq 0 ]; then
    echo "Account added successfully!"
else
    echo "Failed to add account"
fi