#!/usr/bin/env bash
# (Re)register the Debezium MySQL connector against the Connect REST API.
# The stack already auto-registers on `docker compose up`; use this to reload
# after editing connector/register-mysql.json.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG="$HERE/../connector/register-mysql.json"

echo "Deleting existing 'inventory-connector' (if any)..."
curl -s -o /dev/null -X DELETE "$CONNECT_URL/connectors/inventory-connector" || true

echo "Registering connector from $CONFIG ..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @"$CONFIG" "$CONNECT_URL/connectors/" \
  | { command -v jq >/dev/null && jq . || cat; }

echo
echo "Status:"
curl -s "$CONNECT_URL/connectors/inventory-connector/status" \
  | { command -v jq >/dev/null && jq . || cat; }
echo
