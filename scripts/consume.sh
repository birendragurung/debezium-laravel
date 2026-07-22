#!/usr/bin/env bash
# Stream change events from a Kafka topic to your terminal.
#   ./scripts/consume.sh                                  # customers topic
#   ./scripts/consume.sh dbserver1.inventory.orders       # any topic
set -euo pipefail

TOPIC="${1:-dbserver1.inventory.customers}"

echo "Consuming '$TOPIC' (Ctrl-C to stop)..."
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --from-beginning \
  --topic "$TOPIC"
