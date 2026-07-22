# Debezium CDC: MySQL → Kafka

A self-contained Change Data Capture demo. MySQL row changes are captured by
Debezium (running on Kafka Connect) and streamed to Kafka topics.

```
┌─────────┐  binlog   ┌──────────────────┐  produce  ┌────────┐   ┌──────────┐
│  MySQL  │──────────▶│ Debezium/Connect │──────────▶│ Kafka  │──▶│ Kafka UI │
│ :3306   │           │ :8083            │           │ :9092  │   │ :8080    │
└─────────┘           └──────────────────┘           └────────┘   └──────────┘
```

## Layout

| Path | Purpose |
|------|---------|
| `docker-compose.yml` | Kafka (KRaft), MySQL, Kafka Connect, auto-registration, Kafka UI |
| `mysql/conf.d/cdc.cnf` | Enables row-based binary logging required for CDC |
| `mysql/initdb/01-inventory.sql` | Creates `inventory` schema + dummy data + `debezium` user |
| `connector/register-mysql.json` | Debezium MySQL connector configuration |
| `scripts/register-connector.sh` | (Re)register the connector manually |
| `scripts/consume.sh` | Tail change events from a Kafka topic |

## Quick start

```bash
docker compose up -d
docker compose logs -f connector-init      # wait for "Registered connectors"
```

`connector-init` waits for MySQL + Connect to be healthy, then registers the
connector automatically. On startup Debezium takes an initial **snapshot** of
the existing rows, then switches to streaming live changes from the binlog.

### See the events

Open **Kafka UI → http://localhost:8080**, or tail a topic from the CLI:

```bash
./scripts/consume.sh dbserver1.inventory.customers
```

Topics produced (one per table): `dbserver1.inventory.customers`,
`dbserver1.inventory.products`, `dbserver1.inventory.orders`.

### Trigger a live change

In another terminal, modify a row and watch a new event appear:

```bash
docker compose exec mysql mysql -uroot -pdebezium inventory -e \
  "UPDATE customers SET first_name='Sarah' WHERE id=1;
   INSERT INTO customers (first_name,last_name,email)
     VALUES ('New','Person','new.person@example.com');
   DELETE FROM orders WHERE id=4;"
```

Each event is a JSON envelope with `before`, `after`, `op`
(`c`=create, `u`=update, `d`=delete, `r`=snapshot read) and `source` metadata.

## Useful commands

```bash
# Connector status / health
curl -s localhost:8083/connectors/inventory-connector/status | jq

# List Kafka topics
docker compose exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Open a MySQL shell
docker compose exec mysql mysql -uroot -pdebezium inventory

# Reload connector after editing the config
./scripts/register-connector.sh

# Stop (keep data) / wipe everything
docker compose down
docker compose down -v
```

## Credentials

| Service | Value |
|---------|-------|
| MySQL root | `root` / `debezium` |
| MySQL app user | `mysqluser` / `mysqlpw` |
| MySQL Debezium user | `debezium` / `dbz` |
| MySQL database | `inventory` (host `localhost:3306`) |

## Notes

- Kafka runs in **KRaft mode** (no ZooKeeper).
- Kafka is advertised internally as `kafka:9092`; the mapped host port is best
  consumed via Kafka UI or `docker compose exec` (see `scripts/consume.sh`).
- Messages are emitted as plain JSON (schemas disabled) for readability. For
  production, prefer Avro/Protobuf with a schema registry.
