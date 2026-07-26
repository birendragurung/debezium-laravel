# Apache Flink Stream Processing — Design

**Date:** 2026-07-23
**Status:** Approved — implementing directly

## Summary

Add Apache Flink as a downstream stream processor sitting on top of the
existing Debezium/Kafka pipeline. No changes to MySQL, Debezium, or the
Kafka broker. A starter PyFlink job consumes the `dbserver1.inventory.orders`
topic, computes a windowed per-customer order aggregate, and writes results
to a new Kafka topic.

## Goals

- Flink JobManager + TaskManager running in `docker-compose.yml`, on the
  existing `dbz` network, image built from a local `flink/Dockerfile`.
- A starter PyFlink Table API job that reads Debezium CDC events natively
  (via the built-in `debezium-json` format — no manual envelope parsing).
- Job output lands in a new Kafka topic, visible in the existing Kafka UI.
- Job auto-submits on `docker compose up`, following the same one-shot
  "wait then act" pattern as the existing `connector-init` service.

## Non-goals

- No changes to Debezium, the MySQL schema, or `table.include.list`.
- No Laravel-side consumer for the Flink output topic (can be added later,
  following the existing `ConsumeCdcTopic` pattern).
- No revenue/price aggregation — `orders` has no price column
  (`id, order_date, customer_id, product_id, quantity`).

## Architecture

```
Kafka topic dbserver1.inventory.orders
        │  (Debezium JSON envelope: before / after / op / source)
        ▼
Flink Table API source table (format = 'debezium-json')
        │
        ▼
TUMBLE window (1 min, processing time) GROUP BY customer_id
        │  order_count, total_quantity
        ▼
Kafka sink table  →  topic flink.orders.customer-stats
        (viewable in Kafka UI at :8080)
```

The source topic is effectively insert-only in this app (orders are only
ever created, per the existing read-only `Order` CDC model), so a `TUMBLE`
window over the `debezium-json` changelog table is safe here even though
Flink windowing generally expects append-only input.

## Components

- **`flink/Dockerfile`** — `FROM flink:2.2.1-scala_2.12-java17`; installs
  `python3`/`pip`; `pip install --break-system-packages apache-flink==2.2.1`;
  downloads `flink-sql-connector-kafka-5.0.0-2.2.jar` into `/opt/flink/lib/`
  (matches the 2.2 runtime; verified present on Maven Central).
- **`flink/jobs/customer_order_stats.py`** — the job script, bind-mounted
  read-only into both containers so edits don't require an image rebuild.
- **`docker-compose.yml`** — three new services:
  - `jobmanager` — Flink master, Web UI on `:8081`.
  - `taskmanager` — Flink worker, executes the job.
  - `flink-job-submit` — one-shot, waits for the JobManager REST API
    (`:8081/overview`) then runs
    `flink run -py /opt/flink/jobs/customer_order_stats.py`, mirroring
    `connector-init`'s wait-then-register pattern.

## Verification

No Pest tests apply (no PHP changes). Manual verification:
1. `docker compose up -d` — confirm `jobmanager`/`taskmanager` healthy and
   the job shows `RUNNING` at `localhost:8081`.
2. Insert a row into `orders` directly against MySQL.
3. Confirm the CDC event lands on `dbserver1.inventory.orders`, and within
   the 1-minute window an aggregate event appears on
   `flink.orders.customer-stats` (via Kafka UI at `localhost:8080`).
