# CDC → Spatie Activity Log — Design

**Date:** 2026-07-23
**Status:** Approved (pending implementation plan)

## Summary

Consume the Debezium change-event topics for each inventory table
(`customers`, `products`, `orders`) from the Laravel app and record every
change as a [Spatie activitylog](https://spatie.be/docs/laravel-activitylog)
entry.

Debezium already streams MySQL row changes to Kafka topics
`dbserver1.inventory.customers`, `dbserver1.inventory.products`,
`dbserver1.inventory.orders`. This work adds the **consumer** side: a native
Kafka consumer (per table) running inside the Laravel `app` container that
translates each Debezium envelope into an activity-log record.

## Goals

- One consume command per table that reads the table's Kafka topic and logs
  every change (`create` / `update` / `delete` / snapshot `read`) via Spatie.
- Activities attached to an Eloquent subject (`Customer` / `Product` / `Order`
  `#id`), with the before/after payload retained in the activity properties.
- All change-to-activity mapping logic isolated in one testable service.

## Non-goals

- No UI to browse the activity log.
- No changes to the Debezium connector, MySQL schema, or the CDC pipeline
  itself.
- `scripts/consume.sh` keeps its current job (raw terminal tailing); it is not
  repurposed.

## Architecture

```
Kafka topic dbserver1.inventory.<table>
        │  (Debezium JSON envelope: before / after / op / source)
        ▼
php artisan cdc:consume:<table>   (in the app container)
  └─ ConsumeCdcTopic (abstract)   mateusjunges/laravel-kafka consumer loop
        └─ CdcActivityLogger::logEnvelope(...)
              └─ activity(<logName>)->performedOn(<subject>)
                   ->event(<event>)->withProperties([...])->log(...)
        ▼
  activity_log table (Spatie) in the `inventory` DB
```

The `app` service is already on the `dbz` Docker network and can reach
`kafka:9092` and the `inventory` MySQL database.

## Components

### 1. Dependencies & infrastructure

| Change | Detail |
|--------|--------|
| composer | `spatie/laravel-activitylog`, `mateusjunges/laravel-kafka` |
| Dockerfile | add `librdkafka-dev` (apt) + `pecl install rdkafka` + `docker-php-ext-enable rdkafka`, next to the existing redis pecl line |
| docker-compose (`app` env) | `KAFKA_BROKERS: kafka:9092`, `KAFKA_CONSUMER_GROUP_ID: laravel-cdc` |
| migration | publish + run Spatie's `activity_log` migration |

Notes:
- The app image must be rebuilt after the Dockerfile change:
  `docker compose up -d --build app`.
- The `activity_log` table is created in the `inventory` database but is **not**
  in the connector's `table.include.list`, so it does not feed back into CDC.
- `mateusjunges/laravel-kafka` requires `ext-rdkafka`. It is present in the
  container but not on the host used for local tests — see
  [Testing](#6-tests) and [Known constraints](#known-constraints).

### 2. Models — `app/Models/`

`Customer`, `Product`, `Order`, each mapped to its inventory table. The tables
have **no `created_at` / `updated_at`**, so:

- `protected $table = '<table>';`
- `public $timestamps = false;`
- `protected $guarded = [];`

These models exist only to serve as Spatie **subjects**; the application does
not otherwise read or write them.

### 3. Consumer commands — `app/Console/Commands/Cdc/`

- **`ConsumeCdcTopic` (abstract)** — owns the consume loop. Builds a
  `mateusjunges/laravel-kafka` consumer subscribed to `$this->topic`, and for
  each message: skips tombstones (null value), JSON-decodes the value, and calls
  `CdcActivityLogger::logEnvelope($envelope, $this->modelClass, $this->logName)`.
  Runs until interrupted (Ctrl-C).
- **Three subclasses**, each declaring `topic`, `modelClass`, `logName`:
  - `ConsumeCustomers` → signature `cdc:consume:customers`
  - `ConsumeProducts` → signature `cdc:consume:products`
  - `ConsumeOrders` → signature `cdc:consume:orders`

Run inside the container, e.g.
`docker compose exec app php artisan cdc:consume:customers`.

### 4. Shared logic — `app/Services/Cdc/CdcActivityLogger.php`

Single public method:

```php
public function logEnvelope(array $envelope, string $modelClass, string $logName): ?Activity
```

Behaviour:

- **op → event map:** `r` → `snapshot`, `c` → `created`, `u` → `updated`,
  `d` → `deleted`. Unknown op → return `null` (nothing logged).
- **Row selection:** use `after` for `r` / `c` / `u`, `before` for `d`. Derive
  `id` from that row's `id` key.
- **Subject:** hydrate a *non-persisted* `$modelClass` instance via `forceFill`
  with the selected row (which includes `id`), then `performedOn()` it. This
  records `subject_type` + `subject_id` (e.g. `Customer #12`) **without querying
  the live CDC table**, and works even for deletes where the row is gone.
- **Properties:** `['op' => ..., 'before' => ..., 'after' => ..., 'source' => ['table' => ..., 'ts_ms' => ...]]`.
- **Write:** `activity($logName)->performedOn($subject)->event($event)->withProperties($props)->log($description)`,
  where `$description` is `"<table> #<id> <event>"` (e.g. `"customers #12 updated"`).
  No causer (changes originate from the binlog / system).
- Returns the created `Activity`, or `null` when nothing was logged (tombstone
  already filtered in the command; unknown/malformed op guarded here).

### 5. `scripts/consume.sh`

Unchanged — remains the raw terminal tailer. The README gains a short section
documenting the three `cdc:consume:*` artisan commands and the required image
rebuild.

### 6. Tests — Pest, `tests/Feature/Cdc/`

Target `CdcActivityLogger` directly with `RefreshDatabase` (sqlite). Only the
`activity_log` table is needed because subjects are hydrated, not fetched, so no
inventory tables are required in the test DB.

Cases:
- **create** (`op = c`) → one activity: `log_name` = table, `event` = `created`,
  `subject_type` = `Customer`, `subject_id` from `after.id`, `properties.after`
  == row.
- **update** (`op = u`) → `event` = `updated`, properties carry both `before`
  and `after`.
- **delete** (`op = d`) → `event` = `deleted`, `subject_id` from `before.id`,
  `properties.before` set, `after` null.
- **snapshot** (`op = r`) → `event` = `snapshot`.
- **unknown op** → returns `null`, writes nothing.

(Tombstone/null-value handling lives in the command and is a thin guard; the
service is only reached with a decoded envelope.)

## Data reference — Debezium envelope

Value JSON (schemas disabled) looks like:

```json
{
  "before": { "id": 1, "first_name": "Sally", "last_name": "Thomas", "email": "sally.thomas@example.com" },
  "after":  { "id": 1, "first_name": "Sarah", "last_name": "Thomas", "email": "sally.thomas@example.com" },
  "op": "u",
  "source": { "table": "customers", "ts_ms": 1737600000000, "db": "inventory" },
  "ts_ms": 1737600000123
}
```

- `op`: `r` snapshot read, `c` create, `u` update, `d` delete.
- On delete, `after` is null; a subsequent **tombstone** message (null value)
  is also emitted and must be skipped.

Table schemas (all keyed on `id`, no timestamps):
- `customers(id, first_name, last_name, email)`
- `products(id, name, description, weight)`
- `orders(id, order_date, customer_id, product_id, quantity)`

## Known constraints

- **rdkafka on the host:** local `composer install` / `require` uses
  `--ignore-platform-req=ext-rdkafka`. Business logic stays in
  `CdcActivityLogger` (pure PHP) so tests never load the extension; the package
  only touches rdkafka when a consumer is built at runtime in the container.
- **Consumer group offsets:** all three commands share group
  `laravel-cdc` (each subscribes to a distinct topic). Re-running a command
  resumes from the last committed offset.
- **Docker image rebuild** is required once, to pick up the rdkafka extension.
```
