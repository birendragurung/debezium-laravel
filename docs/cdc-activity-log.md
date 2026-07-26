# CDC → Activity Log

Every MySQL row change, streamed through Debezium and Kafka, written to a
Spatie activity log — what shipped, why, how to run it, and what's next.

**Tables covered:** `customers`, `products`, `orders`
**Tests:** 11 passing, 39 assertions
**Status:** shipped, ready to run

## 01 — What shipped

The existing pipeline — MySQL binlog → Debezium (Kafka Connect) → Kafka
topics → Kafka UI — is untouched. What's new sits entirely on the consuming
side, inside the Laravel `app` container.

```
┌─────────┐  binlog   ┌──────────────────┐  produce  ┌────────┐
│  MySQL  │──────────▶│ Debezium/Connect │──────────▶│ Kafka  │
└─────────┘           └──────────────────┘           └───┬────┘
                                                          │ per-table topic
                                                          ▼
                                                ┌────────────────────┐
                                                │  cdc:consume:*      │  (new)
                                                │  artisan command     │
                                                └──────────┬──────────┘
                                                           ▼
                                                ┌────────────────────┐
                                                │  activity_log        │  (new)
                                                │  Spatie table         │
                                                └────────────────────┘
```

Each command subscribes to one topic, decodes the Debezium JSON envelope, and
hands it to a single service that knows how to turn it into an activity:

```json
{
  "before": null,
  "after": {
    "id": 1006,
    "first_name": "Sarah",
    "last_name": "Thomas",
    "email": "sally.thomas@example.com"
  },
  "op": "u",
  "source": { "table": "customers", "ts_ms": 1737600000000 }
}
```

Debezium `op` → activity event:

| `op` | Event | Meaning |
|------|-------|---------|
| `r` | `snapshot` | initial read |
| `c` | `created` | insert |
| `u` | `updated` | update |
| `d` | `deleted` | delete |

Components added:

| Piece | File | Job |
|-------|------|-----|
| 3 commands | `Console/Commands/Cdc/Consume{Customers,Products,Orders}.php` | Declare topic + model + log name. Nothing else. |
| Base command | `Console/Commands/Cdc/ConsumeCdcTopic.php` | Owns the Kafka consume loop, skips tombstones. |
| Mapping service | `Services/Cdc/CdcActivityLogger.php` | Envelope → activity. All the real logic, fully unit-tested. |
| 3 models | `Models/{Customer,Product,Order}.php` | Read-only activity subjects — no timestamps, no persistence. |

## 02 — Why this shape

The data already changes outside the app — Debezium reads the binlog, not
Eloquent. So the audit trail has to be built from the event stream, not from
model events, and it has to survive the row being gone by the time anyone
looks (a delete leaves nothing to query).

**One command per table**
Each topic is its own long-running process, so `customers` can run, restart,
or fall behind independently of `orders`. Same consumer group
(`laravel-cdc`), one partition set each.

**Subjects, never queries**
The activity subject is a model instance hydrated with `forceFill()` from the
envelope's own row — never fetched. That's what makes deletes loggable at
all: nothing needs to still exist in `customers` for the log entry to be
correct.

**One service, no rdkafka in it**
`CdcActivityLogger` is plain PHP — array in, activity out. It's the only
class with real branching logic, and the only one the test suite needs to
touch; the Kafka extension stays confined to the command's `handle()` loop.

**No `LogsActivity` trait**
That trait logs Eloquent events. These changes don't originate from Eloquent,
so the models stay plain and every log entry is written explicitly, on
purpose, from the consumer.

## 03 — Install & run

Everything below assumes the base pipeline (`docker compose up -d`) is
already running and the connector is registered.

1. **Rebuild the app image.** The Dockerfile now installs
   `librdkafka-dev` and the `rdkafka` PHP extension — needed once, then
   cached.

   ```bash
   docker compose up -d --build app
   ```

2. **Check the broker settings.** Already set on the `app` service;
   override in `.env` for a non-Docker run.

   ```
   KAFKA_BROKERS=kafka:9092
   KAFKA_CONSUMER_GROUP_ID=laravel-cdc
   ```

3. **Run a consumer.** One per table, each in its own terminal. `Ctrl-C`
   to stop; re-running resumes from the last committed Kafka offset.

   ```bash
   docker compose exec app php artisan cdc:consume:customers
   docker compose exec app php artisan cdc:consume:products
   docker compose exec app php artisan cdc:consume:orders
   ```

4. **Trigger a change and watch it land.**

   ```bash
   docker compose exec mysql mysql -uroot -pdebezium inventory -e \
     "UPDATE customers SET first_name='Sarah' WHERE id=1;"
   ```

   The consumer prints `logged: customers #1 updated`; the row is now in
   `activity_log` with the full before/after payload in `properties`.

## 04 — Roadmap

Deliberately out of scope for this pass — the shortlist for what's next,
roughly in the order it'd get picked up.

**Next — Resilient consume loop**
An exception inside the message handler currently propagates and kills that
topic's process. Add a log-and-continue (or bounded retry) so one bad
message doesn't take the consumer down.

**Later — Browse the log**
No UI yet — reading activity means a DB client or `tinker`. A simple Inertia
page (filter by table, subject, date) turns this from an audit trail into
something people actually check.

**Later — Process supervision**
Each `cdc:consume:*` command is a foreground process today. Long-running
deployment needs a supervisor (Supervisor, systemd, or a Horizon-style
manager) to keep all three alive and restart on crash.

**Someday — New tables, same pattern**
Adding a table means three small, mechanical additions: extend the
connector's `table.include.list`, add a read-only model, add a one-line
command subclass. No changes to the shared logger.

**Someday — Schema registry**
Messages are plain JSON today for readability. A production setup would
move to Avro/Protobuf with a schema registry — already noted as a tradeoff
in the base pipeline's own docs.

---

*debezium-laravel · CDC activity log · PHP 8.3 · Laravel 13 ·
[spatie/laravel-activitylog](https://spatie.be/docs/laravel-activitylog) ·
[mateusjunges/laravel-kafka](https://github.com/mateusjunges/laravel-kafka)*
