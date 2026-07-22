# CDC → Spatie Activity Log Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consume each inventory table's Debezium Kafka topic from the Laravel app and record every row change as a Spatie activity-log entry.

**Architecture:** A per-table Artisan command (`cdc:consume:<table>`) runs a native `mateusjunges/laravel-kafka` consumer inside the `app` container. An abstract base command owns the consume loop; three thin subclasses declare their topic/model/log-name. Each decoded Debezium envelope is handed to a single `CdcActivityLogger` service that writes the Spatie activity, attaching a hydrated (non-persisted) Eloquent model as the subject.

**Tech Stack:** PHP 8.3, Laravel 13, Pest 4, `spatie/laravel-activitylog`, `mateusjunges/laravel-kafka` (php-rdkafka), Debezium/Kafka, Docker Compose.

## Global Constraints

- PHP 8.3; Laravel 13 (`laravel/framework` ^13.17). Use PHP 8 constructor property promotion, explicit return types and param type hints, curly braces on all control structures.
- Do **not** add the Spatie `LogsActivity` trait to the models — changes arrive from Kafka, not Eloquent events; logging is manual.
- Kafka topics: `dbserver1.inventory.customers`, `dbserver1.inventory.products`, `dbserver1.inventory.orders`.
- Table schemas (all keyed on `id`, **no timestamps**): `customers(id, first_name, last_name, email)`, `products(id, name, description, weight)`, `orders(id, order_date, customer_id, product_id, quantity)`.
- Debezium `op` map: `r`→`snapshot`, `c`→`created`, `u`→`updated`, `d`→`deleted`. Any other/absent op → log nothing.
- Consumer group id: `laravel-cdc`. Broker: `kafka:9092`.
- `mateusjunges/laravel-kafka` requires `ext-rdkafka`. It is added to the container image but is absent on the host; all *testable* logic lives in `CdcActivityLogger` (pure PHP, never touches rdkafka). Composer resolves on the host via a faked `config.platform.ext-rdkafka`.
- After any PHP change run `vendor/bin/pint --dirty --format agent` before committing (project rule).

## Prerequisites

This repo is **not** git-initialized. If you want the per-task commit steps to work, run once at the start:

```bash
git init && git add -A && git commit -m "chore: baseline before CDC activity log work"
```

Otherwise treat the `git commit` steps as optional/no-ops.

## File Structure

**Create:**
- `app/Models/Customer.php`, `app/Models/Product.php`, `app/Models/Order.php` — Eloquent subjects mapped to the inventory tables.
- `app/Services/Cdc/CdcActivityLogger.php` — envelope → activity mapping (all real logic).
- `app/Console/Commands/Cdc/ConsumeCdcTopic.php` — abstract consume loop + payload handler.
- `app/Console/Commands/Cdc/ConsumeCustomers.php`, `ConsumeProducts.php`, `ConsumeOrders.php` — per-table commands.
- `tests/Feature/Cdc/ActivitylogInstalledTest.php`, `CdcModelsTest.php`, `CdcActivityLoggerTest.php`, `ConsumeCustomersTest.php`.

**Modify:**
- `composer.json` — add `config.platform.ext-rdkafka` + the two packages.
- `Dockerfile` — add `librdkafka-dev` + `rdkafka` extension.
- `docker-compose.yml` — add `KAFKA_BROKERS` / `KAFKA_CONSUMER_GROUP_ID` to the `app` service.
- `README.md` — document the consume commands + image rebuild.

**Generated (published):**
- `database/migrations/*_create_activity_log_table.php` (+ event/batch-uuid migrations) from Spatie.

---

### Task 1: Dependencies & infrastructure

**Files:**
- Modify: `composer.json` (config block)
- Modify: `Dockerfile`
- Modify: `docker-compose.yml` (`app` service env)
- Generated: `database/migrations/*activity_log*.php` (via vendor:publish)
- Test: `tests/Feature/Cdc/ActivitylogInstalledTest.php`

**Interfaces:**
- Consumes: nothing.
- Produces: `spatie/laravel-activitylog` (`activity()` helper, `Spatie\Activitylog\Models\Activity`) and `mateusjunges/laravel-kafka` (`Junges\Kafka\Facades\Kafka`, `Junges\Kafka\Contracts\ConsumerMessage`) installed and bootable; `activity_log` table available under `RefreshDatabase`.

- [ ] **Step 1: Fake the rdkafka platform requirement so the host can resolve deps**

Edit `composer.json` — add a `platform` key inside the existing `config` object:

```json
    "config": {
        "platform": {
            "ext-rdkafka": "6.0.0"
        },
        "optimize-autoloader": true,
        "preferred-install": "dist",
        "sort-packages": true,
        "allow-plugins": {
            "pestphp/pest-plugin": true,
            "php-http/discovery": true
        }
    },
```

- [ ] **Step 2: Require the packages**

Run: `composer require spatie/laravel-activitylog mateusjunges/laravel-kafka -W`
Expected: both added to `composer.json`/`composer.lock`; `php artisan package:discover` runs without fatal error.

Troubleshooting (Laravel 13 is new):
- If composer reports a version conflict on `illuminate/*`, re-run allowing the newest tags: `composer require "spatie/laravel-activitylog:*" "mateusjunges/laravel-kafka:*" -W`.
- If a package genuinely lacks Laravel 13 support, pin to its latest published tag and report back before continuing — do not downgrade `laravel/framework`.

- [ ] **Step 3: Publish the Spatie activity_log migrations**

Run: `php artisan vendor:publish --provider="Spatie\Activitylog\ActivitylogServiceProvider" --tag="activitylog-migrations"`
Expected: migration file(s) matching `*_create_activity_log_table.php` (and event / batch-uuid migrations) appear in `database/migrations/`.

- [ ] **Step 4: Add rdkafka to the Dockerfile**

Replace the existing `RUN apt-get ...` block in `Dockerfile` with:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
      unzip \
      libzip-dev \
      libicu-dev \
      libonig-dev \
      librdkafka-dev \
      default-mysql-client \
    && docker-php-ext-install pdo_mysql bcmath zip intl \
    && pecl install redis rdkafka \
    && docker-php-ext-enable redis rdkafka \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
```

- [ ] **Step 5: Point the app's consumer at the broker**

In `docker-compose.yml`, in the `app` service `environment:` block, add below the `SESSION_DRIVER: redis` line:

```yaml
      KAFKA_BROKERS: kafka:9092
      KAFKA_CONSUMER_GROUP_ID: laravel-cdc
```

- [ ] **Step 6: Write the failing smoke test**

Create `tests/Feature/Cdc/ActivitylogInstalledTest.php`:

```php
<?php

use Spatie\Activitylog\Models\Activity;

it('has the spatie activity_log table and can write to it', function () {
    activity()->log('smoke test');

    expect(Activity::count())->toBe(1);
});
```

- [ ] **Step 7: Run it — verify it passes**

Run: `php artisan test --compact --filter=ActivitylogInstalledTest`
Expected: PASS (RefreshDatabase runs the published migration on sqlite `:memory:`).
If it fails with "no such table: activity_log", confirm Step 3 wrote the migration into `database/migrations/`.

- [ ] **Step 8: Commit**

```bash
vendor/bin/pint --dirty --format agent
git add composer.json composer.lock Dockerfile docker-compose.yml database/migrations tests/Feature/Cdc/ActivitylogInstalledTest.php
git commit -m "chore: add spatie/laravel-activitylog + laravel-kafka and rdkafka runtime"
```

---

### Task 2: Eloquent subject models

**Files:**
- Create: `app/Models/Customer.php`, `app/Models/Product.php`, `app/Models/Order.php`
- Test: `tests/Feature/Cdc/CdcModelsTest.php`

**Interfaces:**
- Consumes: nothing.
- Produces: `App\Models\Customer`, `App\Models\Product`, `App\Models\Order` — each `extends Illuminate\Database\Eloquent\Model` with `$table` set, `public $timestamps = false`, `protected $guarded = []`. Used by Tasks 3 and 4 as Spatie subjects.

- [ ] **Step 1: Write the failing test**

Create `tests/Feature/Cdc/CdcModelsTest.php`:

```php
<?php

use App\Models\Customer;
use App\Models\Order;
use App\Models\Product;

it('maps cdc models to their inventory tables without timestamps', function () {
    expect((new Customer)->getTable())->toBe('customers');
    expect((new Product)->getTable())->toBe('products');
    expect((new Order)->getTable())->toBe('orders');

    expect((new Customer)->usesTimestamps())->toBeFalse();
    expect((new Product)->usesTimestamps())->toBeFalse();
    expect((new Order)->usesTimestamps())->toBeFalse();
});

it('exposes the primary key as the activity subject id after force fill', function () {
    $customer = (new Customer)->forceFill(['id' => 42, 'first_name' => 'Grace']);

    expect($customer->getKey())->toBe(42);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `php artisan test --compact --filter=CdcModelsTest`
Expected: FAIL with "Class \"App\Models\Customer\" not found".

- [ ] **Step 3: Create the models**

`app/Models/Customer.php`:

```php
<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Customer extends Model
{
    protected $table = 'customers';

    public $timestamps = false;

    protected $guarded = [];
}
```

`app/Models/Product.php`:

```php
<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Product extends Model
{
    protected $table = 'products';

    public $timestamps = false;

    protected $guarded = [];
}
```

`app/Models/Order.php`:

```php
<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class Order extends Model
{
    protected $table = 'orders';

    public $timestamps = false;

    protected $guarded = [];
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `php artisan test --compact --filter=CdcModelsTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
vendor/bin/pint --dirty --format agent
git add app/Models/Customer.php app/Models/Product.php app/Models/Order.php tests/Feature/Cdc/CdcModelsTest.php
git commit -m "feat: add read-only Customer/Product/Order CDC subject models"
```

---

### Task 3: CdcActivityLogger service

**Files:**
- Create: `app/Services/Cdc/CdcActivityLogger.php`
- Test: `tests/Feature/Cdc/CdcActivityLoggerTest.php`

**Interfaces:**
- Consumes: `App\Models\Customer` (Task 2); `spatie/laravel-activitylog` (Task 1).
- Produces: `App\Services\Cdc\CdcActivityLogger::logEnvelope(array $envelope, string $modelClass, string $logName): ?Spatie\Activitylog\Models\Activity`. Returns the created `Activity`, or `null` for unknown/absent `op`. Used by Task 4.

- [ ] **Step 1: Write the failing tests**

Create `tests/Feature/Cdc/CdcActivityLoggerTest.php`:

```php
<?php

use App\Models\Customer;
use App\Services\Cdc\CdcActivityLogger;
use Spatie\Activitylog\Models\Activity;

function customerEnvelope(string $op, ?array $before, ?array $after): array
{
    return [
        'before' => $before,
        'after' => $after,
        'op' => $op,
        'source' => ['table' => 'customers', 'ts_ms' => 1737600000000],
        'ts_ms' => 1737600000123,
    ];
}

function logCustomer(array $envelope): ?Activity
{
    return app(CdcActivityLogger::class)->logEnvelope($envelope, Customer::class, 'customers');
}

it('logs a create as a created activity with the after row', function () {
    $after = ['id' => 5, 'first_name' => 'New', 'last_name' => 'Person', 'email' => 'new@example.com'];

    $activity = logCustomer(customerEnvelope('c', null, $after));

    expect(Activity::count())->toBe(1);
    expect($activity->log_name)->toBe('customers');
    expect($activity->event)->toBe('created');
    expect($activity->subject_type)->toBe(Customer::class);
    expect((int) $activity->subject_id)->toBe(5);
    expect($activity->properties['op'])->toBe('c');
    expect($activity->properties['after'])->toEqual($after);
    expect($activity->properties['before'])->toBeNull();
    expect($activity->description)->toBe('customers #5 created');
});

it('logs an update with both before and after', function () {
    $before = ['id' => 1, 'first_name' => 'Sally', 'last_name' => 'Thomas', 'email' => 's@example.com'];
    $after = ['id' => 1, 'first_name' => 'Sarah', 'last_name' => 'Thomas', 'email' => 's@example.com'];

    $activity = logCustomer(customerEnvelope('u', $before, $after));

    expect($activity->event)->toBe('updated');
    expect((int) $activity->subject_id)->toBe(1);
    expect($activity->properties['before'])->toEqual($before);
    expect($activity->properties['after'])->toEqual($after);
});

it('logs a delete using the before row for the subject', function () {
    $before = ['id' => 9, 'first_name' => 'Gone', 'last_name' => 'Away', 'email' => 'g@example.com'];

    $activity = logCustomer(customerEnvelope('d', $before, null));

    expect($activity->event)->toBe('deleted');
    expect((int) $activity->subject_id)->toBe(9);
    expect($activity->properties['before'])->toEqual($before);
    expect($activity->properties['after'])->toBeNull();
});

it('logs a snapshot read as a snapshot event', function () {
    $after = ['id' => 2, 'first_name' => 'George', 'last_name' => 'Bailey', 'email' => 'g@foobar.com'];

    $activity = logCustomer(customerEnvelope('r', null, $after));

    expect($activity->event)->toBe('snapshot');
    expect((int) $activity->subject_id)->toBe(2);
});

it('ignores an unknown op and writes nothing', function () {
    $activity = logCustomer(customerEnvelope('x', null, ['id' => 3]));

    expect($activity)->toBeNull();
    expect(Activity::count())->toBe(0);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `php artisan test --compact --filter=CdcActivityLoggerTest`
Expected: FAIL with "Class \"App\Services\Cdc\CdcActivityLogger\" not found".

- [ ] **Step 3: Implement the service**

Create `app/Services/Cdc/CdcActivityLogger.php`:

```php
<?php

namespace App\Services\Cdc;

use Illuminate\Database\Eloquent\Model;
use Spatie\Activitylog\Models\Activity;

class CdcActivityLogger
{
    /**
     * Record a single Debezium change event as a Spatie activity.
     *
     * @param  array<string, mixed>  $envelope  Decoded Debezium value (before/after/op/source).
     * @param  class-string<Model>  $modelClass  Eloquent model used as the activity subject.
     * @param  string  $logName  Spatie log name (the table name).
     */
    public function logEnvelope(array $envelope, string $modelClass, string $logName): ?Activity
    {
        $op = $envelope['op'] ?? null;

        $event = match ($op) {
            'r' => 'snapshot',
            'c' => 'created',
            'u' => 'updated',
            'd' => 'deleted',
            default => null,
        };

        if ($event === null) {
            return null;
        }

        $row = $op === 'd' ? ($envelope['before'] ?? []) : ($envelope['after'] ?? []);
        $id = $row['id'] ?? null;

        $subject = new $modelClass;
        $subject->forceFill($row);

        $properties = [
            'op' => $op,
            'before' => $envelope['before'] ?? null,
            'after' => $envelope['after'] ?? null,
            'source' => [
                'table' => $envelope['source']['table'] ?? null,
                'ts_ms' => $envelope['source']['ts_ms'] ?? null,
            ],
        ];

        return activity($logName)
            ->performedOn($subject)
            ->event($event)
            ->withProperties($properties)
            ->log(sprintf('%s #%s %s', $logName, $id ?? '?', $event));
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `php artisan test --compact --filter=CdcActivityLoggerTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
vendor/bin/pint --dirty --format agent
git add app/Services/Cdc/CdcActivityLogger.php tests/Feature/Cdc/CdcActivityLoggerTest.php
git commit -m "feat: map Debezium change events to Spatie activity log"
```

---

### Task 4: Per-table consume commands + docs

**Files:**
- Create: `app/Console/Commands/Cdc/ConsumeCdcTopic.php` (abstract)
- Create: `app/Console/Commands/Cdc/ConsumeCustomers.php`, `ConsumeProducts.php`, `ConsumeOrders.php`
- Modify: `README.md`
- Test: `tests/Feature/Cdc/ConsumeCustomersTest.php`

**Interfaces:**
- Consumes: `App\Services\Cdc\CdcActivityLogger::logEnvelope(...)` (Task 3); `App\Models\{Customer,Product,Order}` (Task 2); `Junges\Kafka\Facades\Kafka`, `Junges\Kafka\Contracts\ConsumerMessage` (Task 1).
- Produces: abstract `App\Console\Commands\Cdc\ConsumeCdcTopic` with `protected string $topic`, `protected string $modelClass`, `protected string $logName`, constructor `__construct(protected CdcActivityLogger $logger)`, and `public function handlePayload(?array $body): ?Spatie\Activitylog\Models\Activity`. Concrete commands register signatures `cdc:consume:customers`, `cdc:consume:products`, `cdc:consume:orders`.

- [ ] **Step 1: Write the failing tests**

Create `tests/Feature/Cdc/ConsumeCustomersTest.php`:

```php
<?php

use App\Console\Commands\Cdc\ConsumeCustomers;
use App\Models\Customer;
use Illuminate\Support\Facades\Artisan;
use Spatie\Activitylog\Models\Activity;

it('registers the per-table consume commands', function () {
    expect(array_keys(Artisan::all()))
        ->toContain('cdc:consume:customers')
        ->toContain('cdc:consume:products')
        ->toContain('cdc:consume:orders');
});

it('logs an activity from a decoded message body', function () {
    $body = [
        'before' => null,
        'after' => ['id' => 7, 'first_name' => 'Ada', 'last_name' => 'Lovelace', 'email' => 'ada@example.com'],
        'op' => 'c',
        'source' => ['table' => 'customers', 'ts_ms' => 1737600000000],
    ];

    $activity = app(ConsumeCustomers::class)->handlePayload($body);

    expect($activity)->not->toBeNull();
    expect(Activity::count())->toBe(1);
    expect($activity->log_name)->toBe('customers');
    expect($activity->subject_type)->toBe(Customer::class);
    expect((int) $activity->subject_id)->toBe(7);
});

it('skips a tombstone (null body) without logging', function () {
    $activity = app(ConsumeCustomers::class)->handlePayload(null);

    expect($activity)->toBeNull();
    expect(Activity::count())->toBe(0);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `php artisan test --compact --filter=ConsumeCustomersTest`
Expected: FAIL with "Class \"App\Console\Commands\Cdc\ConsumeCustomers\" not found".

- [ ] **Step 3: Create the abstract base command**

`app/Console/Commands/Cdc/ConsumeCdcTopic.php`:

```php
<?php

namespace App\Console\Commands\Cdc;

use App\Services\Cdc\CdcActivityLogger;
use Illuminate\Console\Command;
use Junges\Kafka\Contracts\ConsumerMessage;
use Junges\Kafka\Facades\Kafka;
use Spatie\Activitylog\Models\Activity;

abstract class ConsumeCdcTopic extends Command
{
    /** Kafka topic this command subscribes to. */
    protected string $topic;

    /** @var class-string Eloquent model used as the Spatie activity subject. */
    protected string $modelClass;

    /** Spatie log name for recorded activities (the table name). */
    protected string $logName;

    public function __construct(protected CdcActivityLogger $logger)
    {
        parent::__construct();
    }

    public function handle(): int
    {
        $this->info("Consuming '{$this->topic}' -> activity log (Ctrl-C to stop)...");

        Kafka::consumer([$this->topic])
            ->withConsumerGroupId(config('kafka.consumer_group_id'))
            ->withHandler(function (ConsumerMessage $message): void {
                $activity = $this->handlePayload($message->getBody());

                if ($activity !== null) {
                    $this->line("  logged: {$activity->description}");
                }
            })
            ->build()
            ->consume();

        return self::SUCCESS;
    }

    /**
     * Turn a decoded Debezium message body into an activity-log entry.
     * Tombstones (null/empty body) are skipped.
     *
     * @param  array<string, mixed>|null  $body
     */
    public function handlePayload(?array $body): ?Activity
    {
        if (empty($body)) {
            return null;
        }

        return $this->logger->logEnvelope($body, $this->modelClass, $this->logName);
    }
}
```

- [ ] **Step 4: Create the three concrete commands**

`app/Console/Commands/Cdc/ConsumeCustomers.php`:

```php
<?php

namespace App\Console\Commands\Cdc;

use App\Models\Customer;

class ConsumeCustomers extends ConsumeCdcTopic
{
    protected $signature = 'cdc:consume:customers';

    protected $description = 'Consume the customers CDC topic and log changes to the activity log.';

    protected string $topic = 'dbserver1.inventory.customers';

    protected string $modelClass = Customer::class;

    protected string $logName = 'customers';
}
```

`app/Console/Commands/Cdc/ConsumeProducts.php`:

```php
<?php

namespace App\Console\Commands\Cdc;

use App\Models\Product;

class ConsumeProducts extends ConsumeCdcTopic
{
    protected $signature = 'cdc:consume:products';

    protected $description = 'Consume the products CDC topic and log changes to the activity log.';

    protected string $topic = 'dbserver1.inventory.products';

    protected string $modelClass = Product::class;

    protected string $logName = 'products';
}
```

`app/Console/Commands/Cdc/ConsumeOrders.php`:

```php
<?php

namespace App\Console\Commands\Cdc;

use App\Models\Order;

class ConsumeOrders extends ConsumeCdcTopic
{
    protected $signature = 'cdc:consume:orders';

    protected $description = 'Consume the orders CDC topic and log changes to the activity log.';

    protected string $topic = 'dbserver1.inventory.orders';

    protected string $modelClass = Order::class;

    protected string $logName = 'orders';
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `php artisan test --compact --filter=ConsumeCustomersTest`
Expected: PASS (3 tests). Commands in `app/Console/Commands/` are auto-discovered by Laravel — no manual registration needed.

- [ ] **Step 6: Document the commands in the README**

In `README.md`, after the `### See the events` section, add:

```markdown
### Log changes to the activity log

The Laravel `app` service consumes each table's topic and records every change
as a [Spatie activity log](https://spatie.be/docs/laravel-activitylog) entry.
Rebuild the image once so the rdkafka extension is available:

```bash
docker compose up -d --build app
```

Then run a per-table consumer (Ctrl-C to stop):

```bash
docker compose exec app php artisan cdc:consume:customers
docker compose exec app php artisan cdc:consume:products
docker compose exec app php artisan cdc:consume:orders
```

Each consumed event writes an `activity_log` row whose subject is the
`Customer` / `Product` / `Order` record and whose properties hold the Debezium
`before` / `after` / `op` payload.
```

- [ ] **Step 7: Run the full CDC test suite**

Run: `php artisan test --compact --filter=Cdc`
Expected: PASS (all Task 1–4 tests green).

- [ ] **Step 8: Commit**

```bash
vendor/bin/pint --dirty --format agent
git add app/Console/Commands/Cdc README.md tests/Feature/Cdc/ConsumeCustomersTest.php
git commit -m "feat: add cdc:consume:{customers,products,orders} logging commands"
```

---

## Manual verification (in Docker, after implementation)

Not part of the automated suite — confirms the real Kafka path end-to-end.

```bash
docker compose up -d --build            # rebuild app with rdkafka
docker compose exec -T app php artisan cdc:consume:customers &   # start consumer
docker compose exec mysql mysql -uroot -pdebezium inventory -e \
  "UPDATE customers SET first_name='Sarah' WHERE id=1;"
# consumer prints: "logged: customers #1 updated"
docker compose exec app php artisan tinker --execute \
  'echo \Spatie\Activitylog\Models\Activity::latest()->first()->description;'
```

## Self-Review Notes

- **Spec coverage:** deps/infra (Task 1), models (Task 2), `CdcActivityLogger` incl. op-map/subject/properties/tombstone-guard (Task 3), abstract+3 commands+README (Task 4), tests for all five service cases + tombstone (Tasks 3–4). consume.sh intentionally untouched per spec.
- **rdkafka constraint:** honored — service is pure PHP; command's rdkafka use (`Kafka::consumer(...)`) is never exercised in tests (only `handlePayload` is), so the host extension is not required.
- **Type consistency:** `logEnvelope(array, string, string): ?Activity` and `handlePayload(?array): ?Activity` are referenced identically across Tasks 3–4.
