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
