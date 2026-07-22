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
