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
