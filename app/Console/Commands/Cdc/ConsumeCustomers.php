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
