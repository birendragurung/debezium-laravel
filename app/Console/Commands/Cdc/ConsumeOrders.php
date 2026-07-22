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
