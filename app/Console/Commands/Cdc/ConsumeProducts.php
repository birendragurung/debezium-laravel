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
