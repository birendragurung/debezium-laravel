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
