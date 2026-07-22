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
