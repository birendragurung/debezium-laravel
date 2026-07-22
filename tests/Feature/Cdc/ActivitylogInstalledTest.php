<?php

use Spatie\Activitylog\Models\Activity;

it('has the spatie activity_log table and can write to it', function () {
    activity()->log('smoke test');

    expect(Activity::count())->toBe(1);
});
