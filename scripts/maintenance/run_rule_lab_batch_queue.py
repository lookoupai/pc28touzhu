#!/usr/bin/env python3
"""Run an explicitly approved finite list of batches, then enable daily continuation.

The manifest lists existing batch IDs and frozen configuration hashes. This is a
one-off operator queue; it never creates experiments or resumes a user pause.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_store import RuleLabError


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def queue_step(service, manifest, *, now):
    user_id = manifest['user_id']
    reports = []
    for item in manifest['batches']:
        batch_id = item['batch_id']
        if item.get('configured') or item.get('removed'):
            reports.append(dict(batch_id=batch_id, state='removed' if item.get('removed') else 'configured'))
            continue
        try:
            eligibility = service.store.queue_eligibility(user_id,batch_id,item['configuration_hash'])
            if eligibility['removed']:
                item['removed'] = True
                reports.append(dict(batch_id=batch_id,state='removed',reason=eligibility['reason']))
                continue
            batch = service.store.get_batch(user_id, batch_id)
            preview = service.continuation_preview(user_id, 'batch', batch_id, now=now)
            if preview['configuration_hash'] != item['configuration_hash']:
                raise RuleLabError('configuration_changed', '批次配置与已批准的队列不一致', 409)
            if batch['status'] == 'completed':
                continuation = preview['continuation']
                if continuation['enabled']:
                    # A pre-existing enabled series or an operator change is not
                    # permission to keep overriding settings on every poll.
                    item['configured'] = True
                    reports.append(dict(batch_id=batch_id, state='configured'))
                    continue
                if not preview['eligible']:
                    reports.append(dict(batch_id=batch_id, state='blocked', blockers=preview['blockers']))
                    continue
                payload = dict(request_id=manifest['request_prefix'] + '-daily-' + str(batch_id),
                    expected_revision=preview['expected_revision'], capture_enabled=True,
                    compute_enabled=True, daily_at=manifest['daily_at'], timezone='Asia/Shanghai')
                service.set_continuation(user_id, 'batch', batch_id, payload, now=now)
                item['configured'] = True
                reports.append(dict(batch_id=batch_id, state='configured'))
            elif batch['status'] == 'paused' and not batch['started_at'] and not item.get('started'):
                if batch['experiment_count'] <= service.store.capacity()['remaining_running']:
                    result=service.store.start_queued_batch(user_id,batch_id,item['configuration_hash'],now=now)
                    if result['removed']:
                        item['removed']=True
                        reports.append(dict(batch_id=batch_id,state='removed',reason=result.get('reason','')))
                    elif result['started']:
                        item['started']=True
                        reports.append(dict(batch_id=batch_id,state='running'))
                    else:reports.append(dict(batch_id=batch_id,state='queued'))
                else:
                    reports.append(dict(batch_id=batch_id, state='queued'))
            else:
                if batch['started_at']:
                    item['started'] = True
                reports.append(dict(batch_id=batch_id, state=batch['status'],
                                    processed_issues=batch['processed_issues'], error=batch['error']))
        except RuleLabError as error:
            reports.append(dict(batch_id=batch_id, state='waiting' if error.status == 503 else 'blocked',
                                code=error.code, error=error.message))
    return dict(complete=all(item.get('configured') or item.get('removed') for item in manifest['batches']), batches=reports)


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as output:
        os.chmod(temporary, 0o600)
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    path = args.manifest.resolve()
    stopped = False
    def stop(*unused):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with path.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest = json.loads(path.read_text())
        if (manifest.get('version') != 1 or type(manifest.get('user_id')) is not int
                or not isinstance(manifest.get('batches'), list) or not 1 <= len(manifest['batches']) <= 50):
            raise ValueError('队列清单无效')
        service = RuleLabService()
        previous = None
        while not stopped:
            result = queue_step(service, manifest, now=timestamp())
            manifest['updated_at'] = timestamp()
            manifest['result'] = result
            save(path, manifest)
            # Progress lives in the manifest; journal emits only state transitions.
            signature = [(item['batch_id'], item['state'], item.get('code')) for item in result['batches']]
            if signature != previous:
                print(json.dumps(result, ensure_ascii=False), flush=True)
                previous = signature
            if args.once or result['complete']:
                return
            until = time.monotonic() + 15
            while not stopped and time.monotonic() < until:
                time.sleep(min(.5, until - time.monotonic()))


if __name__ == '__main__':
    main()
