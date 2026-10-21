"""Read-only exact-run CI barrier; no workflow dispatch, retries or mutations."""
import json
from pathlib import Path
import subprocess
import time


def validate_run(record, run_id, expected_sha):
    assert record['id'] == int(run_id), 'CI run ID mismatch'
    assert record['head_sha'] == expected_sha, 'CI head SHA mismatch'
    assert record['event'] == 'push', 'Expected automatic push workflow'
    assert record['head_branch'] == 'master', 'Expected master push'
    assert record['path'] == '.github/workflows/ci.yml', 'Unexpected workflow path'
    assert record['head_repository']['full_name'] == 'ligon/LSMS_Library'
    status = record['status']
    if status == 'completed':
        return 'success' if record['conclusion'] == 'success' else 'failure'
    assert status in {'queued', 'in_progress', 'waiting', 'pending', 'requested'}, status
    return 'waiting'


def read_run(run_id):
    result = subprocess.run(['gh', 'api', f'repos/ligon/LSMS_Library/actions/runs/{int(run_id)}'],
                            text=True, capture_output=True, timeout=60, check=True)
    return json.loads(result.stdout)


def wait_for_success(run_id, expected_sha, out, timeout=10800, poll=45,
                     fetch=read_run, sleep=time.sleep, clock=time.monotonic):
    assert str(run_id).isdigit() and len(expected_sha) == 40
    assert timeout > 0 and poll == 45
    out = Path(out)
    started = clock()
    previous = None
    read_errors = 0
    while True:
        try:
            record = fetch(run_id)
        except (subprocess.SubprocessError, json.JSONDecodeError, OSError):
            read_errors += 1
            if read_errors >= 5:
                raise RuntimeError('Five consecutive read-only CI metadata requests failed')
            state = ('metadata_read_retry', None)
        else:
            read_errors = 0
            verdict = validate_run(record, run_id, expected_sha)
            state = (record['status'], record['conclusion'])
            summary = {'run_id': int(run_id), 'head_sha': expected_sha, 'event': record['event'],
                       'head_branch': record['head_branch'], 'workflow_path': record['path'],
                       'status': record['status'], 'conclusion': record['conclusion'],
                       'html_url': record.get('html_url'), 'elapsed_wait_seconds': clock() - started}
            (out / 'ci-status.json').write_text(json.dumps(summary, indent=2) + '\n')
            if verdict == 'failure':
                raise RuntimeError(f'Exact master CI run completed with {record["conclusion"]}; no tag permitted')
            if verdict == 'success':
                if state != previous:
                    print(f'Exact master CI: {state[0]} / {state[1]}', flush=True)
                return summary
        if state != previous:
            print(f'Exact master CI: {state[0]} / {state[1]}', flush=True)
            previous = state
        if clock() - started >= timeout:
            raise TimeoutError('Exact master CI wait expired; no tag permitted')
        sleep(poll)
