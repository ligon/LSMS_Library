"""Read-only exact-master platform monitoring; completed-job logs stay private."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time

OUT = Path(__file__).resolve().parent
RUN = 39604418333
SHA = '097283459f80016b0f00a51b814a0cb964a42789'
TARGETS = {'unit-tests', 'windows-smoke', 'windows-script-build', 'dvc-remote-reachability', 'dvc-lock-check', 'docs'}


def write(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def get(path):
    return json.loads(subprocess.check_output(['gh', 'api', f'repos/ligon/LSMS_Library/{path}'], text=True, timeout=90))


def parse_log(job):
    path = OUT / f'{job["name"]}-raw-private.log'
    if not path.exists() or path.stat().st_size == 0:
        with path.open('wb') as log:
            subprocess.run(['gh', 'api', '--allow-escape-sequences', f'repos/ligon/LSMS_Library/actions/jobs/{job["id"]}/logs'],
                           stdout=log, check=True, timeout=180)
    text = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', path.read_text(errors='replace'))
    lines = text.splitlines()
    checkouts = []
    for pos, line in enumerate(lines):
        if re.search(r'log\s+-1\s+--format=[\'\"]?%H', line):
            for following in lines[pos + 1:pos + 5]:
                found = re.search(r'(?:^|\s)([0-9a-f]{40})\s*$', following)
                if found:
                    checkouts.append(found.group(1))
    assert checkouts and set(checkouts) == {SHA}, (job['name'], checkouts)
    result = {'actual_checkout_shas': sorted(set(checkouts)),
              'raw_log_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
              'raw_log_bytes': path.stat().st_size,
              'credential_indicator_line_counts': {
                  marker: sum(marker in line for line in lines)
                  for marker in ['NoCredentialsError', 'PartialCredentialsError', 'UnmarkedS3TestWarning',
                                 'needs S3 credentials to build country data', 'Skipped: no S3 credentials',
                                 'reached a credential-less skip at RUN time']},
              'credential_count_limit': 'Literal line witnesses, not automatic adjudication. Unit/smoke jobs intentionally skip authentication; their credential-related skips are expected and not classified as platform failures.',
              'pytest_summaries': [re.sub(r'^\S+\s+', '', line).strip() for line in lines
                                   if re.search(r'\b\d+ passed(?:,| in )', line) and len(line) < 600]}
    if job['name'] == 'windows-script-build':
        native = []
        for match in re.finditer(r'RESULT (Uganda:\w+) rows=(\d+) cols=(\d+) waves=(\d+) digest=([0-9a-f]{12})', text):
            spec, rows, cols, waves, digest = match.groups()
            native.append({'table': spec, 'rows': int(rows), 'columns': int(cols), 'waves': int(waves), 'digest': digest})
        expected = [{'table': 'Uganda:shocks', 'rows': 14457, 'columns': 10, 'waves': 8, 'digest': '0ab2ea4c79dd'},
                    {'table': 'Uganda:assets', 'rows': 116808, 'columns': 1, 'waves': 8, 'digest': '9202ed542a14'}]
        assert native == expected, native
        assert 'installed code and configs:' in text
        makevars = re.findall(r'MAKEVARS Uganda parquet-list=(\d+) find-errors=(\d+)', text)
        assert makevars and all(int(n) > 0 and int(errors) == 0 for n, errors in makevars), makevars
        assert 'MISMATCH Uganda:' not in text and 'RAISED Uganda:' not in text
        result.update(native_results=native, installed_code_and_packaged_config_assertions_passed=True,
                      makevars=[{'parquet_list': int(n), 'find_errors': int(e)} for n, e in makevars],
                      credential_scope='S3 configuration step and actual cold builds succeed; no separate requires_s3 gate is defined in this native job')
    if job['name'] == 'dvc-lock-check':
        result['lock_check_limit'] = 'This step uses continue-on-error; job success alone does not prove clean locks. Refresh-job admission/conclusion is recorded separately.'
    return result


def main():
    previous = None
    complete = {}
    while True:
        run = get(f'actions/runs/{RUN}')
        assert run['head_sha'] == SHA and run['event'] == 'push' and run['head_branch'] == 'master'
        assert run['path'] == '.github/workflows/ci.yml'
        jobs = get(f'actions/runs/{RUN}/jobs?per_page=100')['jobs']
        write('run-private.json', run)
        write('jobs-private.json', jobs)
        state = {job['name']: [job['status'], job['conclusion']] for job in jobs}
        if state != previous:
            print(json.dumps({'at': datetime.now(timezone.utc).isoformat(), 'jobs': state}), flush=True)
            previous = state
        for job in jobs:
            if job['name'] not in TARGETS | {'refresh-dvc-locks'}:
                continue
            if job['status'] == 'completed' and job['conclusion'] not in {'success', 'skipped'}:
                write('FAILURE.json', {'job': job['name'], 'id': job['id'], 'conclusion': job['conclusion'], 'sha': SHA})
                print('PLATFORM_FAILURE ' + job['name'] + ' ' + str(job['conclusion']), flush=True)
                return 1
            if job['status'] != 'completed' or job['name'] in complete:
                continue
            record = {'job_id': job['id'], 'status': job['status'], 'conclusion': job['conclusion'],
                      'steps': [{'name': step['name'], 'status': step['status'], 'conclusion': step['conclusion']}
                                for step in job['steps']]}
            if job['conclusion'] == 'success':
                record.update(parse_log(job))
            complete[job['name']] = record
            write('completed-platform-jobs.json', complete)
        write('progress.json', {'run': RUN, 'sha': SHA, 'completed': sorted(complete), 'statuses': state})
        if TARGETS <= complete.keys() and all(job['status'] == 'completed' for job in jobs if job['name'] == 'refresh-dvc-locks'):
            data = next(job for job in jobs if job['name'] == 'data-tests')
            summary = {'state': 'platform jobs passed; Linux data suite reported separately',
                       'run': RUN, 'sha': SHA, 'event': run['event'], 'branch': run['head_branch'],
                       'workflow_status_at_platform_completion': run['status'],
                       'jobs': complete,
                       'data_auth_gate_metadata': [s for s in data['steps'] if s['name'] == 'Assert the requires_s3 gate is open'],
                       'data_suite_status': data['status'], 'data_suite_conclusion': data['conclusion'],
                       'no_project_import_or_tests_executed_locally': True, 'github_mutations': False,
                       'raw_logs_private': True, 'author': 'Sue'}
            write('verification.json', summary)
            print('PLATFORM_COMPLETE', flush=True)
            return 0
        time.sleep(45)


if __name__ == '__main__':
    raise SystemExit(main())
