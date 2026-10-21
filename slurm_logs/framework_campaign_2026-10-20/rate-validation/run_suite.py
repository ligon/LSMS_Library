"""Frozen pooled-rate suite controller, reusing the follow-up controller with baseline XPASS metadata adapted. Requires the parent's explicit final pin.

Run from --repo with PYTHONPATH and LSMS_COUNTRIES_ROOT naming that checkout.
This writes only private evidence/cache; it never changes source or Git state.
"""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def affinity(spec):
    out = set()
    for part in spec.split(','):
        bits = [int(x) for x in part.split('-')]
        out.update(range(bits[0], bits[-1] + 1))
    return out


def baseline_cases(path, repo):
    cases = {}
    for case in ET.parse(path).getroot().iter('testcase'):
        cls = case.attrib['classname'].split('.')
        for end in range(len(cls), 0, -1):
            file = Path(*cls[:end]).with_suffix('.py')
            if (repo / file).exists():
                node = '::'.join([file.as_posix(), *cls[end:], case.attrib['name']])
                break
        else:
            raise AssertionError(f'Cannot map JUnit classname {cls}')
        skip = case.find('skipped')
        failure = case.find('failure')
        error = case.find('error')
        outcome = ('error' if error is not None else 'failure' if failure is not None
                   else 'xfailed' if skip is not None and skip.get('type') == 'pytest.xfail'
                   else 'skipped' if skip is not None else 'passed_or_nonstrict_xpass')
        cases[node] = {'outcome': outcome, 'reason': skip.get('message', '') if skip is not None else ''}
    return cases


class Reports:
    def __init__(self, out):
        self.out = out
        self.events = (out / 'events-private.jsonl').open('w')
        self.results = {}
        self.warnings = Counter()
        self.workers = set()
        self.summary = {}

    def pytest_runtest_logreport(self, report):
        wasxfail = getattr(report, 'wasxfail', None)
        if report.failed:
            outcome = 'failure' if report.when == 'call' else 'error'
            write_json(self.out / 'FAILURE.json', {'nodeid': report.nodeid, 'phase': report.when, 'outcome': outcome})
        elif report.skipped:
            outcome = 'xfailed' if wasxfail else 'skipped'
        elif report.when == 'call':
            outcome = 'xpassed' if wasxfail else 'passed'
        else:
            return
        reason = (str(wasxfail) if wasxfail else str(report.longrepr[2])
                  if report.skipped and isinstance(report.longrepr, tuple) else '')
        if reason.startswith('Skipped: '):
            reason = reason[len('Skipped: '):]
        record = {'nodeid': report.nodeid, 'phase': report.when, 'outcome': outcome,
                  'reason': reason, 'seconds': report.duration}
        node = getattr(report, 'node', None)
        if node is not None:
            record['worker'] = node.gateway.id
            self.workers.add(record['worker'])
        self.events.write(json.dumps(record) + '\n')
        self.events.flush()
        old = self.results.get(report.nodeid)
        if old is None or old['outcome'] not in {'failure', 'error'}:
            self.results[report.nodeid] = record

    def pytest_collectreport(self, report):
        if report.failed:
            write_json(self.out / 'FAILURE.json', {'nodeid': report.nodeid, 'phase': 'collection', 'outcome': 'error'})

    def pytest_warning_recorded(self, warning_message, when, nodeid, location):
        self.warnings[warning_message.category.__name__] += 1

    def pytest_sessionfinish(self, session, exitstatus):
        terminal = session.config.pluginmanager.get_plugin('terminalreporter')
        self.summary = {'terminal_counts': {name: len(terminal.stats.get(name, [])) for name in
                        ['passed', 'skipped', 'xfailed', 'xpassed', 'failed', 'error', 'warnings']},
                        'exitstatus': int(exitstatus)}
        write_json(self.out / 'outcomes-private.json', self.results)
        write_json(self.out / 'terminal-counts.json', self.summary)
        self.events.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--expected-sha', required=True)
    p.add_argument('--data', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--copy-manifest', type=Path, required=True)
    p.add_argument('--baseline-xml', type=Path, required=True)
    p.add_argument('--baseline-summary', type=Path, required=True)
    p.add_argument('--affinity', default='0-7,20-27')
    a = p.parse_args()
    repo, data, out = a.repo.resolve(), a.data.resolve(), a.out.resolve()
    assert len(a.expected_sha) == 40, 'Supply the final full frozen SHA'
    assert Path.cwd().resolve() == repo
    assert os.environ['PYTHONPATH'] == str(repo)
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == repo / 'lsms_library/countries'
    assert Path(os.environ['LSMS_DATA_DIR']).resolve() == data
    assert data.is_relative_to(Path('/local')) and not data.is_relative_to(repo)
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == a.expected_sha
    assert not subprocess.check_output(['git', 'diff', 'HEAD', '--name-only'], text=True).strip()
    for name in ['OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS', 'LSMS_BUILD_WORKERS', 'LSMS_MAKE_JOBS']:
        assert os.environ.get(name) == '1', (name, os.environ.get(name))
    assert not os.environ.get('LSMS_NO_CACHE')
    os.environ['LSMS_EXPECTED_ROOT'] = str(repo)
    os.environ['LSMS_ISSUES_LOG'] = str(out / 'issues-private.log')
    cpus = affinity(a.affinity)
    assert cpus <= affinity('0-7,20-31') and len(cpus) >= 16
    os.sched_setaffinity(0, cpus)
    out.mkdir(parents=True, exist_ok=True)
    assert not (out / 'STARTED.json').exists(), 'Use a new output directory for each run'
    copied = json.loads(a.copy_manifest.read_text())
    files = {str(path.relative_to(data)): path for path in data.rglob('*') if path.is_file() and 'dvc-cache' not in path.relative_to(data).parts}
    assert set(files) == set(copied), 'Prepared suite cache file set changed before launch'
    for name, record in copied.items():
        assert sha256(files[name]) == record['sha256'], name
        assert not files[name].is_symlink(), name
    import lsms_library as ll
    from lsms_library.paths import countries_root, data_root
    assert Path(ll.__file__).resolve().parent.parent == repo
    assert countries_root().resolve() == repo / 'lsms_library/countries'
    assert data_root().resolve() == data
    import pytest
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from threadpoolctl import threadpool_info
    pin = {'source_commit': a.expected_sha, 'package': str(Path(ll.__file__).resolve()),
           'cwd': str(repo), 'countries_root': str(countries_root()), 'data_root': str(data_root()),
           'interpreter': sys.executable, 'affinity': sorted(cpus), 'workers': 16,
           'distribution': 'loadfile', 'timeout_seconds': 600, 'timeout_method': 'thread',
           'manifest_sha256': sha256(a.copy_manifest), 'prepared_files': len(copied),
           'versions': {'numpy': np.__version__, 'pandas': pd.__version__, 'pyarrow': pa.__version__, 'pytest': pytest.__version__},
           'blas': threadpool_info(), 'state': 'running'}
    write_json(out / 'STARTED.json', pin)
    before = baseline_cases(a.baseline_xml, repo)
    plugin = Reports(out)
    xml = out / 'suite-private.xml'
    args = ['-n', '16', '--dist=loadfile', '--timeout=600', '--timeout-method=thread',
            '--maxfail=1', '--tb=short', '-ra', '--durations=25', f'--junitxml={xml}']
    start = time.monotonic()
    code = pytest.main(args, plugins=[plugin])
    elapsed = time.monotonic() - start
    after = plugin.results
    baseline = json.loads(a.baseline_summary.read_text())
    transitions = []
    for node in sorted(before.keys() | after.keys()):
        old, new = before.get(node), after.get(node)
        if old and new and old['outcome'] == 'passed_or_nonstrict_xpass' and new['outcome'] in {'passed', 'xpassed'}:
            continue
        if old and new and old['outcome'] == new['outcome'] and old['reason'] == new['reason']:
            continue
        transitions.append({'nodeid': node, 'before': old, 'after': new})
    report = {**pin, 'state': 'passed' if int(code) == 0 else 'failed', 'seconds': elapsed,
              'counts': plugin.summary, 'workers_reporting': sorted(plugin.workers),
              'warnings_by_category': dict(plugin.warnings),
              'skip_reasons': dict(Counter(r['reason'] for r in after.values() if r['outcome'] == 'skipped')),
              'xfail_reasons': dict(Counter(r['reason'] for r in after.values() if r['outcome'] == 'xfailed')),
              'candidate_xpass_cases': [n for n, r in after.items() if r['outcome'] == 'xpassed'],
              'baseline': {'source_commit': baseline['source_commit'], 'counts': baseline['counts'],
                           'junit_sha256': sha256(a.baseline_xml), 'summary_sha256': sha256(a.baseline_summary)},
              'baseline_xpass_cases': baseline.get('candidate_xpass_cases', []),
              'baseline_xpass_identity_limit': ('Baseline candidate_xpass_cases directly captured by its report plugin; compare to the current direct report list.' if baseline.get('candidate_xpass_cases') else 'Baseline JUnit encodes non-strict XPASS as PASS; no exact baseline identity is claimed.'),
              'transition_count': len(transitions), 'transitions': transitions,
              'junit_sha256': sha256(xml) if xml.exists() else None,
              'head_unchanged_after': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == a.expected_sha,
              'tracked_source_unchanged_after': not subprocess.check_output(['git', 'diff', 'HEAD', '--name-only'], text=True).strip()}
    if not report['head_unchanged_after'] or not report['tracked_source_unchanged_after']:
        code = 1
        report['state'] = 'failed'
        write_json(out / 'FAILURE.json', {'phase': 'source_identity_after_suite'})
    write_json(out / 'summary.json', report)
    write_json(out / 'completion.json', {'exitstatus': int(code), 'state': report['state'], 'source_commit': a.expected_sha})
    return int(code)


if __name__ == '__main__':
    raise SystemExit(main())
