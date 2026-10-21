"""Frozen scanner/runtime orchestration; writes only private validation files."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

label = sys.argv[1]
root = Path('/local/job40255479/canonical-audit-compare')
targets = {
    'baseline': ('/local/job40255479/worktrees/baseline-976fc840d',
                 '976fc840d12ba40f1602b7dff58b81548632b2a9'),
    'combined': ('/local/job40255479/worktrees/framework-integration',
                 '852ac2bfcd5f798533f56e7efa1f77db80c0138f'),
}
runtime, expected = targets[label]
scanner = Path('/local/job40255479/worktrees/framework-integration/bench/feature_audit/scan.py')
controller_sha = hashlib.sha256(scanner.read_bytes()).hexdigest()
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=runtime, text=True).strip() == expected
env = dict(os.environ)
env.update({
    'PATH': '/local/job40255479/venv_img/bin:' + env['PATH'],
    'PYTHONPATH': runtime,
    'LSMS_COUNTRIES_ROOT': runtime + '/lsms_library/countries',
    'LSMS_DATA_DIR': str(root / f'data-{label}'),
    'LSMS_ISSUES_LOG': str(root / f'{label}-issues.log'),
    'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1',
    'LSMS_BUILD_WORKERS': '1', 'LSMS_MAKE_JOBS': '1',
    'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONUNBUFFERED': '1',
})
python = '/local/job40255479/venv_img/bin/python'
identity_code = """
import json, os
from pathlib import Path
import lsms_library as ll
from lsms_library.paths import countries_root, data_root
root = Path(os.environ['PYTHONPATH']).resolve()
assert Path(ll.__file__).resolve().is_relative_to(root), ll.__file__
assert countries_root().resolve() == root/'lsms_library/countries'
assert data_root().resolve() == Path(os.environ['LSMS_DATA_DIR']).resolve()
print(json.dumps({'package': ll.__file__, 'countries_root': str(countries_root()), 'data_root': str(data_root())}))
"""
identity = json.loads(subprocess.check_output([python, '-c', identity_code],
                                             cwd=runtime, env=env, text=True))
started = time.time()
cmd = [python, str(scanner), '--features', 'canonical', '--phases', '1,2',
       '--jobs', '3', '--out', str(root / f'{label}-raw.jsonl')]
metadata = {'label': label, 'runtime': runtime, 'head': expected,
            'scanner': str(scanner), 'scanner_sha256': controller_sha,
            'identity': identity, 'command': cmd,
            'cpu_affinity': sorted(os.sched_getaffinity(0)),
            'nice': os.getpriority(os.PRIO_PROCESS, 0), 'started': started}
(root / f'{label}-started.json').write_text(json.dumps(metadata, indent=2, sort_keys=True) + '\n')
with (root / f'{label}-scan.log').open('w', encoding='utf-8') as log:
    run = subprocess.run(cmd, cwd=runtime, env=env, stdout=log, stderr=subprocess.STDOUT)
metadata.update(returncode=run.returncode, elapsed_seconds=time.time() - started)
metadata['controller_unchanged'] = hashlib.sha256(scanner.read_bytes()).hexdigest() == controller_sha
metadata['runtime_head_unchanged'] = subprocess.check_output(
    ['git', 'rev-parse', 'HEAD'], cwd=runtime, text=True).strip() == expected
assert metadata['controller_unchanged'] and metadata['runtime_head_unchanged']
(root / f'{label}-completion.json').write_text(json.dumps(metadata, indent=2, sort_keys=True) + '\n')
print(json.dumps(metadata, sort_keys=True))
sys.exit(run.returncode)
