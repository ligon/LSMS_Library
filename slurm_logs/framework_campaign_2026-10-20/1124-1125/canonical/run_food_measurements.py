"""After one audit finishes, measure its three comma-affected countries."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import time

label = sys.argv[1]
root = Path('/local/job40255479/canonical-audit-compare')
meta = json.loads((root / f'{label}-completion.json').read_text())
assert meta['returncode'] == 0 and meta['runtime_head_unchanged']
runtime = meta['runtime']
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=runtime, text=True).strip() == meta['head']
out = root / 'food-measurements'
out.mkdir(exist_ok=True)
env = dict(os.environ, PYTHONPATH=runtime, LSMS_COUNTRIES_ROOT=runtime + '/lsms_library/countries',
           LSMS_DATA_DIR=str(root / f'data-{label}'),
           LSMS_ISSUES_LOG=str(root / f'{label}-issues.log'),
           OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
           LSMS_BUILD_WORKERS='1', LSMS_MAKE_JOBS='1', PYTHONDONTWRITEBYTECODE='1')
env['PATH'] = '/local/job40255479/venv_img/bin:' + env['PATH']
script = '/local/job40255479/worktrees/framework-integration/slurm_logs/framework_campaign_2026-10-20/1143/measure_food.py'
started = time.time()
def run(country):
    with (root / f'{label}-{country}-measure.log').open('w') as log:
        result = subprocess.run(['/local/job40255479/venv_img/bin/python', script,
                                 country, label, str(out)], cwd=runtime, env=env,
                                stdout=log, stderr=subprocess.STDOUT)
    return country, result.returncode
with ThreadPoolExecutor(max_workers=3) as pool:
    results = dict(pool.map(run, ['Senegal', 'Benin', 'CotedIvoire']))
assert not any(results.values()), results
(root / f'{label}-food-completion.json').write_text(json.dumps({
    'returncodes': results, 'head': meta['head'], 'elapsed_seconds': time.time() - started,
    'cpu_affinity': sorted(os.sched_getaffinity(0)), 'same_audit_cache_after_scan_completion': True,
}, indent=2, sort_keys=True) + '\n')
print(json.dumps(results, sort_keys=True))
