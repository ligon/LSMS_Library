"""Held-only incremental validation of the tenth fix with the reviewed nine."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

os.umask(0o077)
ROOT = Path('/local/job40255479/worktrees/isa-integration')
CONTROL = Path('/local/job40255479/control/isa-backlog-20261021')
OUT = CONTROL / 'integration/mali-ten-increment-v4'
NINE = CONTROL / 'integration/api-nine-review'
LABOR = CONTROL / 'review-1121-Mali'
HEAD = 'bbdcf34578085fb90e981184a0cadadc9701a366'
PYTHON = '/local/job40255479/venv_img/bin/python'
OUT.mkdir(exist_ok=False)
DATA = OUT / 'data'

def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def tracked():
    result = {}
    for raw in subprocess.check_output(['git', 'ls-files', '-z'], cwd=ROOT).split(b'\0'):
        if not raw:
            continue
        name = os.fsdecode(raw)
        path = ROOT / name
        digest = hashlib.sha256(os.fsencode(os.readlink(path))).hexdigest() if path.is_symlink() else sha(path)
        result[name] = [path.lstat().st_mode, digest]
    return result

assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() == HEAD
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)
before = tracked()
sources = {}
manifest = json.loads((NINE / 'l1-copy-manifest-private.json').read_text())
for key, record in manifest.items():
    path = NINE / 'data/dvc-cache' / key[:2] / key[2:]
    assert sha(path) == record['sha256']
    sources['dvc-cache/' + key[:2] + '/' + key[2:]] = path
for path in (NINE / 'data').rglob('*.parquet'):
    sources[str(path.relative_to(NINE / 'data'))] = path
assert sum(name.endswith('.parquet') for name in sources) == 1519
for path in (LABOR / 'review-data/dvc-cache').rglob('*'):
    if not path.is_file():
        continue
    key = path.parent.name + path.name
    assert len(key) == 32 and hashlib.md5(path.read_bytes()).hexdigest() == key
    name = 'dvc-cache/' + key[:2] + '/' + key[2:]
    if name in sources:
        assert sha(sources[name]) == sha(path)
    else:
        sources[name] = path

# Labor normal finalization also needs Niger's stale supporting sample.
# Stage its complete already-held binary footprint, never fetch absent blobs.
held_niger = unavailable_niger = 0
for sidecar in (ROOT / 'lsms_library/countries/Niger').rglob('*.dta.dvc'):
    match = re.search(r'md5:\s+([0-9a-f]{32})\b', sidecar.read_text())
    assert match is not None
    key = match[1]
    source = CONTROL / 'integration/data/dvc-cache' / key[:2] / key[2:]
    if not source.is_file():
        unavailable_niger += 1
        continue
    assert hashlib.md5(source.read_bytes()).hexdigest() == key
    held_niger += 1
    name = 'dvc-cache/' + key[:2] + '/' + key[2:]
    if name in sources:
        assert sha(sources[name]) == sha(source)
    else:
        sources[name] = source
assert held_niger == 161

def copy_one(item):
    name, source = item
    dest = DATA / name
    digest = sha(source)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, dest)
    assert not dest.is_symlink() and sha(dest) == digest
    assert (source.stat().st_dev, source.stat().st_ino) != (dest.stat().st_dev, dest.stat().st_ino)
    return name, {'source': str(source), 'sha256': digest, 'bytes': dest.stat().st_size}

start = time.monotonic()
with ThreadPoolExecutor(max_workers=4) as pool:
    copied = dict(pool.map(copy_one, sorted(sources.items())))
(OUT / 'copy-manifest-private.json').write_text(json.dumps(copied, sort_keys=True) + '\n')
shutil.copyfile(NINE / 'sitecustomize.py', OUT / 'sitecustomize.py')
env = os.environ.copy()
for key in ('LSMS_NO_CACHE', 'LSMS_BUILD_BACKEND'):
    env.pop(key, None)
env.update({
    'PYTHONPATH': str(ROOT) + ':' + str(OUT),
    'LSMS_COUNTRIES_ROOT': str(ROOT / 'lsms_library/countries'),
    'LSMS_DATA_DIR': str(DATA), 'LSMS_EXPECTED_ROOT': str(ROOT),
    'LSMS_ISSUES_LOG': str(OUT / 'issues-private.jsonl'),
    'PATH': '/local/job40255479/venv_img/bin:' + env['PATH'],
    'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONHASHSEED': '1',
    'OPENBLAS_NUM_THREADS': '1', 'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1',
    'LSMS_BUILD_WORKERS': '1', 'LSMS_MAKE_JOBS': '1', 'AWS_EC2_METADATA_DISABLED': 'true',
})
commands = []
for phase in ('initial', 'warm'):
    command = ['unshare', '-rn', '--', PYTHON, str(CONTROL / 'integration/check_mali_ten_increment.py'), phase]
    commands.append(command)
    with (OUT / (phase + '-private.log')).open('w') as stream:
        subprocess.run(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT, check=True)
command = ['unshare', '-rn', '--', PYTHON, '-m', 'pytest', '-n', '2', '--dist=loadfile',
           '--timeout=600', '--timeout-method=thread', '--tb=short', '-ra',
           '-o', 'cache_dir=' + str(OUT / 'pytest-cache'),
           '--basetemp=' + str(OUT / 'pytest-temp'),
           '--junitxml=' + str(OUT / 'tests-private.xml'),
           'tests/test_mali_plot_labor_source.py', 'tests/test_mali_community_price_readings.py']
commands.append(command)
with (OUT / 'tests-private.log').open('w') as stream:
    result = subprocess.run(command, cwd=ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT)
outcomes = {}
for case in ET.parse(OUT / 'tests-private.xml').getroot().findall('.//testcase'):
    outcome = 'error' if case.find('error') is not None else 'failed' if case.find('failure') is not None else 'skipped' if case.find('skipped') is not None else 'passed'
    outcomes[outcome] = outcomes.get(outcome, 0) + 1
assert all(sha(Path(record['source'])) == record['sha256'] for record in copied.values())
summary = {
    'head': HEAD, 'exit_code': result.returncode, 'outcomes': outcomes,
    'seconds_including_copy': round(time.monotonic() - start, 3),
    'commands': commands, 'private_L2_copies': 1519,
    'private_L1_copies': len(copied) - 1519,
    'held_Niger_binary_sidecars_verified': held_niger,
    'unheld_Niger_binary_declarations_left_absent': unavailable_niger,
    'source_files_unchanged': True, 'tracked_content_modes_unchanged': before == tracked(),
    'head_unchanged': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() == HEAD,
    'clean_status': not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT),
    'scope': 'Mali1121 incremental coexistence with822; not a repeat of the complete nine-fix cohort or full suite',
}
(OUT / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary), flush=True)
assert summary['tracked_content_modes_unchanged'] and summary['head_unchanged'] and summary['clean_status']
raise SystemExit(result.returncode)
