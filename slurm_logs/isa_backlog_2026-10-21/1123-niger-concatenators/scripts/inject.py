"""Break one Niger wave script on purpose, in a SCRATCH copy of the config
tree, and record what Country('Niger').<table>() does.  Aggregates only.

Usage: python inject.py <checkout> <case> <table> <wave> <backend> <mode> <workdir> <out.json>
  backend: make | dvc
  mode:    raise        -- the checkout's own code, wave script broken
           andand-only  -- additionally make niger.concat_wave_parquets SKIP
                           a missing wave instead of refusing (the ruling's
                           premise: `&&` alone still loses the wave)
The build runs in a child (build_all.py) with a FRESH data root; its stderr
(where make's and the concatenator's messages land) is scanned for the
refusal line naming the wave.
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

checkout, case, table, wave, backend, mode, workdir, out = sys.argv[1:9]
checkout = Path(checkout)
work = Path(workdir) / case
if work.exists():
    shutil.rmtree(work)
countries = work / 'countries'
countries.mkdir(parents=True)
src = checkout / 'lsms_library' / 'countries'
for name in ['.dvc', '.dvcignore', 'dvc.yaml']:
    p = src / name
    if p.is_dir():
        shutil.copytree(p, countries / name)
    elif p.exists():
        shutil.copy2(p, countries / name)
shutil.copytree(src / 'Niger', countries / 'Niger',
                ignore=shutil.ignore_patterns('__pycache__', '*.parquet'))

script = countries / 'Niger' / wave / '_' / f'{table}.py'
script.write_text("raise SystemExit('injected wave failure (niger-andand)')\n" + script.read_text())

if mode == 'andand-only':
    niger = countries / 'Niger' / '_' / 'niger.py'
    s = niger.read_text()
    old = "        if not fn.exists():\n            raise FileNotFoundError("
    assert s.count(old) == 1, 'andand-only needs the branch helper'
    niger.write_text(s.replace(old, "        if not fn.exists():\n            continue\n"
                                    "            raise FileNotFoundError("))
else:
    assert mode == 'raise', mode

data = work / 'data'
data.mkdir()
os.symlink('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache', data / 'dvc-cache')
env = dict(os.environ, LSMS_COUNTRIES_ROOT=str(countries), LSMS_DATA_DIR=str(data),
           LSMS_ISSUES_LOG=str(work / 'issues.log'))
env.pop('LSMS_NO_CACHE', None)
if backend == 'make':
    env['LSMS_BUILD_BACKEND'] = 'make'
else:
    env.pop('LSMS_BUILD_BACKEND', None)
child_json = work / 'child.json'
err = work / 'stderr.txt'
with open(err, 'w') as fh:
    proc = subprocess.run([sys.executable, str(Path(__file__).with_name('build_all.py')),
                           str(checkout), case, str(child_json), str(work / 'private'), table],
                          env=env, stdout=subprocess.DEVNULL, stderr=fh)
child = json.loads(child_json.read_text())['tables'][table] if child_json.exists() else None
text = err.read_text(errors='replace')
res = {
    'case': case, 'table': table, 'broken_wave': wave, 'backend': backend, 'mode': mode,
    'child_rc': proc.returncode,
    'result': child,
    'stderr_refusal_lines_naming_wave': text.count(f'wave {wave} declares {table}'),
    'stderr_refusal_lines_any_wave': text.count(f'declares {table} (../'),
    'stderr_injected_failures': text.count('injected wave failure (niger-andand)'),
    'warn_makefile_failed': sum(v for k, v in ((child or {}).get('warning_heads') or {}).items()
                                if k.startswith('Makefile execution failed')),
    'warn_python_fallback_failed': sum(v for k, v in ((child or {}).get('warning_heads') or {}).items()
                                       if k.startswith('Python fallback failed')),
    'wave_parquets_built': sorted(p.parent.parent.name for p in
                                  (data / 'Niger').glob(f'*/_/{table}.parquet')),
    'var_parquet_written': (data / 'Niger' / 'var' / f'{table}.parquet').exists(),
    'intree_parquets': len(list(countries.rglob('*.parquet'))),
}
Path(out).write_text(json.dumps(res, indent=1, sort_keys=True))
print(json.dumps(res, sort_keys=True))
