"""Run recorded coverage gaps only after the parent copies the quiescent corpus."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

wt = Path('/local/job39919092/release-work/cache-coverage')
cache = Path('/local/job39919092/release-work/cache-coverage-data')
control = Path(__file__).resolve().parent
logs = Path('/local/job39919092/release-work/logs/cache-coverage')
logs.mkdir(exist_ok=True)
expected = '4f1f044bb9b3079e7f427898f77627ad1503ec5e'
assert subprocess.check_output(['git', '-C', str(wt), 'rev-parse', 'HEAD'], text=True).strip() == expected
assert (cache / 'CORPUS_COPY_COMPLETE.json').is_file(), 'Do not test a partial/live corpus copy'
assert not (cache / 'Uganda').is_symlink()
env = os.environ.copy()
env.update(PYTHONPATH=str(wt), LSMS_COUNTRIES_ROOT=str(wt/'lsms_library/countries'),
           LSMS_DATA_DIR=str(cache), LSMS_EXPECTED_ROOT=str(wt),
           OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
           LSMS_BUILD_WORKERS='1', MAKEFLAGS='-j1',
           PATH='/local/job39919092/venv_img/bin:'+env.get('PATH',''))
for name in ['LSMS_NO_CACHE', 'LSMS_BUILD_BACKEND', 'LSMS_SKIP_AUTH', 'LSMS_CONFIG_DIR', 'PYTHONSAFEPATH']:
    env.pop(name, None)
python = '/local/job39919092/venv_img/bin/python'
identity = "import lsms_library; from lsms_library.paths import countries_root,data_root; from pathlib import Path; import os; assert Path(lsms_library.__file__).resolve().parent==Path(os.environ['PYTHONPATH'])/'lsms_library'; assert countries_root()==Path(os.environ['LSMS_COUNTRIES_ROOT']); assert data_root()==Path(os.environ['LSMS_DATA_DIR']); print('Source/config/private-cache identity verified')"
subprocess.run([python,'-c',identity],cwd=wt,env=env,check=True)
groups=json.loads((control/'cache-gap-groups.json').read_text())
# The poisoning cases get their own serial invocation after all read-only checks.
# Run the complete affected GH866 file so its new fixture is exercised with data.
groups['mutating']=['tests/test_gh866_stale_country_parquet_not_laundered.py']+[x for x in groups['mutating'] if 'test_gh809_' in x]
results=[]
for name in ['structural','ordinary','uganda_baseline','mutating']:
    cases=groups[name]
    if name=='ordinary': cases=cases+['tests/test_categorical_mapping_resolution.py::test_import_is_from_the_expected_checkout']
    workers=['-n','6','--dist=loadfile'] if name in ('structural','ordinary') else []
    cmd=['taskset','-c','24-31',python,'-m','pytest',*workers,'--timeout=600',
         '--timeout-method=thread','--durations=15','-ra','-q',
         '--junitxml='+str(logs/f'{name}.xml'),*cases]
    start=time.time()
    with (logs/f'{name}.log').open('w') as out:
        proc=subprocess.run(cmd,cwd=wt,env=env,stdout=out,stderr=subprocess.STDOUT)
    row={'group':name,'source_sha':expected,'exit_code':proc.returncode,'seconds':time.time()-start,
         'command':cmd,'log':str(logs/f'{name}.log')}
    results.append(row)
    (control/'cache-coverage-results.json').write_text(json.dumps(results,indent=2)+'\n')
    print(json.dumps({k:v for k,v in row.items() if k!='command'}),flush=True)
print('All recorded groups finished; inspect failures and skip reasons before declaring coverage.',flush=True)
