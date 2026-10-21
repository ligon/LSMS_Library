"""Rebuild the already-unpacked local source archive without Git metadata."""
from pathlib import Path
import json
import os
import subprocess
import time

OUT = Path(__file__).resolve().parent
ROOT = next((OUT / 'sdist-unpacked').iterdir())
PYTHON = '/local/job39919092/release-work/framework/release-validation-env/bin/python'
assert not (ROOT / '.git').exists()
assert subprocess.run(['git', 'rev-parse', '--is-inside-work-tree'], cwd=ROOT,
                      capture_output=True).returncode != 0
env = {key: value for key, value in os.environ.items()
       if not key.startswith(('AWS_', 'LSMS_', 'MICRODATA_'))
       and key not in {'PYTHONPATH', 'PYTHONSAFEPATH'}}
config = OUT / 'sdist-build-config'
data = OUT / 'sdist-build-data'
config.mkdir(exist_ok=False)
data.mkdir(exist_ok=False)
env.update(LSMS_CONFIG_DIR=str(config), LSMS_DATA_DIR=str(data), LSMS_SKIP_AUTH='1',
           OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
           LSMS_BUILD_WORKERS='1', AWS_EC2_METADATA_DISABLED='true',
           AWS_SHARED_CREDENTIALS_FILE=str(config / 'aws-credentials'),
           AWS_CONFIG_FILE=str(config / 'aws-config'),
           PYTHON_KEYRING_BACKEND='keyring.backends.null.Keyring')
command = ['taskset', '-c', '24-31', PYTHON, '-m', 'build', '--wheel',
           '--no-isolation', '--outdir', str(OUT / 'sdist-rebuild-dist')]
started = time.time()
with (OUT / 'sdist-rebuild.log').open('w') as log:
    result = subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                            stderr=subprocess.STDOUT)
(OUT / 'sdist-rebuild.json').write_text(json.dumps({
    'command': command, 'cwd': str(ROOT), 'git_present': False,
    'exit': result.returncode, 'seconds': time.time() - started,
}, indent=2))
raise SystemExit(result.returncode)
