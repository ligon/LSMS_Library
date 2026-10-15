"""Run the installed-release smoke with empty config/data and scrubbed credentials."""
from pathlib import Path
import os
import subprocess

out = Path(__file__).resolve().parent
env = {key: value for key, value in os.environ.items()
       if not key.startswith(('AWS_', 'LSMS_', 'MICRODATA_'))
       and key not in {'PYTHONPATH', 'PYTHONSAFEPATH'}}
for suffix in ['config', 'data']:
    (out / ('installed-' + suffix)).mkdir(exist_ok=False)
env.update(LSMS_CONFIG_DIR=str(out / 'installed-config'),
           LSMS_DATA_DIR=str(out / 'installed-data'), LSMS_SKIP_AUTH='1',
           OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
           LSMS_BUILD_WORKERS='1', AWS_EC2_METADATA_DISABLED='true',
           AWS_SHARED_CREDENTIALS_FILE=str(out / 'installed-config/aws-credentials'),
           AWS_CONFIG_FILE=str(out / 'installed-config/aws-config'))
command = ['taskset', '-c', '24-31',
           '/local/job39919092/release-work/published-install-env/bin/python',
           str(out / 'published_smoke.py')]
with (out / 'installed-smoke.log').open('x') as log:
    result = subprocess.run(command, cwd=out, env=env, stdout=log, stderr=subprocess.STDOUT)
print('Published installed smoke exit:', result.returncode)
raise SystemExit(result.returncode)
