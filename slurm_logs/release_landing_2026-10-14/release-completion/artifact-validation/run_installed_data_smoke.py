"""Launcher prepared for coordinator-supplied verified release identities."""
from pathlib import Path
import argparse
import os
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--expected-version', required=True)
parser.add_argument('--expected-sha', required=True)
parser.add_argument('--expected-wheel-sha256', required=True)
args = parser.parse_args()
out = Path(__file__).resolve().parent
config = out / 'empty-config'
config.mkdir(exist_ok=False)
env = {key: value for key, value in os.environ.items()
       if not key.startswith(('AWS_', 'LSMS_', 'MICRODATA_'))
       and key not in {'PYTHONPATH', 'PYTHONSAFEPATH'}}
env.update(LSMS_CONFIG_DIR=str(config),
           LSMS_DATA_DIR='/local/job39919092/release-work/runtime/data-installed-release-smoke',
           LSMS_SKIP_AUTH='1', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
           LSMS_BUILD_WORKERS='1', LSMS_MAKE_JOBS='1', AWS_EC2_METADATA_DISABLED='true',
           AWS_SHARED_CREDENTIALS_FILE=str(config / 'aws-credentials'),
           AWS_CONFIG_FILE=str(config / 'aws-config'),
           PYTHON_KEYRING_BACKEND='keyring.backends.null.Keyring')
command = ['taskset', '-c', '0-7', '/local/job39919092/release-work/published-install-env/bin/python',
           str(out / 'installed_data_smoke.py'), '--expected-version', args.expected_version,
           '--expected-sha', args.expected_sha, '--expected-wheel-sha256', args.expected_wheel_sha256]
with (out / 'run.log').open('x') as log:
    result = subprocess.run(command, cwd=out, env=env, stdout=log, stderr=subprocess.STDOUT)
print('Published installed data smoke exit:', result.returncode)
raise SystemExit(result.returncode)
