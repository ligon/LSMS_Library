#!/bin/bash
#SBATCH --job-name=lsms-wheel
#SBATCH --output=slurm_logs/release_v0.10.0/wheel_%j.log
set -uo pipefail
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
export PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring
export POETRY_NO_INTERACTION=1
rm -rf /tmp/wheelcheck_$SLURM_JOB_ID && mkdir -p /tmp/wheelcheck_$SLURM_JOB_ID
poetry build -f wheel -o /tmp/wheelcheck_$SLURM_JOB_ID
echo "=== build rc=$? ==="
python3 - "$SLURM_JOB_ID" <<'PY'
import glob, sys, zipfile
whl = glob.glob(f"/tmp/wheelcheck_{sys.argv[1]}/*.whl")
if not whl:
    print("NO WHEEL BUILT"); sys.exit(1)
print("wheel:", whl[0])
names = zipfile.ZipFile(whl[0]).namelist()
for f in ("s3_reader_creds.enc", "s3_reader_creds.gpg"):
    hit = [n for n in names if n.endswith(f)]
    print(f"  {f:26s} {'SHIPPED ' + hit[0] if hit else '*** MISSING ***'}")
PY
