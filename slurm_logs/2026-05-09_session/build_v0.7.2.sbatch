#!/bin/bash
#SBATCH --job-name=build-lsms-0.7.2
#SBATCH --account=co_carleton
#SBATCH --partition=savio2_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:20:00
#SBATCH --output=slurm_logs/2026-05-09_session/build_v0.7.2_%j.out
#SBATCH --error=slurm_logs/2026-05-09_session/build_v0.7.2_%j.err

set -euo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

echo "=== sbatch prolog ==="
echo "node:        $(hostname)"
echo "jobid:       ${SLURM_JOB_ID:-unset}"
echo "started:     $(date)"
echo "git head:    $(git log -1 --oneline)"
echo "git describe: $(git describe --tags HEAD 2>&1)"
echo

# Migrate venv to node-local SSD per .venv.lustre/README_WHY_THIS_EXISTS.md
# Option A.  poetry + poetry-dynamic-versioning are already in .venv.
LOCAL_VENV=/local/job${SLURM_JOB_ID}/venv
echo "=== migrate venv to $LOCAL_VENV ==="
mkdir -p "$LOCAL_VENV"
tar -C .venv -cf - . | tar -C "$LOCAL_VENV" -xf -
PY="$LOCAL_VENV/bin/python"
PO="$LOCAL_VENV/bin/poetry"
echo "  python: $("$PY" --version)"
echo "  poetry: $("$PO" --version 2>&1)"
echo

echo "=== verify poetry-dynamic-versioning plugin ==="
"$PO" self show plugins 2>&1 | grep -i dynamic-version || {
    echo "(plugin missing -- installing now)"
    "$PO" self add "poetry-dynamic-versioning[plugin]>=1.0.0,<2.0.0" 2>&1 | tail -10
}
echo

echo "=== verify version resolution (should be 0.7.2, not 0.0.0) ==="
"$PO" version 2>&1 || true
echo

echo "=== preserve existing dist/ artifacts ==="
mkdir -p dist/
echo "Before build:"
ls -la dist/ 2>&1 || true
echo

echo "=== poetry build (PYTHON_KEYRING_BACKEND=null) ==="
PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring "$PO" build 2>&1
RC=$?
echo
echo "=== build exit: $RC ==="

if [ "$RC" -ne 0 ]; then
    echo "BUILD FAILED -- exiting"
    exit "$RC"
fi

echo
echo "=== verify wheel filename ==="
ls -la dist/ 2>&1
WHL=$(ls dist/lsms_library-*.whl 2>/dev/null | grep '0.7.2' | head -1)
TAR=$(ls dist/lsms_library-*.tar.gz 2>/dev/null | grep '0.7.2' | head -1)

if [ -z "$WHL" ]; then
    echo "ERROR: no 0.7.2 wheel produced"
    echo "(common cause: poetry-dynamic-versioning plugin not active -- wheel would be 0.0.0)"
    ls dist/ 2>&1
    exit 1
fi

echo
echo "=== sha256 ==="
sha256sum "$WHL" "$TAR" 2>&1 || sha256sum "$WHL" 2>&1
echo
echo "=== wheel metadata sanity ==="
"$PY" -c "
import zipfile, sys
with zipfile.ZipFile('$WHL') as z:
    for n in z.namelist():
        if n.endswith('METADATA'):
            print('--- METADATA (head) ---')
            print(z.read(n).decode().split(chr(10)*2, 1)[0])
            break
"

echo
echo "=== artifacts ready ==="
echo "  wheel: $WHL"
[ -n "$TAR" ] && echo "  sdist: $TAR"
echo
echo "finished:    $(date)"
