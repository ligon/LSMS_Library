#!/bin/bash
# Validate the NEW dependency closure (GH #959) before it is promoted.
#
# What this exists to answer: does the library still work under pyarrow 25
# (up two majors from 23, and it writes every cached parquet) and CFEDemands
# 0.10.0 (up from 0.8.2, which was BELOW the pyproject minimum)?
#
# Four deliberate departures from bin/coverage_lib.sh's cov_provision_venv,
# each of which is a filed defect:
#
#   1. Mounts the STAGED image .venv.sqfs.new, not the live .venv.sqfs.  The
#      live image is untouched until this run is green, so every other session
#      keeps the known-good closure.
#   2. Mounts at THIS job's own /local path and never repoints $MIRROR/.venv.
#      cov_provision_venv calls `savio_venv.sh mount`, which does repoint it,
#      and would yank the condo slice's venv from under it (GH #958).
#      slurm_logs/release_v0.10.0/burst_pytest.sh is the model here.
#   3. Clones the branch into this job's own /local rather than using the
#      mirror's working tree, which is on whatever branch the mirror has
#      checked out (ligon/sucoder#28).
#   4. Exports PYTHONPATH and ASSERTS identity.  The editable install has been
#      removed from the master (GH #957), so an unset PYTHONPATH is now a loud
#      ModuleNotFoundError instead of a silent import of the wrong checkout.
#
# Cache isolation: LSMS_DATA_DIR points at a fresh staging root so L2 is COLD
# -- which is the point, since a warm run would serve pyarrow-23-written
# parquets and exercise none of the new write path.  L1 (the 29 GB DVC blob
# cache) is symlinked to the live one: blobs are content-addressed, so sharing
# is safe and it avoids re-downloading the corpus from S3.  L2 is only ~310 MB.
#
#SBATCH --job-name=lsms-venv-validate
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3
#SBATCH --qos=savio_normal
#SBATCH --nodes=1
#SBATCH --time=10:00:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/venv_validate/%x_%j.out
#SBATCH --error=/global/scratch/fsa/fc_jevons/ligon/venv_validate/%x_%j.err
# Submitted from a /local clone, whose cwd does not exist on the target node.
# Without --chdir slurmstepd logs 'couldn't chdir ... going to /tmp instead'.
# Harmless (the script cds explicitly) but it is noise in every .err.
#SBATCH --chdir=/global/scratch/fsa/fc_jevons/ligon/venv_validate
set -uo pipefail

MIRROR=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
IMG="$MIRROR/.venv.sqfs.new"
BRANCH="${BRANCH:-development}"
CLONE="/local/job$SLURM_JOB_ID/mirrors/LSMS_Library"
MNT="/local/job$SLURM_JOB_ID/venv_img"
LIVE_CACHE=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library
STAGE="/global/scratch/fsa/fc_jevons/ligon/venv_validate/data_$SLURM_JOB_ID"
LOG="/global/scratch/fsa/fc_jevons/ligon/venv_validate/pytest_$SLURM_JOB_ID.log"

echo "=== node=$(hostname) cpus=$SLURM_CPUS_ON_NODE load=$(uptime | sed 's/.*average: //') ==="
[ -f "$IMG" ] || { echo "FATAL: staged image missing: $IMG"; exit 1; }

# --- 1. clone the branch into this job's own node-local disk ------------------
git clone --quiet --branch "$BRANCH" "$MIRROR" "$CLONE" || { echo "FATAL: clone failed"; exit 1; }
echo "clone at $(git -C "$CLONE" rev-parse --short HEAD) on $BRANCH"

# --- 2. mount the staged image at this job's own path ------------------------
shim="/local/job$SLURM_JOB_ID/.fuseshim"
mkdir -p "$shim" "$MNT"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
PATH="$shim:$PATH" /usr/libexec/apptainer/bin/squashfuse_ll "$IMG" "$MNT" \
    || { echo "FATAL: mount failed"; exit 1; }

# --- 3. identity assert, BEFORE 32 workers spin up --------------------------
export PYTHONPATH="$CLONE"
cd "$CLONE"
"$MNT/bin/python" -c "
import lsms_library, pyarrow, pandas, numpy
assert lsms_library.__file__.startswith('$CLONE'), lsms_library.__file__
print('venv OK :', lsms_library.__file__)
print('pyarrow :', pyarrow.__version__)
print('pandas  :', pandas.__version__)
print('numpy   :', numpy.__version__)
import cfe; print('cfe     :', getattr(cfe, '__version__', 'n/a'))
" || { echo "FATAL: identity/import check failed"; fusermount -u "$MNT"; exit 1; }

# --- 4. cache root -----------------------------------------------------------
# TWO MODES, and both are needed.  Collection in this suite is DATA-DRIVEN:
# tests/test_table_structure.py parametrizes over the (country, table) cells it
# finds in the cache, so it yields 66 cases against a cold root and 2,400
# against a warm one.  A cold run therefore collects 4,441 of 6,775 tests and
# CANNOT check the structure of what it just wrote.
#
#   WARM_FROM unset -> fresh staging root, L2 COLD.  Exercises the parquet
#                      WRITE path, which is the only reason to care about a
#                      pyarrow major bump.
#   WARM_FROM=<dir>  -> reuse that root, L2 WARM.  Collects the other 2,334
#                      tests and checks the structure of parquets the cold
#                      run wrote.
#
# L1 (the 29 GB DVC blob cache) is shared either way: blobs are
# content-addressed, so sharing is safe and it avoids re-downloading from S3.
if [ -n "${WARM_FROM:-}" ]; then
    [ -d "$WARM_FROM" ] || { echo "FATAL: WARM_FROM does not exist: $WARM_FROM"; exit 1; }
    export LSMS_DATA_DIR="$WARM_FROM"
    echo "LSMS_DATA_DIR=$WARM_FROM  (L2 WARM, reused; read-side pass)"
else
    mkdir -p "$STAGE"
    ln -sfn "$LIVE_CACHE/dvc-cache" "$STAGE/dvc-cache"
    export LSMS_DATA_DIR="$STAGE"
    echo "LSMS_DATA_DIR=$STAGE  (L2 cold; L1 -> $LIVE_CACHE/dvc-cache)"
fi
echo "collected: $("$MNT/bin/python" -m pytest --collect-only -q 2>/dev/null | tail -1)"

# --- 5. thread discipline (CLAUDE.md scrum-master addendum 3b) ---------------
# OpenBLAS sizes its pool from nproc, not from the worker count, so -n 32 on a
# 32-core node asks for 1024 threads and the kernel refuses.  Pin before -n.
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1

# --- 6. the run.  Output to a FILE: an orphaned xdist worker holds a pipe open
#        and the summary never arrives.
"$MNT/bin/python" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q > "$LOG" 2>&1
rc=$?
echo "=== pytest exit=$rc ==="
tail -25 "$LOG"
echo "=== full log: $LOG ==="
echo "=== L2 root: $LSMS_DATA_DIR ($(du -sh --exclude=dvc-cache "$LSMS_DATA_DIR" 2>/dev/null | cut -f1)) ==="
fusermount -u "$MNT" 2>/dev/null
exit $rc
