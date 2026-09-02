#!/bin/bash
# Full pytest run for the v0.10.0 release check, on a burst allocation.
# Mounts the squashfs venv at THIS job's own /local path and uses it directly --
# deliberately WITHOUT repointing the shared $REPO/.venv symlink, which belongs
# to the persistent condo slice and would break it.
#SBATCH --job-name=lsms-release-pytest
#SBATCH --output=slurm_logs/release_v0.10.0/pytest_%j.log
set -uo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
IMG="$REPO/.venv.sqfs"
SQUASHFUSE=/usr/libexec/apptainer/bin/squashfuse_ll
MNT="/local/job$SLURM_JOB_ID/venv_img"

echo "=== node=$(hostname) cpus=$SLURM_CPUS_ON_NODE ==="
shim="/local/job$SLURM_JOB_ID/.fuseshim"
mkdir -p "$shim" "$MNT"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
PATH="$shim:$PATH" "$SQUASHFUSE" "$IMG" "$MNT" || { echo "mount failed"; exit 1; }

export PYTHONPATH="$REPO"
"$MNT/bin/python" -c "import lsms_library; assert lsms_library.__file__.startswith('$REPO'), lsms_library.__file__; print('venv OK:', lsms_library.__file__)" || exit 1

cd "$REPO"
"$MNT/bin/python" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q
rc=$?
echo "=== pytest exit=$rc ==="
fusermount -u "$MNT" 2>/dev/null
exit $rc
