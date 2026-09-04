#!/bin/bash
#SBATCH --job-name=glss-nutrition
#SBATCH --account=co_carleton
#SBATCH --partition=savio4_htc
#SBATCH --qos=carleton_htc4_normal
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/tmp/wt-glss-fct/slurm_logs/ghana_audit/nutrition_verify_%j.log
# GH #563 STEP 3 verification: cold-build GhanaLSS `nutrition` through the real
# Country API and run every check the brief requires.
#
# Config comes from the WORKTREE (LSMS_COUNTRIES_ROOT); library code from the
# main checkout's venv image.  This is a config-only change plus one country
# script, so library identity does not matter (CLAUDE.md, GH #436).
#
# Deliberately does NOT call `bin/savio_venv.sh mount`: that rewrites the main
# checkout's .venv symlink to THIS job's /local mount and would break the
# interactive slice.  Mount by hand, exactly as matrix_refresh_ghana.sbatch does.
set -u
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
WT=/global/scratch/fsa/fc_jevons/ligon/tmp/wt-glss-fct
cd "$REPO"

SQUASHFUSE="$(grep -oE 'SQUASHFUSE="?[^" ]+' bin/savio_venv.sh | head -1 | sed -E 's/SQUASHFUSE="?//')"
SQUASHFUSE="$(eval echo "$SQUASHFUSE")"
mnt="/local/job$SLURM_JOB_ID/venv_img"
shim="/local/job$SLURM_JOB_ID/.fuseshim"
mkdir -p "$shim" "$mnt"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
if [ -c /dev/fuse ] && [ -x "$SQUASHFUSE" ]; then
  PATH="$shim:$PATH" "$SQUASHFUSE" .venv.sqfs "$mnt" || { echo "squashfuse failed"; exit 1; }
else
  unsquashfs -f -d "$mnt" .venv.sqfs >/dev/null || { echo "unsquashfs failed"; exit 1; }
fi
PY="$mnt/bin/python"
export PATH="$mnt/bin:$PATH"          # `make` shells out to bare `python`

export LSMS_COUNTRIES_ROOT="$WT/lsms_library/countries"
export LSMS_DATA_DIR=/global/scratch/fsa/fc_jevons/ligon/tmp/lsms_data_glss_fct
# GH #764: the framework's `make -j` otherwise defaults to the physical node
# count, not this job's allocation.
export LSMS_MAKE_JOBS="$SLURM_CPUS_ON_NODE"

echo "=== $(date) node=$(hostname) cpus=$SLURM_CPUS_ON_NODE ==="
echo "HEAD=$(git -C "$WT" rev-parse --short HEAD) branch=$(git -C "$WT" branch --show-current)"
echo "LSMS_MAKE_JOBS=$LSMS_MAKE_JOBS"

# Force a genuinely cold descent: the L2-country parquet's hash is stale after
# the config edits, but be explicit rather than trusting that.
rm -f "$LSMS_DATA_DIR/GhanaLSS/var/nutrition.parquet"

"$PY" "$WT/slurm_logs/ghana_audit/nutrition_verify.py"
rc=$?
echo "=== verify rc=$rc $(date) ==="
fusermount -u "$mnt" 2>/dev/null
exit $rc
