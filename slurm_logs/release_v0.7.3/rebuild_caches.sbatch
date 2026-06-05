#!/bin/bash
#SBATCH --job-name=lsms-rebuild-v073
#SBATCH --account=co_carleton
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=10:00:00
#SBATCH --requeue
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/release_v0.7.3/rebuild_%j.out
#SBATCH --open-mode=append
#
# Clean-room release gate for v0.7.3: cold `pytest --rebuild-caches` across all
# countries, fully isolated from the interactive session's shared parquet cache
# and shared `.venv` symlink.
#
# Isolation strategy (see CLAUDE.md "Cache Behavior" + scrum-master-hpc skill):
#   * node-local venv bootstrapped from .venv.lustre (never touches repo .venv)
#   * dedicated LSMS_DATA_DIR on scratch (fresh L2; survives preemption/requeue)
#   * L1 dvc-cache symlinked to the shared blob cache (content-addressed, safe to
#     share; --rebuild-caches clears L2 only, so blobs are not re-downloaded)
#   * run_make_target build_env() inherits sys.executable -> all make-shellouts
#     use this node-local venv automatically.

set -uo pipefail
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

echo "=== node=$(hostname) job=$SLURM_JOB_ID cpus=${SLURM_CPUS_ON_NODE:-?} HEAD=$(git rev-parse --short HEAD) $(date) ==="

# 1. node-local venv from the cold Lustre backup (fast imports on node SSD)
VENV=/local/job${SLURM_JOB_ID}/venv
mkdir -p "$VENV"
echo "--- bootstrapping venv -> $VENV"
tar -C "$REPO/.venv.lustre" -cf - . | tar -C "$VENV" -xf -
PY="$VENV/bin/python"
export PATH="$VENV/bin:$PATH"
"$PY" -c "import lsms_library, pytest; print('venv OK; pytest', pytest.__version__)" || { echo "VENV BOOTSTRAP FAILED"; exit 2; }

# 2. isolated data dir (fresh L2) on scratch; reuse shared L1 blob cache
export LSMS_DATA_DIR=/global/scratch/fsa/fc_jevons/ligon/mirrors/lsms_rebuild_data_v0.7.3
mkdir -p "$LSMS_DATA_DIR"
SHARED_L1="$HOME/.local/share/lsms_library/dvc-cache"
if [ ! -e "$LSMS_DATA_DIR/dvc-cache" ] && [ -d "$SHARED_L1" ]; then
    ln -s "$SHARED_L1" "$LSMS_DATA_DIR/dvc-cache"
fi
echo "--- LSMS_DATA_DIR=$LSMS_DATA_DIR  (L1 -> $SHARED_L1)"

# 3. cold rebuild + full test suite
NPROC=${SLURM_CPUS_ON_NODE:-16}
echo "=== pytest --rebuild-caches -n $NPROC --dist=loadfile  starting $(date) ==="
"$PY" -m pytest -n "$NPROC" --dist=loadfile --rebuild-caches -q
RC=$?
echo "=== pytest exit=$RC  $(date) ==="
exit $RC
