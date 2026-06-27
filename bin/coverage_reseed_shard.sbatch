#!/bin/bash
#SBATCH --job-name=cov-reseed-shard
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=01:30:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%A_%a.out
#SBATCH --error=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%A_%a.err
#
# One country of the MONTHLY authoritative reseed: clear that country's cache,
# then COLD-build from the clone's config -> a shard CSV. Submitted as an array
# by bin/coverage_reseed.sbatch. Per-task node-local venv (tar-pipe) because the
# squashfuse mount races under array concurrency (see the cube run, 2026-06-25).
set -u
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
WD="${COV_WORKDIR:?set COV_WORKDIR}"
SHARDS="${COV_SHARDROOT:?set COV_SHARDROOT}/shards"

mapfile -t COUNTRIES < "${COV_SHARDROOT}/countries.txt"
C="${COUNTRIES[$SLURM_ARRAY_TASK_ID]}"
echo "=== reseed shard: '$C'  node=$(hostname)  $(date) ==="

LV="/local/job${SLURM_JOB_ID}/venv"
mkdir -p "$LV"
tar -C "$REPO/.venv.lustre" -cf - . 2>/dev/null | tar -C "$LV" -xf - 2>/dev/null
PY="$LV/bin/python"
"$PY" -c 'import lsms_library, yaml, pandas' || { echo "venv provisioning FAILED"; exit 1; }

echo "--- cache clear '$C' (authoritative) ---"
"$PY" -m lsms_library.cli cache clear --country "$C" || echo "  (clear noop/failed)"
echo "--- cold build '$C' ---"
LSMS_COUNTRIES_ROOT="$WD/lsms_library/countries" \
    "$PY" "$WD/bench/matrix.py" --countries "$C" \
          --snapshot "$SHARDS/$C.csv" --no-html
echo "=== done '$C' rc=$? $(date) ==="
