#!/bin/bash
#SBATCH --job-name=cube-shard
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=01:30:00
#SBATCH --array=0-33%10
#SBATCH --output=slurm_logs/2026-06-25_coverage_matrix/cube/logs/shard_%A_%a.out
#SBATCH --error=slurm_logs/2026-06-25_coverage_matrix/cube/logs/shard_%A_%a.err

set -u   # NOT pipefail: the tar|tar venv copy can emit benign warnings.
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
CUBE=slurm_logs/2026-06-25_coverage_matrix/cube

mapfile -t COUNTRIES < "$CUBE/countries.txt"
C="${COUNTRIES[$SLURM_ARRAY_TASK_ID]}"
echo "=== task ${SLURM_ARRAY_TASK_ID}: country='$C'  node=$(hostname)  $(date) ==="

# Per-task node-local venv via tar-pipe from the Lustre master. We do NOT use
# bin/savio_venv.sh mount here: its squashfuse mount RACES when many array tasks
# share a node (observed broken mounts -> ModuleNotFoundError: yaml -> exit 127).
# The tar-pipe gives each task an independent venv (the proven probe_* pattern).
LOCAL_VENV="/local/job${SLURM_JOB_ID}/venv"
echo "--- provisioning venv -> $LOCAL_VENV ---"
mkdir -p "$LOCAL_VENV"
tar -C "$REPO/.venv.lustre" -cf - . 2>/dev/null | tar -C "$LOCAL_VENV" -xf - 2>/dev/null
PY="$LOCAL_VENV/bin/python"
"$PY" -c 'import lsms_library, yaml, pandas' || { echo "venv provisioning FAILED"; exit 1; }

# Authoritative readout: clear THIS country's derived caches first so a stale
# script-path wave parquet (GH #479 hazard, e.g. Uganda earnings) can't shadow
# the build. Per-country clear -> isolated, safe to run concurrently across
# different countries (CLAUDE.md addendum #1). Rebuilds a clean warm cache.
echo "--- cache clear $C ---"
"$PY" -m lsms_library.cli cache clear --country "$C" || echo "  (clear noop/failed; continuing)"

# One country's full readiness cube, built from a clean cache.
echo "--- build $C ---"
"$PY" bench/matrix.py --countries "$C" \
      --snapshot "$CUBE/shards/$C.csv" --no-html
rc=$?
echo "=== done '$C' rc=$rc  $(date) ==="
exit $rc
