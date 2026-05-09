#!/bin/bash
#SBATCH --job-name=uganda-hybrid-v-test
#SBATCH --account=co_carleton
#SBATCH --partition=savio2_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:15:00
#SBATCH --output=slurm_logs/2026-05-09_session/run_uganda_hybrid_v_test_%j.out
#SBATCH --error=slurm_logs/2026-05-09_session/run_uganda_hybrid_v_test_%j.err

set -euo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

echo "=== sbatch prolog ==="
echo "node:        $(hostname)"
echo "jobid:       ${SLURM_JOB_ID:-unset}"
echo "git head:    $(git log -1 --oneline)"
echo "started:     $(date)"
echo

LOCAL_VENV=/local/job${SLURM_JOB_ID}/venv
echo "=== migrate venv to $LOCAL_VENV ==="
mkdir -p "$LOCAL_VENV"
tar -C .venv -cf - . | tar -C "$LOCAL_VENV" -xf -
PY="$LOCAL_VENV/bin/python"
echo

echo "=== run failing test (warm cache) ==="
"$PY" -m pytest -v --no-header --tb=short \
    tests/test_sample.py::TestUganda2009MarketFallback \
    -W ignore 2>&1
RC1=$?
echo
echo "=== exit (warm cache): $RC1 ==="
echo

# Now repeat with caches forced cold to confirm it's not cache-state masking
echo "=== run failing test (LSMS_NO_CACHE=1) ==="
LSMS_NO_CACHE=1 "$PY" -m pytest -v --no-header --tb=short \
    tests/test_sample.py::TestUganda2009MarketFallback \
    -W ignore 2>&1
RC2=$?
echo
echo "=== exit (cold cache): $RC2 ==="
echo "finished:    $(date)"

# We exit OK if both runs pass; otherwise propagate the first failure.
if [ "$RC1" -ne 0 ]; then exit "$RC1"; fi
if [ "$RC2" -ne 0 ]; then exit "$RC2"; fi
exit 0
