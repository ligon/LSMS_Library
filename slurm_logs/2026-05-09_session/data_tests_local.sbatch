#!/bin/bash
#SBATCH --job-name=lsms-data-tests-local
#SBATCH --account=co_carleton
#SBATCH --partition=savio2
#SBATCH --qos=savio_lowprio
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=24
#SBATCH --time=02:00:00
#SBATCH --output=slurm_logs/2026-05-09_session/data_tests_local_%j.out
#SBATCH --error=slurm_logs/2026-05-09_session/data_tests_local_%j.err

set -uo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

# Mirror what GH Actions data-tests does, on a whole 24-core savio2 node:
#   1. Build Uganda tables via Make.
#   2. Run pytest --rebuild-caches with full parallelism.
# Surface every step so the eventual report is self-documenting.

NPROC=${SLURM_CPUS_ON_NODE:-24}

echo "=================================================="
echo "=== sbatch prolog $(date)"
echo "=================================================="
echo "node:        $(hostname)"
echo "jobid:       ${SLURM_JOB_ID:-unset}"
echo "cpus:        $NPROC"
echo "git head:    $(git log -1 --oneline)"
echo "branch:      $(git branch --show-current)"
echo "free mem:    $(free -h | awk 'NR==2 {print $7}')"
echo

LOCAL_VENV=/local/job${SLURM_JOB_ID}/venv
echo "=== migrate venv to $LOCAL_VENV ==="
mkdir -p "$LOCAL_VENV"
tar -C .venv -cf - . | tar -C "$LOCAL_VENV" -xf -
PY="$LOCAL_VENV/bin/python"
PT="$LOCAL_VENV/bin/pytest"
echo "  python:    $PY"
echo

echo "=================================================="
echo "=== STEP 1: Build Uganda tables (mirrors GH Actions 'Build Uganda tables')"
echo "=================================================="
cd lsms_library/countries/Uganda/_
echo "cwd: $(pwd)"
echo "PATH: $LOCAL_VENV/bin:\$PATH (so make rules pick up the right python)"
export PATH="$LOCAL_VENV/bin:$PATH"

# Run make -j$NPROC; capture exit code; do NOT abort the job on failure --
# we want the full picture, so continue to STEP 2.
# NOTE: ``make`` is a system binary, not a venv binary -- earlier sbatch
# revisions used $LOCAL_VENV/bin/make and silently exited 127 ("No such
# file or directory") without ever building Uganda's tables.  Use plain
# ``make`` from $PATH (which already has $LOCAL_VENV/bin prepended so
# child processes find the right ``python``).
echo "make -j$NPROC starting at $(date)"
which make
make -j"$NPROC" 2>&1
MAKE_RC=$?
echo
echo "=== make exit: $MAKE_RC ==="
cd "$REPO"
echo

echo "=================================================="
echo "=== STEP 2: Full pytest --rebuild-caches"
echo "=================================================="
# --ff (fail-first): re-runs the most recently failing test first, then
# the rest, on iteration after the first fix.  On a fresh suite this
# falls back to normal ordering, so it's safe to keep on.  See
# Makefile target ``test-ff`` for the same flag in interactive use.
echo "pytest -n $NPROC --dist=loadfile --rebuild-caches --ff starting at $(date)"
"$PT" -n "$NPROC" --dist=loadfile --rebuild-caches --ff --tb=short 2>&1
PT_RC=$?
echo
echo "=== pytest exit: $PT_RC ==="
echo

echo "=================================================="
echo "=== verdict $(date)"
echo "=================================================="
echo "make    exit: $MAKE_RC  ($([ $MAKE_RC -eq 0 ] && echo PASS || echo FAIL))"
echo "pytest  exit: $PT_RC  ($([ $PT_RC -eq 0 ] && echo PASS || echo FAIL))"
echo
if [ "$MAKE_RC" -eq 0 ] && [ "$PT_RC" -eq 0 ]; then
    echo "ALL GREEN -- master CI's data-tests should pass after this branch lands."
    exit 0
else
    echo "STILL FAILING -- inspect the per-step output above for the next latent bug."
    exit 1
fi
