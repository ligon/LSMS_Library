#!/bin/bash
#SBATCH --job-name=verify-silent-skip-warn
#SBATCH --account=co_carleton
#SBATCH --partition=savio2_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=00:15:00
#SBATCH --output=slurm_logs/2026-05-09_session/verify_silent_skip_warn_%j.out
#SBATCH --error=slurm_logs/2026-05-09_session/verify_silent_skip_warn_%j.err

set -uo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

echo "=== sbatch prolog ==="
echo "node:    $(hostname)"
echo "jobid:   ${SLURM_JOB_ID:-unset}"
echo "git:     $(git log -1 --oneline)"
echo "branch:  $(git branch --show-current)"
echo "started: $(date)"
echo

LOCAL_VENV=/local/job${SLURM_JOB_ID}/venv
mkdir -p "$LOCAL_VENV"
tar -C .venv -cf - . | tar -C "$LOCAL_VENV" -xf -
PT="$LOCAL_VENV/bin/pytest"
echo

echo "=== STEP 1: new unit tests (should all pass) ==="
"$PT" -v --no-header --tb=short tests/test_join_v_silent_skip_warn.py
RC1=$?
echo "=== exit: $RC1 ==="
echo

echo "=== STEP 2: Uganda hybrid-v tests (must NOT trigger spurious warning) ==="
# Capture warnings via -W default (default behavior); pipe through grep
# afterward to detect any silent-skip warnings.
"$PT" --no-header --tb=short tests/test_sample.py::TestUganda2009MarketFallback 2>&1 | tee /tmp/uganda_hybrid_v.log | tail -5
RC2=$?
echo "=== exit: $RC2 ==="
echo

echo "=== STEP 3: scan STEP 2 output for spurious silent-skip warnings ==="
if grep -q '100% NaN v' /tmp/uganda_hybrid_v.log; then
    echo "FAIL: silent-skip warning fired on Uganda hybrid-v (should not have):"
    grep -B1 -A2 '100% NaN v' /tmp/uganda_hybrid_v.log
    RC3=1
else
    echo "PASS: no spurious silent-skip warning on Uganda hybrid-v test"
    RC3=0
fi
echo

echo "=== verdict $(date) ==="
echo "  unit tests:           $RC1  ($([ $RC1 -eq 0 ] && echo PASS || echo FAIL))"
echo "  Uganda hybrid-v:      $RC2  ($([ $RC2 -eq 0 ] && echo PASS || echo FAIL))"
echo "  no spurious warning:  $RC3  ($([ $RC3 -eq 0 ] && echo PASS || echo FAIL))"

if [ "$RC1" -eq 0 ] && [ "$RC2" -eq 0 ] && [ "$RC3" -eq 0 ]; then
    exit 0
fi
exit 1
