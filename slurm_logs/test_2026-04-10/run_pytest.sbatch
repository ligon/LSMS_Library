#!/bin/bash
#SBATCH --account=fc_jevons
#SBATCH --partition=savio4_htc
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --job-name=lsms_pytest
#SBATCH --output=slurm_logs/test_2026-04-10/pytest_%j.out
#SBATCH --error=slurm_logs/test_2026-04-10/pytest_%j.err

set -euo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

# Environment
export PYTHONPATH="$REPO"
export LSMS_BUILD_BACKEND=make          # bypass DVC lock contention
export LSMS_SKIP_AUTH=${LSMS_SKIP_AUTH:-1}

echo "=== Job metadata ==="
echo "JobID:      $SLURM_JOB_ID"
echo "Node:       $(uname -n)"
echo "Cores:      $SLURM_CPUS_PER_TASK"
echo "Branch:     $(git rev-parse --abbrev-ref HEAD)"
echo "Commit:     $(git rev-parse --short HEAD)"
echo "Date:       $(date)"
echo

echo "=== Running pytest ==="
# -x: stop on first failure is too strict given 3 expected sample() failures;
#     let it run to completion so we see the full picture.
# --tb=short: compact tracebacks for easier scrolling.
# -ra: summary of all non-passed at the end.
.venv/bin/python -m pytest tests/ \
    --tb=short \
    -ra \
    -v \
    2>&1 | tee slurm_logs/test_2026-04-10/pytest_${SLURM_JOB_ID}.log

rc=${PIPESTATUS[0]}
echo
echo "=== pytest exit code: $rc ==="
exit $rc
