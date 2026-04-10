#!/bin/bash
#SBATCH --account=co_carleton
#SBATCH --partition=savio4_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --job-name=lsms_pytest_xdist
#SBATCH --output=slurm_logs/test_2026-04-10/pytest_xdist_%j.out
#SBATCH --error=slurm_logs/test_2026-04-10/pytest_xdist_%j.err

set -euo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

# Environment
export PYTHONPATH="$REPO"
export LSMS_BUILD_BACKEND=make          # bypass DVC lock contention
export LSMS_SKIP_AUTH=${LSMS_SKIP_AUTH:-1}

# Saturate what Slurm actually gave us, not what we asked for.
# On whole-node partitions $SLURM_CPUS_ON_NODE > $SLURM_CPUS_PER_TASK.
NPROC="${SLURM_CPUS_ON_NODE:-${SLURM_CPUS_PER_TASK:-1}}"

echo "=== Job metadata ==="
echo "JobID:          $SLURM_JOB_ID"
echo "Node:           $(uname -n)"
echo "Partition:      $SLURM_JOB_PARTITION"
echo "Account:        $SLURM_JOB_ACCOUNT"
echo "cpus-per-task:  $SLURM_CPUS_PER_TASK"
echo "cpus-on-node:   ${SLURM_CPUS_ON_NODE:-unset}"
echo "NPROC (used):   $NPROC"
echo "Branch:         $(git rev-parse --abbrev-ref HEAD)"
echo "Commit:         $(git rev-parse --short HEAD)"
echo "Date:           $(date)"
echo

echo "=== Running pytest -n $NPROC --dist=loadfile ==="
# --dist=loadfile: group tests from the same file into the same worker.
# This prevents multiple workers from invoking `make` concurrently against
# the same Uganda table in test_uganda_tables.py, and lets test_sample.py's
# module-level _sample_cache behave as designed (per-file, not per-test).
# Cross-file parallelism is preserved — test_sample, test_uganda_invariance,
# test_feature etc. still run on different workers concurrently.
.venv/bin/python -m pytest tests/ \
    -n "$NPROC" \
    --dist=loadfile \
    --tb=short \
    -ra \
    2>&1 | tee slurm_logs/test_2026-04-10/pytest_xdist_${SLURM_JOB_ID}.log

rc=${PIPESTATUS[0]}
echo
echo "=== pytest exit code: $rc ==="
exit $rc
