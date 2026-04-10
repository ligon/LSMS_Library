#!/bin/bash
#SBATCH --account=co_carleton
#SBATCH --partition=savio4_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --job-name=lsms_release_0_6_0
#SBATCH --output=slurm_logs/test_2026-04-10/release_%j.out
#SBATCH --error=slurm_logs/test_2026-04-10/release_%j.err

set -euo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

# Environment
export PYTHONPATH="$REPO"
export LSMS_BUILD_BACKEND=make          # bypass DVC lock contention
export LSMS_SKIP_AUTH=${LSMS_SKIP_AUTH:-1}
export PATH="$HOME/.local/bin:$PATH"    # for poetry

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

# Sanity check: we should be on master after the merge
branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$branch" != "master" ]; then
    echo "ERROR: expected to be on 'master', but on '$branch'"
    exit 2
fi

# Sanity check: tag v0.6.0 must not already exist
if git rev-parse "v0.6.0" >/dev/null 2>&1; then
    echo "ERROR: tag v0.6.0 already exists"
    exit 3
fi

echo "=== Running make release v=0.6.0 ==="
# Makefile uses POETRY=poetry by default; it invokes `poetry run pytest $PYTEST_ARGS`
# which defaults to -n auto --dist=loadfile (see Makefile PYTEST_ARGS default).
# The release target: (1) validates tag absence, (2) runs pytest, (3) creates
# annotated tag, (4) builds dist/.
make release v=0.6.0 \
    2>&1 | tee slurm_logs/test_2026-04-10/release_${SLURM_JOB_ID}.log

rc=${PIPESTATUS[0]}
echo
echo "=== make release exit code: $rc ==="

if [ "$rc" -eq 0 ]; then
    echo "=== Post-release state ==="
    echo "Tags:"
    git tag --list "v0.*" | tail -5
    echo "Dist:"
    ls -la dist/ 2>&1
fi

exit $rc
