#!/bin/bash
#SBATCH --job-name=serbia2007-verify
#SBATCH --account=co_carleton
#SBATCH --partition=savio2_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:15:00
#SBATCH --output=slurm_logs/2026-05-09_session/verify_serbia_fix_%j.out
#SBATCH --error=slurm_logs/2026-05-09_session/verify_serbia_fix_%j.err

set -euo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

echo "=== sbatch prolog ==="
echo "node:        $(hostname)"
echo "jobid:       ${SLURM_JOB_ID:-unset}"
echo "cpus:        ${SLURM_CPUS_ON_NODE:-unset}"
echo "git head:    $(git log -1 --oneline)"
echo "branch:      $(git branch --show-current)"
echo "started:     $(date)"
echo

# Migrate the venv from Lustre to node-local SSD per
# .venv.lustre/README_WHY_THIS_EXISTS.md Option A.
LOCAL_VENV=/local/job${SLURM_JOB_ID}/venv
echo "=== migrate venv to $LOCAL_VENV ==="
mkdir -p "$LOCAL_VENV"
tar -C .venv -cf - . | tar -C "$LOCAL_VENV" -xf -
PY="$LOCAL_VENV/bin/python"
echo "  python:    $PY"
"$PY" -c 'import sys; print("  python ver:", sys.version.split()[0])'
echo

echo "=== verify ==="
"$PY" -W ignore "$REPO/slurm_logs/2026-05-09_session/verify_serbia_fix.py"
RC=$?
echo
echo "=== verify exit: $RC ==="
echo "finished:    $(date)"
exit "$RC"
