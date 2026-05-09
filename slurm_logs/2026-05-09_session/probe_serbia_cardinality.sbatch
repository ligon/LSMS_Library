#!/bin/bash
#SBATCH --job-name=serbia2007-cardinality
#SBATCH --account=co_carleton
#SBATCH --partition=savio2_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:10:00
#SBATCH --output=slurm_logs/2026-05-09_session/probe_serbia_cardinality_%j.out
#SBATCH --error=slurm_logs/2026-05-09_session/probe_serbia_cardinality_%j.err

set -euo pipefail

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"

echo "=== sbatch prolog ==="
echo "node:        $(hostname)"
echo "jobid:       ${SLURM_JOB_ID:-unset}"
echo "cpus:        ${SLURM_CPUS_ON_NODE:-unset}"
echo "started:     $(date)"
echo

# Per .venv.lustre/README_WHY_THIS_EXISTS.md Option A: tar-pipe the
# Lustre venv to node-local SSD for fast imports.  The .venv symlink
# is already pointed at .venv.lustre by Option B (login-node fix
# applied earlier in the session); we re-point it to the new SSD copy.
LOCAL_VENV=/local/job${SLURM_JOB_ID}/venv
echo "=== migrate venv to $LOCAL_VENV ==="
mkdir -p "$LOCAL_VENV"
tar -C .venv -cf - . | tar -C "$LOCAL_VENV" -xf -
# Replace the .venv symlink for this job's process tree only -- by
# pointing PYTHONHOME-equivalent path; safer is to call $LOCAL_VENV/bin/python
# directly so we don't disturb the on-disk symlink that other sessions share.
PY="$LOCAL_VENV/bin/python"
echo "  python:    $PY"
"$PY" -c 'import sys; print("  python ver:", sys.version.split()[0])'
echo

echo "=== probe ==="
"$PY" -W ignore "$REPO/slurm_logs/2026-05-09_session/probe_serbia_cardinality.py"
RC=$?
echo
echo "=== probe exit: $RC ==="
echo "finished:    $(date)"
exit "$RC"
