#!/bin/bash
#SBATCH --job-name=uganda-hc-cold
#SBATCH --account=co_carleton
#SBATCH --partition=savio2_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:15:00
#SBATCH --output=slurm_logs/2026-05-09_session/probe_uganda_hc_cold_%j.out
#SBATCH --error=slurm_logs/2026-05-09_session/probe_uganda_hc_cold_%j.err

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

echo "=== probe ==="
"$PY" -W ignore "$REPO/slurm_logs/2026-05-09_session/probe_uganda_hc_cold.py"
RC=$?
echo
echo "=== probe exit: $RC ==="
echo "finished:    $(date)"
exit "$RC"
