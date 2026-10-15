#!/bin/bash
#SBATCH --account=co_carleton
#SBATCH --partition=savio4_htc
#SBATCH --qos=savio_lowprio
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:15:00
#SBATCH --job-name=claude-headless-probe
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/venv_validate/headless_probe/%x_%j.out
cd "$HOME"
echo "host=$(hostname) nproc=$(nproc) job=$SLURM_JOB_ID"
export GH_REPO=ligon/LSMS_Library
time timeout 600 claude -p "Using the Bash tool, run: hostname; nproc; gh issue view 964 --json title -q .title. Return exactly those three lines of output and nothing else." \
  --output-format json --permission-mode bypassPermissions --max-turns 6 2>&1 | head -c 4000
echo; echo "exit=${PIPESTATUS[0]}"
