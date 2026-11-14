#!/bin/bash
# NOT SUBMITTED by the red team (the brief caps jobs at 3).  Authorise with:
#   sbatch /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/scripts/job4_labels.sh
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 1:00:00
#SBATCH --exclude=n0035.savio4
#SBATCH -J rt1697-lbl
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/logs/job4-%j.out
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/scripts/job_common.sh
setup_clones head
# checks2021.py with the StataReader fix: re-runs shock_labels and varlabels (and the rest) on job1's frames.
runpy head lbl $SCR/checks2021.py $J/head $R/frames/job1/head $R/results/job4_checks_labels.json > $R/logs/job4-labels.log 2>&1
echo "labels rc=$?"; echo JOB4 DONE
