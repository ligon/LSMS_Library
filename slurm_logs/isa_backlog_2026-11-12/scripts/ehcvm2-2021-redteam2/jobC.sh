#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 3:00:00
#SBATCH --exclude=n0035.savio4
#SBATCH -J rt2-1697-C
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam2/logs/jobC-%j.out
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam2/scripts/job_common.sh
setup_clones prev head
FR=$J/frames; mkdir -p $FR
runpy prev fa-prev $SCR/fa_only.py $J/prev prev $FR Togo > $O/logs/jobC-fa-prev.log 2>&1 &
runpy head fa-head $SCR/fa_only.py $J/head head $FR Togo > $O/logs/jobC-fa-head.log 2>&1 &
wait
echo "fa built $(date)"
runpy head kgiso2 $SCR/kgiso2.py $J/head $FR $O/results/jobC_kgiso2.json > $O/logs/jobC-kgiso2.log 2>&1; echo "kgiso2 rc=$?"
for side in prev head; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  git -C $J/$side status --porcelain | head -5
done
echo "JOBC DONE $(date)"
