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
#SBATCH -J rt2-1697-A
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam2/logs/jobA-%j.out
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam2/scripts/job_common.sh
setup_clones prev head
FR=$J/frames; mkdir -p $FR
for side in prev head; do
  for C in Togo CotedIvoire Benin; do
    runpy $side $side-$C $SCR/build.py $J/$side $side $FR $C > $O/logs/jobA-build-$side-$C.log 2>&1 &
  done
done
wait
echo "builds done $(date)"
runpy head cmp $SCR/compare.py $FR Togo,CotedIvoire,Benin $O/results/jobA_compare.json > $O/logs/jobA-compare.log 2>&1; echo "compare rc=$?"
runpy head kgiso $SCR/kgiso.py $J/head $FR $O/results/jobA_kgiso.json Togo,CotedIvoire > $O/logs/jobA-kgiso.log 2>&1; echo "kgiso rc=$?"
for side in prev head; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side:"; git -C $J/$side status --porcelain | head -20
done
du -sh $J/priv-* $FR 2>/dev/null
echo "JOBA DONE $(date)"
