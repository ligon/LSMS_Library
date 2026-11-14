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
#SBATCH -J rt1697-old
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/logs/job1-%j.out
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/scripts/job_common.sh
setup_clones base head
FR=$R/frames/job1; mkdir -p $FR
for side in base head; do
  for C in Togo Benin CotedIvoire; do
    runpy $side $side-$C $SCR/build.py $J/$side $side $FR $C all > $R/logs/job1-build-$side-$C.log 2>&1 &
  done
done
wait
echo builds done
runpy head cmp $SCR/compare.py $FR Togo,Benin,CotedIvoire $R/results/job1_compare.json > $R/logs/job1-compare.log 2>&1; echo "compare rc=$?"
for side in base head; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side:"; git -C $J/$side status --porcelain | head -20
done
du -sh $J/priv-* 2>/dev/null
echo JOB1 DONE
