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
#SBATCH -J rt1697-oth
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/logs/job2-%j.out
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/scripts/job_common.sh
setup_clones base head
FR=$R/frames/job2; mkdir -p $FR
TB=food_acquired,crop_production,food_quantities,food_prices,food_quantities__units
for side in base head; do
  runpy $side hash-$side $SCR/hashprobe.py $J/$side $R/results/job2_hash_$side.json > $R/logs/job2-hash-$side.log 2>&1 &
done
wait
for side in base head; do
  runpy $side $side-g1 $SCR/build.py $J/$side $side $FR Senegal,Mali,Guinea-Bissau $TB > $R/logs/job2-build-$side-g1.log 2>&1 &
  runpy $side $side-g2 $SCR/build.py $J/$side $side $FR Niger,Burkina_Faso $TB > $R/logs/job2-build-$side-g2.log 2>&1 &
done
wait
echo builds done
runpy head cmp $SCR/compare.py $FR Senegal,Mali,Guinea-Bissau,Niger,Burkina_Faso $R/results/job2_compare.json > $R/logs/job2-compare.log 2>&1; echo "compare rc=$?"
for side in base head; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side:"; git -C $J/$side status --porcelain | head -20
done
echo JOB2 DONE
