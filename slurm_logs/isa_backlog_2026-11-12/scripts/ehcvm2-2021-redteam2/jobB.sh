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
#SBATCH -J rt2-1697-B
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam2/logs/jobB-%j.out
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam2/scripts/job_common.sh
setup_clones prev head
runpy head raw $SCR/rawchecks.py $J/head $O/results/jobB_rawchecks.json > $O/logs/jobB-rawchecks.log 2>&1 &
RAWPID=$!
runpy prev hash-prev $SCR/hashprobe.py $J/prev $O/results/jobB_hash_prev.json > $O/logs/jobB-hash-prev.log 2>&1 &
H1=$!
runpy head hash-head $SCR/hashprobe.py $J/head $O/results/jobB_hash_head.json > $O/logs/jobB-hash-head.log 2>&1 &
H2=$!
wait $H1 $H2
echo "hash probes done $(date)"
runpy head hashcmp $SCR/hashcmp.py $O/results/jobB_hash_prev.json $O/results/jobB_hash_head.json $O/results/jobB_hashcmp.json > $O/logs/jobB-hashcmp.log 2>&1; echo "hashcmp rc=$?"
cd $J/head
TESTS=""
for t in test_togo_2021_wave test_civ_2021_wave test_benin_2021_wave test_ehcvm_7day test_derivations \
  test_gh1575_cotedivoire1575-stratawiring_strata test_gh1575_togo1575-stratawiring_strata \
  test_gh1575_benin1575-stratawiring_strata test_gh812_ehcvm_strata test_sampling test_recall_record \
  test_recall_period test_build_transform_hash test_gh1578_derivation_duplicates test_ghanalss_8h_farmgate \
  test_food_kg_size_pool test_ehcvm_unit_size test_962_unit_sentinel test_gh323_benin_togo \
  test_togo_nonfood_expenditures test_ehcvm_unit_weights test_categorical_mapping_resolution \
  test_feature_attrs_carriers test_gh1556_ehcvm_shocks_2021 test_ehcvm_crop_production_condition; do
  [ -f tests/$t.py ] && TESTS="$TESTS tests/$t.py" || echo "missing tests/$t.py"
done
echo "TESTS: $TESTS"
runpy head pytest -m pytest $TESTS -n 6 --dist=loadfile -q -rfEs -p no:cacheprovider > $O/logs/jobB-pytest.log 2>&1
echo "pytest rc=$? $(date)"
runpy head pytest2 -m pytest tests/test_schema_consistency.py tests/test_sample.py -k "Togo or Benin or CotedIvoire" -n 6 --dist=loadfile -q -rfEs -p no:cacheprovider > $O/logs/jobB-pytest2.log 2>&1
echo "pytest2 rc=$? $(date)"
wait $RAWPID; echo "rawchecks rc=$? $(date)"
for side in prev head; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side:"; git -C $J/$side status --porcelain | head -20
done
echo "JOBB DONE $(date)"
