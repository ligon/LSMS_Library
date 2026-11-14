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
#SBATCH -J rt1697-tst
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/logs/job3-%j.out
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam/scripts/job_common.sh
setup_clones head
# a throwaway clone at HEAD for re-running the record generators
git clone -q --no-hardlinks $MIRROR $J/regen && git -C $J/regen checkout -q --detach $HEAD
( cd $J/regen && bin/savio_venv.sh mount ) >/dev/null 2>&1
echo "regen HEAD $(git -C $J/regen rev-parse HEAD)"
P=$J/priv-regen; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
( cd $J/regen && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp MPLCONFIGDIR=$P/mpl \
   LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/regen \
   bash -c "$PY -c 'import lsms_library,os;print(lsms_library.__file__)'; \
            $PY scripts/promote_recall_records.py --check; $PY scripts/promote_recall_records.py; \
            $PY scripts/promote_population_records.py --check; $PY scripts/promote_population_records.py" ) > $R/logs/job3-regen.log 2>&1
echo "regen git status (must be empty):" >> $R/logs/job3-regen.log
git -C $J/regen status --porcelain >> $R/logs/job3-regen.log
git -C $J/regen diff --stat >> $R/logs/job3-regen.log
git -C $J/regen diff | head -200 >> $R/logs/job3-regen.log
echo "regen done"

TESTS="tests/test_togo_2021_wave.py tests/test_benin_2021_wave.py tests/test_civ_2021_wave.py
 tests/test_gh1575_benin1575-stratawiring_strata.py tests/test_gh1575_cotedivoire1575-stratawiring_strata.py
 tests/test_gh1575_togo1575-stratawiring_strata.py tests/test_gh1575_guineabissau1575-stratawiring_strata.py
 tests/test_gh812_ehcvm_strata.py tests/test_ehcvm_7day.py tests/test_food_kg_size_pool.py tests/test_ehcvm_unit_size.py
 tests/test_derivations.py tests/test_sampling.py tests/test_recall_record.py tests/test_population.py
 tests/test_gh323_benin_togo.py tests/test_gh323_benin_cluster_features.py tests/test_gh323_togo_cluster_features.py
 tests/test_gh675_degraded_build.py tests/test_nonfood_expenditures_schema.py tests/test_togo_nonfood_expenditures.py
 tests/test_gh1093_declared_unit_consumers.py tests/test_gh1556_ehcvm_shocks_2021.py tests/test_shocks_extension_food_effects.py
 tests/test_ehcvm_crop_production_condition.py tests/test_ehcvm_condition_helpers.py tests/test_gh1437_mali_harvest_total.py
 tests/test_build_transform_hash.py tests/test_categorical_mapping_resolution.py tests/test_panel_ids_persistence.py
 tests/test_ehcvm_unit_weights.py tests/test_gh1452_older_rows.py tests/test_gh1454_served_unit_fingerprint.py
 tests/test_feature_attrs_carriers.py"
runpy head pytest -m pytest $TESTS -n 6 --dist=loadfile -q -rfEs -p no:cacheprovider > $R/logs/job3-pytest.log 2>&1
echo "pytest rc=$?"
runpy head pytest2 -m pytest tests/test_schema_consistency.py tests/test_sample.py -k "Togo or Benin or CotedIvoire" -n 6 --dist=loadfile -q -rfEs -p no:cacheprovider > $R/logs/job3-pytest2.log 2>&1
echo "pytest2 rc=$?"

# wait for job1's head frames (compare result marks completion), up to 90 min
for k in $(seq 1 90); do [ -f $R/results/job1_compare.json ] && break; sleep 60; done
runpy head checks $SCR/checks2021.py $J/head $R/frames/job1/head $R/results/job3_checks.json > $R/logs/job3-checks.log 2>&1
echo "checks rc=$?"
echo "in-tree parquets head: $(find $J/head/lsms_library/countries -name '*.parquet' | wc -l)"
git -C $J/head status --porcelain | head
echo JOB3 DONE
