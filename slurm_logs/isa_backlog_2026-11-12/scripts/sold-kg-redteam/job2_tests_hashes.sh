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
#SBATCH -J rt1560-tests
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam/logs/job2-%j.out
set -uo pipefail
J=/local/job$SLURM_JOB_ID
mkdir -p $J/tmp
export TMPDIR=$J/tmp
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam
SCR=$OUT/scripts
RES=$OUT/results
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
CAND=43632ff5b1f21a14845d4cd01766d01b9649459d
mkdir -p $RES $OUT/logs $OUT/frames2
P0=$J/priv-mount; mkdir -p $P0/data $P0/xdg $P0/tmp $P0/mpl
export LSMS_DATA_DIR=$P0/data XDG_CACHE_HOME=$P0/xdg MPLCONFIGDIR=$P0/mpl LSMS_ISSUES_LOG=$P0/issues.log
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_QUANTITY_STRICT LSMS_GRAIN_STRICT LSMS_READ_STRICT
echo "host $(hostname) job $SLURM_JOB_ID start $(date -Is)"

clone() {  # clone <dir> <sha>
  git clone -q --shared --no-checkout $MIRROR $1 || exit 2
  git -C $1 checkout -q --detach $2 || exit 2
  echo "$1 HEAD $(git -C $1 rev-parse HEAD)"
}
clone $J/cand $CAND
clone $J/base $BASE
clone $J/base_qa $BASE
clone $J/base_qa_noop $BASE
clone $J/base_ctrl $BASE
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
export PATH=$J/venv_img/bin:$PATH
$PY -m py_compile $SCR/*.py || exit 4

# Variants (private clones only)
git -C $J/cand show $CAND:lsms_library/quantity_audit.py > $J/base_qa/lsms_library/quantity_audit.py
git -C $J/cand show $CAND:lsms_library/quantity_audit.py > $J/base_qa_noop/lsms_library/quantity_audit.py
$PY $SCR/noop_patch.py $J/base_qa_noop/lsms_library/quantity_audit.py \
    check_quantities audit_quantities audit_near_zero _audit_crop_sales _cell_references _reports_for _axis _group_quantiles || exit 6
$PY $SCR/noop_patch.py $J/base_ctrl/lsms_library/local_tools.py df_data_grabber || exit 6
for v in base_qa base_qa_noop base_ctrl; do echo "$v diff:"; git -C $J/$v diff --stat; done

envp() {  # envp <root-name> <clone> cmd...
  local P=$J/priv-$1; local C=$2; shift 2; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $C && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$C "$@" )
}
touch $OUT/logs/job2.stage0.ok

# Stage 1: hash census (config/sidecar reads only)
for v in base cand base_qa base_qa_noop base_ctrl; do
  envp hash-$v $J/$v timeout 1500 $PY $SCR/hashes.py $J/$v $v $RES/hashes_$v.json > $OUT/logs/hashes-$v.log 2>&1 &
done
wait
$PY $SCR/hashdiff.py $RES/hashdiff.json $RES/hashes_base.json $RES/hashes_cand.json $RES/hashes_base_qa.json \
    $RES/hashes_base_qa_noop.json $RES/hashes_base_ctrl.json > $OUT/logs/hashdiff.log 2>&1
echo "hashdiff rc=$?"; cat $OUT/logs/hashdiff.log

# Stage 2: warm Niger/Mali crop_production in each test root, then pytest
envp pytest-cand $J/cand timeout 2400 $PY $SCR/build.py $J/cand cand $OUT/frames2 Niger:crop_production Mali:crop_production > $OUT/logs/warm-pytest-cand.log 2>&1 &
envp pytest-base $J/base timeout 2400 $PY $SCR/build.py $J/base base $OUT/frames2 Niger:crop_production Mali:crop_production > $OUT/logs/warm-pytest-base.log 2>&1 &
wait

TESTS="tests/test_niger_crop_sold_kg.py tests/test_mali_crop_sold_kg.py tests/test_gh1462_niger_dry_season.py
tests/test_gh824_mali_ehcvm_crop_production.py tests/test_niger_sold_unit.py tests/test_niger_crop_production_observations.py
tests/test_niger_crop_survey_kg.py tests/test_mali_crop_survey_kg.py tests/test_crop_survey_kg.py
tests/test_gh1457_niger_multi_plot_sales.py tests/test_gh1457_mali_multi_plot_sales.py tests/test_gh1528_sold_condition.py
tests/test_gh1437_mali_harvest_total.py tests/test_ehcvm_crop_production_condition.py tests/test_niger_crop_production_condition.py
tests/test_niger_value_sentinels.py tests/test_niger_u_sentinel.py tests/test_gh1569_sold_diagnostics.py
tests/test_quantity_screen.py tests/test_gh1493_near_zero_screen.py tests/test_gh1482_1485_quantity_diagnostics.py
tests/test_derivations.py tests/test_schema_consistency.py tests/test_build_transform_hash.py
tests/test_feature_attrs_carriers.py tests/test_feature_canonical_index.py tests/test_gh1469_farm_level_occurrence.py
tests/test_gh1460_crop_level.py tests/test_gh824_mali_unit_sold.py tests/test_gh824_mali_field_sold_factors.py
tests/test_gh1476_mali_tree_farm_level.py tests/test_gh1267_mali_tree_quantity.py tests/test_mali_tree_sale_window_followup.py
tests/test_niger_ci_oracle_20261110.py tests/test_gh1494_mali_2021_namespace.py tests/test_mali_namespace_oracle.py
tests/test_null_index_guard.py tests/test_failure_is_not_absence.py tests/test_gh1535_crop_month_carriers.py
tests/test_niger_make_integration.py tests/test_niger_five_compiler_declarations.py tests/test_gh1287_mali_actual_declarations.py"

runpy() {
  local side=$1; local C=$J/$side; local LIST=""
  for t in $TESTS; do [ -f $C/$t ] && LIST="$LIST $t"; done
  echo "$side: $(echo $LIST | wc -w) test files"
  envp pytest-$side $C timeout 6000 $PY -m pytest -n 4 --dist=loadfile -q -rfEs -p no:cacheprovider \
      --junitxml=$RES/pytest_$side.xml $LIST > $RES/pytest_$side.txt 2>&1
  echo "pytest $side rc=$?"
}
runpy cand &
runpy base &
wait
tail -5 $RES/pytest_cand.txt $RES/pytest_base.txt

for side in base cand; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side: $(git -C $J/$side status --porcelain | grep -v '^?? .venv$' | wc -l) changed"
done
echo "JOB2 DONE $(date -Is)"
