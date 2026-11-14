#!/bin/bash
# GH #1509 final2 (framework fix agent, Claude Opus 5.5).  usage: job.sh MODE
#   syn      synthetic/data-free subset of tests at CAND (no microdata reads)
#   measure  three arms (base/head/cand) in parallel, cold, private scratch
#            roots; then compare, warm-root Uganda nutrition, probe census,
#            data-gated pytest on the cand and base roots
#   full     full non-S3 suite at BASE and CAND, data-free
#   final    census at FINAL vs CAND, data-gated pins at FINAL on the cand root
# Private roots are exported BEFORE the venv mount; nothing runs on n0035.
set -uo pipefail
MODE=$1
F=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/framework-1509-final2
W=$F/work; S=$F/scripts; O=$F/out; L=$F/logs
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE_SHA=e5fdd2fa831c4d03a247d264be7f339da459e6b0
HEAD_SHA=0966d62ef3dc3ee60ba9a17f6ab90dd26043c15c
CAND_SHA=${CAND_SHA:-30523f946ae760863432241018854acc9dc53b2c}
FINAL_SHA=${FINAL_SHA:-}
J=/local/job$SLURM_JOB_ID/f2
case "$(hostname)" in n0035*) echo "refusing to run on n0035"; exit 4;; esac
mkdir -p $J/tmp $W/private/job-$SLURM_JOB_ID/{xdg,mpl}
# Job-level private env FIRST (before any mount or python).
export TMPDIR=$J/tmp XDG_CACHE_HOME=$W/private/job-$SLURM_JOB_ID/xdg
export MPLCONFIGDIR=$W/private/job-$SLURM_JOB_ID/mpl LSMS_ISSUES_LOG=$W/private/job-$SLURM_JOB_ID/issues.log
export LSMS_DATA_DIR=$W/roots/mount-$SLURM_JOB_ID
mkdir -p $LSMS_DATA_DIR
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_COUNTRIES_ROOT LSMS_NO_CACHE LSMS_SKIP_AUTH LSMS_ALLOW_INTREE_SOURCES || true
echo "host $(hostname) job $SLURM_JOB_ID mode $MODE start $(date -Is) cpus ${SLURM_CPUS_ON_NODE:-?}"
uptime

clone() {  # clone <dir> <sha>
  local d=$1 sha=$2
  [ "${#sha}" -eq 40 ] || { echo "need a FULL sha"; exit 2; }
  git clone -q --no-checkout $MIRROR $d && git -C $d checkout -q --detach $sha || { echo CLONE FAIL $d; exit 1; }
  [ "$(git -C $d rev-parse HEAD)" = "$sha" ] || { echo SHA FAIL $d; exit 1; }
}
mount_once() {  # mount_once <clone>
  bash $1/bin/savio_venv.sh mount || { echo MOUNT FAIL $1; exit 1; }
}
VB=/local/job$SLURM_JOB_ID/venv_img/bin
PY=$VB/python
armenv() {  # armenv <arm> <sha> <clone> <root>
  export ARM=$1 WANT_SHA=$2 CHECKOUT=$3 PRIVATE_ROOT=$4
  export LSMS_DATA_DIR=$PRIVATE_ROOT
  export XDG_CACHE_HOME=$W/private/$1-$SLURM_JOB_ID/xdg TMPDIR=$J/tmp-$1
  export MPLCONFIGDIR=$W/private/$1-$SLURM_JOB_ID/mpl LSMS_ISSUES_LOG=$W/private/$1-$SLURM_JOB_ID/issues.log
  export PYTHONPATH=$CHECKOUT PATH=$VB:$PATH
  mkdir -p $PRIVATE_ROOT $XDG_CACHE_HOME $TMPDIR $MPLCONFIGDIR
}
DATA_TESTS="tests/test_currency_denominated_units.py tests/test_962_unit_sentinel.py
 tests/test_ghanalss_nutrition.py tests/test_gh1042_missing_unit_spellings.py
 tests/test_food_kg_inference.py tests/test_food_kg_size_pool.py
 tests/test_gh1487_kg_subset_is_a_slice.py tests/test_feature_attrs_carriers.py
 tests/test_food_prices_units_kwarg.py tests/test_burkina_faso_2014_passages.py
 tests/test_gh1509_1068_1129_food_unit_evidence.py tests/test_gh1509_nonpurchase_fallback.py
 tests/test_gh1509_unit_nonpurchase_rung.py tests/test_gh1509_build_time_food_api_dependency.py
 tests/test_unpriceable_price_drop.py tests/test_build_transform_hash.py
 tests/test_uganda_invariance.py tests/test_gh1171_ethiopia_nutrition_make.py
 tests/test_gh1062_uganda_sibling_writes.py tests/test_gh771_offline_fct_refresh.py
 tests/test_gh1454_served_unit_fingerprint.py tests/test_metric_decimal_comma.py
 tests/test_gh1509_data_pins.py"
existing() { local out=(); for p in $DATA_TESTS; do [ -e "$p" ] && out+=("$p"); done; echo "${out[@]}"; }
COUNTRIES="GhanaLSS GhanaSPS Uganda Ethiopia Cambodia Serbia Burkina_Faso Malawi Niger Panama EthiopiaRHS"
export SUBSET_COUNTRIES="GhanaLSS Uganda GhanaSPS" NUTRITION_COUNTRIES="GhanaLSS Uganda Ethiopia"

case $MODE in
syn)
  clone $J/cand $CAND_SHA; mount_once $J/cand
  armenv cand $CAND_SHA $J/cand $W/roots/syn-$SLURM_JOB_ID; cd $CHECKOUT
  $PY $S/guard.py
  timeout 2400 $PY -m pytest -p no:cacheprovider -n 6 --dist=loadfile -q -rfE -m "not requires_s3" \
    tests/test_gh1509_1068_1129_food_unit_evidence.py tests/test_gh1509_nonpurchase_fallback.py \
    tests/test_gh1509_unit_nonpurchase_rung.py tests/test_gh1509_build_time_food_api_dependency.py \
    tests/test_food_kg_inference.py tests/test_food_kg_size_pool.py tests/test_build_transform_hash.py \
    tests/test_gh1062_uganda_sibling_writes.py tests/test_gh771_offline_fct_refresh.py \
    tests/test_gh1171_ethiopia_nutrition_make.py tests/test_gh1454_served_unit_fingerprint.py \
    tests/test_metric_decimal_comma.py tests/test_currency_denominated_units.py \
    tests/test_food_prices_units_kwarg.py tests/test_962_unit_sentinel.py \
    tests/test_feature_attrs_carriers.py tests/test_unpriceable_price_drop.py \
    > $O/pytest-syn-cand.log 2>&1 < /dev/null
  tail -15 $O/pytest-syn-cand.log ;;
measure)
  clone $J/base $BASE_SHA; clone $J/head $HEAD_SHA; clone $J/cand $CAND_SHA
  mount_once $J/base; mount_once $J/head; mount_once $J/cand
  for arm in base head cand; do
    case $arm in base) sha=$BASE_SHA;; head) sha=$HEAD_SHA;; cand) sha=$CAND_SHA;; esac
    (
      armenv $arm $sha $J/$arm $W/roots/$arm; cd $CHECKOUT
      $PY $S/census.py $O/census-$arm.json > $L/M_census_$arm.log 2>&1
      $PY $S/measure.py $O/$arm $COUNTRIES > $L/M_measure_$arm.log 2>&1
      touch $O/$arm/DONE
    ) &
  done
  wait; date
  ( armenv cand $CAND_SHA $J/cand $W/roots/cand; cd $CHECKOUT
    $PY $S/compare.py $O > $L/M_compare.log 2>&1
    $PY $S/census_diff.py $O/census-base.json $O/census-cand.json $O/census-diff-base-cand.json > $L/M_cdiff_bc.log 2>&1
    $PY $S/census_diff.py $O/census-head.json $O/census-cand.json $O/census-diff-head-cand.json > $L/M_cdiff_hc.log 2>&1
    $PY $S/census_diff.py $O/census-base.json $O/census-head.json $O/census-diff-base-head.json > $L/M_cdiff_bh.log 2>&1 )
  date
  # Warm roots: copies of the BASE root (its Uganda nutrition built by base code).
  for arm in head cand; do
    rm -rf $W/roots/warm-$arm; mkdir -p $W/roots/warm-$arm
    cp -al $W/roots/base/dvc-cache $W/roots/warm-$arm/dvc-cache
    for d in $W/roots/base/*; do b=$(basename $d); [ "$b" = dvc-cache ] && continue; cp -a $d $W/roots/warm-$arm/$b; done
  done
  ( armenv head $HEAD_SHA $J/head $W/roots/warm-head; cd $CHECKOUT; $PY $S/warm.py $O/warm-head.json > $L/M_warm_head.log 2>&1 ) &
  ( armenv cand $CAND_SHA $J/cand $W/roots/warm-cand; cd $CHECKOUT; $PY $S/warm.py $O/warm-cand.json > $L/M_warm_cand.log 2>&1 ) &
  # The no-op probe: one statement in food_kg_factors, in a private clone.
  ( clone $J/probe $CAND_SHA
    sed -i 's/^    seeded = _seeded_kg_factors(df, volume_as_mass=volume_as_mass, unit_kg=unit_kg)$/    _gh1509_probe = 0\n&/' $J/probe/lsms_library/transformations.py
    git -C $J/probe diff --stat > $L/M_probe_diff.txt; git -C $J/probe diff >> $L/M_probe_diff.txt
    armenv probe $CAND_SHA $J/probe $W/roots/probe-census; cd $CHECKOUT
    $PY $S/census.py $O/census-probe.json > $L/M_census_probe.log 2>&1
    $PY $S/census_diff.py $O/census-cand.json $O/census-probe.json $O/census-diff-cand-probe.json > $L/M_cdiff_probe.log 2>&1 ) &
  wait; date
  ( armenv cand $CAND_SHA $J/cand $W/roots/cand; cd $CHECKOUT
    timeout 4500 $PY -m pytest -p no:cacheprovider -n 3 --dist=loadfile -q -rfEs $(existing) \
      > $O/pytest-data-cand.log 2>&1 < /dev/null; tail -5 $O/pytest-data-cand.log ) &
  ( armenv base $BASE_SHA $J/base $W/roots/base; cd $CHECKOUT
    timeout 4500 $PY -m pytest -p no:cacheprovider -n 3 --dist=loadfile -q -rfEs $(existing) \
      > $O/pytest-data-base.log 2>&1 < /dev/null; tail -5 $O/pytest-data-base.log ) &
  wait ;;
full)
  clone $J/base $BASE_SHA; clone $J/cand $CAND_SHA; mount_once $J/base; mount_once $J/cand
  for arm in base cand; do
    case $arm in base) sha=$BASE_SHA;; cand) sha=$CAND_SHA;; esac
    ( armenv $arm $sha $J/$arm $W/roots/full-$arm-$SLURM_JOB_ID; cd $CHECKOUT
      $PY $S/guard.py
      timeout 9000 $PY -m pytest -p no:cacheprovider -n 4 --dist=loadfile -q -rfE -m "not requires_s3" tests \
        > $O/pytest-full-$arm.log 2>&1 < /dev/null; tail -5 $O/pytest-full-$arm.log ) &
  done
  wait ;;
final)
  clone $J/final $FINAL_SHA; mount_once $J/final
  ( armenv cand $FINAL_SHA $J/final $W/roots/final-census-$SLURM_JOB_ID; cd $CHECKOUT
    $PY $S/census.py $O/census-final.json > $L/X_census_final.log 2>&1
    $PY $S/census_diff.py $O/census-cand.json $O/census-final.json $O/census-diff-cand-final.json > $L/X_cdiff.log 2>&1 )
  ( armenv cand $FINAL_SHA $J/final $W/roots/cand; cd $CHECKOUT
    timeout 4500 $PY -m pytest -p no:cacheprovider -n 3 --dist=loadfile -q -rfEs $(existing) \
      > $O/pytest-data-final.log 2>&1 < /dev/null; tail -5 $O/pytest-data-final.log ) ;;
esac
echo "done $(date -Is)"
