#!/bin/bash
# GH #1509 final2, last check at the PR head: census vs the measured SHA, the
# full non-S3 suite and the data-gated set (cand root), in parallel.
set -uo pipefail
F=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/framework-1509-final2
W=$F/work; S=$F/scripts; O=$F/out; L=$F/logs
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
FINAL_SHA=$1
J=/local/job$SLURM_JOB_ID/f2
case "$(hostname)" in n0035*) echo "refusing to run on n0035"; exit 4;; esac
mkdir -p $J/tmp $W/private/job-$SLURM_JOB_ID/{xdg,mpl}
export TMPDIR=$J/tmp XDG_CACHE_HOME=$W/private/job-$SLURM_JOB_ID/xdg
export MPLCONFIGDIR=$W/private/job-$SLURM_JOB_ID/mpl LSMS_ISSUES_LOG=$W/private/job-$SLURM_JOB_ID/issues.log
export LSMS_DATA_DIR=$W/roots/mount-$SLURM_JOB_ID; mkdir -p $LSMS_DATA_DIR
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_COUNTRIES_ROOT LSMS_NO_CACHE LSMS_SKIP_AUTH LSMS_ALLOW_INTREE_SOURCES || true
echo "host $(hostname) job $SLURM_JOB_ID final2 $FINAL_SHA start $(date -Is)"; uptime
[ "${#FINAL_SHA}" -eq 40 ] || exit 2
git clone -q --no-checkout $MIRROR $J/final && git -C $J/final checkout -q --detach $FINAL_SHA || exit 1
[ "$(git -C $J/final rev-parse HEAD)" = "$FINAL_SHA" ] || exit 1
bash $J/final/bin/savio_venv.sh mount || exit 1
VB=/local/job$SLURM_JOB_ID/venv_img/bin; PY=$VB/python
armenv() {
  export ARM=$1 WANT_SHA=$FINAL_SHA CHECKOUT=$J/final PRIVATE_ROOT=$2
  export LSMS_DATA_DIR=$PRIVATE_ROOT XDG_CACHE_HOME=$W/private/$1-$SLURM_JOB_ID/xdg TMPDIR=$J/tmp-$1
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
 tests/test_gh1509_data_pins.py tests/test_gh1454_code_prefixed_units.py"
( armenv census $W/roots/final2-census-$SLURM_JOB_ID; cd $CHECKOUT
  $PY $S/census.py $O/census-final2.json > $L/Y_census.log 2>&1
  $PY $S/census_diff.py $O/census-cand.json $O/census-final2.json $O/census-diff-cand-final2.json > $L/Y_cdiff.log 2>&1 ) &
( armenv full $W/roots/full-final2-$SLURM_JOB_ID; cd $CHECKOUT; $PY $S/guard.py
  timeout 9000 $PY -m pytest -p no:cacheprovider -n 4 --dist=loadfile -q -rfE -m "not requires_s3" tests \
    > $O/pytest-full-final2.log 2>&1 < /dev/null; tail -n 2 $O/pytest-full-final2.log ) &
( armenv cand $W/roots/cand; cd $CHECKOUT
  timeout 4500 $PY -m pytest -p no:cacheprovider -n 3 --dist=loadfile -q -rfEs $DATA_TESTS \
    > $O/pytest-data-final2.log 2>&1 < /dev/null; tail -n 2 $O/pytest-data-final2.log ) &
wait; echo "done $(date -Is)"
