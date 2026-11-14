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
#SBATCH -J rt1560-more
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam/logs/job5-%j.out
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
P0=$J/priv-mount; mkdir -p $P0/data $P0/xdg $P0/tmp $P0/mpl
export LSMS_DATA_DIR=$P0/data XDG_CACHE_HOME=$P0/xdg MPLCONFIGDIR=$P0/mpl LSMS_ISSUES_LOG=$P0/issues.log
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_QUANTITY_STRICT LSMS_GRAIN_STRICT LSMS_READ_STRICT
echo "host $(hostname) job $SLURM_JOB_ID start $(date -Is)"
for s in cand base; do
  sha=$BASE; [ $s = cand ] && sha=$CAND
  git clone -q --shared --no-checkout $MIRROR $J/$s || exit 2
  git -C $J/$s checkout -q --detach $sha || exit 2
  echo "$s HEAD $(git -C $J/$s rev-parse HEAD)"
done
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
export PATH=$J/venv_img/bin:$PATH
$PY -m py_compile $SCR/*.py || exit 4
envp() {
  local P=$J/priv-$1; local C=$2; shift 2; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $C && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$C "$@" )
}
envp extra $J/cand $PY $SCR/extra2.py $OUT/frames $RES/extra2.json > $OUT/logs/extra2.log 2>&1; echo "extra2 rc=$?"
envp extra $J/cand $PY $SCR/extra3.py $J/cand $RES/extra3.json > $OUT/logs/extra3.log 2>&1; echo "extra3 rc=$?"

TESTS="tests/test_962_unit_sentinel.py tests/test_coverage_matrix.py tests/test_ethiopia_shipped_factors.py
tests/test_gh1045_parse_datetime.py tests/test_gh1047_nigeria_unit_codes.py tests/test_gh1146_cache_coverage_guards.py
tests/test_gh1288_make_failed_recipe.py tests/test_gh1288_niger_livestock_source_literal.py tests/test_gh1433_nigeria_crop_lines.py
tests/test_gh1460_yield_area_t.py tests/test_gh1494_mali_eaci_namespace.py tests/test_gh824_burkina_unit_sold.py
tests/test_gh843_nigeria_household_crop_sales.py tests/test_gh844_nigeria_harvest_value.py
tests/test_gh866_stale_country_parquet_not_laundered.py tests/test_gh868_reduce_to_agreed_ledger.py tests/test_label_selection.py
tests/test_mali_plot_inputs_crop_marker.py tests/test_niger_2014_15_extension_key.py tests/test_niger_concatenator_refusal.py
tests/test_niger_food_label_drift.py tests/test_niger_producer_compiler_integration.py tests/test_nigeria_kg_factor.py
tests/test_nigeria_safe_harvest.py tests/test_nigeria_sold_unit.py tests/test_gh842_uganda_harvest_unit.py"
runpy() {
  local side=$1; local C=$J/$side; local LIST=""
  for t in $TESTS; do [ -f $C/$t ] && LIST="$LIST $t"; done
  echo "$side: $(echo $LIST | wc -w) test files"
  envp pytest-$side $C timeout 9000 $PY -m pytest -n 4 --dist=loadfile -q -rfEs -p no:cacheprovider \
      --junitxml=$RES/pytest2_$side.xml $LIST > $RES/pytest2_$side.txt 2>&1
  echo "pytest2 $side rc=$?"
}
runpy cand &
runpy base &
wait
for s in base cand; do
  echo "in-tree parquets $s: $(find $J/$s/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "in-tree raw sources $s: $(git -C $J/$s status --porcelain --ignored lsms_library/countries | grep -ciE '\.(dta|csv|sav)$')"
  echo "git status $s: $(git -C $J/$s status --porcelain | grep -v '^?? .venv$' | wc -l) changed"
done
echo "JOB5 DONE $(date -Is)"
