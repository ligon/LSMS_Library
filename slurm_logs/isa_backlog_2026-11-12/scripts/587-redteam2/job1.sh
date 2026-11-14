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
#SBATCH -J rt587b
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-redteam2/logs/job1-%j.out
# GH #587 delta re-review: base / prev (e5940eaa9) / cand (9e74f2cf9), cold, private roots.
set -uo pipefail
J=/local/job$SLURM_JOB_ID
mkdir -p $J
export TMPDIR=$J/tmp; mkdir -p $TMPDIR
[ "$(hostname -s)" != "n0035" ] || { echo "refusing to run on n0035"; exit 9; }
# private roots FIRST (before clone / mount / any python)
for v in $(compgen -e | grep '^LSMS_'); do unset "$v"; done
unset PYTHONPATH
JP=$J/priv-job; mkdir -p $JP/data $JP/xdg $JP/mpl
export LSMS_DATA_DIR=$JP/data XDG_CACHE_HOME=$JP/xdg MPLCONFIGDIR=$JP/mpl LSMS_ISSUES_LOG=$JP/issues.log
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-redteam2
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-redteam2
SCR=$OUT/scripts
LOG=$PRIV/logs; FR=$PRIV/frames; RES=$PRIV/results
mkdir -p $LOG $FR $RES
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
PREV=e5940eaa944aeb0555b06f85017e791589f6eceb
CAND=9e74f2cf9b5961d2c6873f791967166e8eb40dad
echo "host $(hostname) job $SLURM_JOB_ID $(date -Is)"
env | grep -E '^(LSMS_|XDG_|TMPDIR|PYTHONPATH|OPENBLAS|OMP_|MKL_|MPLCONFIGDIR)' | sort

for side in base prev cand; do
  case $side in base) sha=$BASE;; prev) sha=$PREV;; cand) sha=$CAND;; esac
  git clone -q --no-hardlinks $MIRROR $J/$side || exit 2
  git -C $J/$side checkout -q --detach $sha || exit 2
  got=$(git -C $J/$side rev-parse HEAD); echo "$side HEAD $got"
  [ "$got" = "$sha" ] || exit 2
done
for side in cand base prev; do
  ( cd $J/$side && bin/savio_venv.sh mount ) || { echo "mount $side failed"; exit 3; }
done
PY=$J/venv_img/bin/python
[ -x $PY ] || { echo "no python at $PY"; exit 3; }
$PY -m py_compile $SCR/common.py $SCR/build.py $SCR/compare.py $SCR/probe.py $SCR/mutate.py \
  || { echo "py_compile failed"; exit 5; }

renv() {  # side -> env prefix with that side's private root
  local P=$J/priv-$1
  mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  echo env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$2 \
      OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
}

for side in base prev cand; do
  ( cd $J/$side && $(renv $side $J/$side) $PY -c "
import sys; sys.path.insert(0, '$SCR')
from common import assert_env; assert_env('$J/$side')
import importlib; D = importlib.import_module('lsms_library.derivations')
print('registry keys', len(D.derivation_records('Nigeria')))
" ) || { echo "failfast $side failed"; exit 4; }
done

runside() {
  local side=$1
  ( cd $J/$side && $(renv $side $J/$side) $PY $SCR/build.py $J/$side $side $FR $RES > $LOG/build-$side.log 2>&1 )
  echo "build $side rc=$? $(date -Is)"
}
runside base &
runside prev &
runside cand &
wait

( cd $J/cand && $(renv cmp $J/cand) $PY $SCR/compare.py $FR $RES > $LOG/compare.log 2>&1 ); echo "compare rc=$? $(date -Is)"
( cd $J/cand && $(renv cand $J/cand) $PY $SCR/probe.py $J/cand $FR $RES > $LOG/probe.log 2>&1 ); echo "probe rc=$? $(date -Is)"

# ---- tests on the candidate (warm candidate root), loadfile, output to files
DATA_FILES="tests/test_gh587_nigeria_pp_community_serving.py tests/test_gh587_nigeria_pp_price_reports.py
 tests/test_nigeria_community_units.py tests/test_gh637_nigeria_w3_obs.py tests/test_gh637_nigeria_w1w2_variety.py
 tests/test_nigeria_cluster_identity.py tests/test_gh1047_nigeria_unit_codes.py tests/test_gh1287_nigeria_four_declarations.py"
CONF_FILES="tests/test_derivations.py tests/test_schema_consistency.py tests/test_categorical_mapping_resolution.py
 tests/test_build_transform_hash.py tests/test_feature_canonical_index.py tests/test_coverage_matrix.py
 tests/test_962_unit_sentinel.py tests/test_1234_map_index_unit_sentinel.py tests/test_gh1537_partial_curation.py
 tests/test_gh1537_aggregate_diagnosis.py tests/test_community_prices_variety.py"
cd $J/cand
sel_d=""; for f in $DATA_FILES; do [ -f $f ] && sel_d="$sel_d $f"; done
sel_c=""; for f in $CONF_FILES; do [ -f $f ] && sel_c="$sel_c $f"; done
$(renv cand $J/cand) $PY -m pytest -n 4 --dist=loadfile -rs -q -p no:cacheprovider $sel_d > $LOG/tests-cand-data.log 2>&1
echo "data tests rc=$? $(date -Is)"
$(renv tconf $J/cand) $PY -m pytest -n 4 --dist=loadfile -rs -q -p no:cacheprovider -m "not requires_s3" $sel_c > $LOG/tests-cand-conf.log 2>&1
echo "conf tests rc=$? $(date -Is)"

# ---- strictness of the moved GH #1537 pins: mutate a throwaway copy, expect red
MUT_TESTS="tests/test_gh1537_aggregate_diagnosis.py tests/test_gh1537_partial_curation.py
 tests/test_gh587_nigeria_pp_community_serving.py::test_semovita_is_served_and_the_w2_sentinel_held"
for mut in none extra_cell drop_cell agg other_cell drop_row; do
  D=$J/mut-$mut
  git -C $J/cand worktree add -q --detach $D $CAND > /dev/null 2>&1 || { echo "worktree $mut failed"; continue; }
  [ "$(git -C $D rev-parse HEAD)" = "$CAND" ] || { echo "worktree $mut wrong HEAD"; continue; }
  ( cd $D && $(renv mut-$mut $D) $PY $SCR/mutate.py $D $mut ) > $LOG/mut-$mut.log 2>&1
  git -C $D diff --stat >> $LOG/mut-$mut.log 2>&1
  ( cd $D && $(renv mut-$mut $D) $PY -m pytest -q -p no:cacheprovider $MUT_TESTS ) >> $LOG/mut-$mut.log 2>&1
  echo "mutation $mut pytest rc=$? $(tail -1 $LOG/mut-$mut.log)"
done

for side in base prev cand; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side:"; git -C $J/$side status --porcelain | head -20
done
du -sh $J/priv-* 2>/dev/null
echo "JOB1 DONE $(date -Is)"
