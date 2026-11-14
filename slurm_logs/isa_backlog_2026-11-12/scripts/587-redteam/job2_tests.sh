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
#SBATCH -J rt587-tests
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-redteam/logs/job2-%j.out
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-redteam
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-redteam
SCR=$OUT/scripts
LOG=$PRIV/logs
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
CAND=e5940eaa944aeb0555b06f85017e791589f6eceb
J=/local/job$SLURM_JOB_ID
mkdir -p $J $LOG
[ "$(hostname -s)" != "n0035" ] || { echo "refusing to run on n0035"; exit 9; }
for v in $(env | sed -n 's/^\(LSMS_[A-Z_]*\)=.*/\1/p'); do unset $v; done
unset PYTHONPATH XDG_CACHE_HOME MPLCONFIGDIR
export TMPDIR=$J/tmp; mkdir -p $TMPDIR
echo "host $(hostname) job $SLURM_JOB_ID"

for side in base cand; do
  sha=$BASE; [ $side = cand ] && sha=$CAND
  git clone -q --no-hardlinks $MIRROR $J/$side || exit 2
  git -C $J/$side checkout -q --detach $sha || exit 2
  [ "$(git -C $J/$side rev-parse HEAD)" = "$sha" ] || exit 2
  echo "$side HEAD $(git -C $J/$side rev-parse HEAD)"
done
# the mount verifies an import: give it a node-local private cache, never ~/.cache
M=$J/mnt; mkdir -p $M/xdg $M/data $M/mpl
( cd $J/cand && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
( cd $J/base && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python

DATA_FILES="tests/test_gh587_nigeria_pp_community_serving.py tests/test_gh587_nigeria_pp_price_reports.py
 tests/test_nigeria_community_units.py tests/test_gh637_nigeria_w3_obs.py tests/test_gh637_nigeria_w1w2_variety.py
 tests/test_nigeria_cluster_identity.py tests/test_gh1047_nigeria_unit_codes.py tests/test_gh1287_nigeria_four_declarations.py"
CONF_FILES="tests/test_derivations.py tests/test_schema_consistency.py tests/test_categorical_mapping_resolution.py
 tests/test_build_transform_hash.py tests/test_feature_canonical_index.py tests/test_coverage_matrix.py
 tests/test_962_unit_sentinel.py tests/test_1234_map_index_unit_sentinel.py tests/test_gh1537_partial_curation.py
 tests/test_community_prices_variety.py"

runtests() {
  side=$1
  P=$J/priv-$side; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  cd $J/$side
  sel_d=""; for f in $DATA_FILES; do [ -f $f ] && sel_d="$sel_d $f"; done
  sel_c=""; for f in $CONF_FILES; do [ -f $f ] && sel_c="$sel_c $f"; done
  E="env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp MPLCONFIGDIR=$P/mpl
     LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$side OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
     MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1"
  $E $PY -c "
import sys; sys.path.insert(0, '$SCR')
from common import assert_env; assert_env('$J/$side')" > $LOG/tests-$side-env.log 2>&1 || { echo "env $side failed"; return; }
  $E $PY -m pytest -n 3 --dist=loadfile -rs -q -p no:cacheprovider $sel_d \
      > $LOG/tests-$side-data.log 2>&1
  echo "data tests $side rc=$?"
  $E $PY -m pytest -n 3 --dist=loadfile -rs -q -p no:cacheprovider -m "not requires_s3" $sel_c \
      > $LOG/tests-$side-conf.log 2>&1
  echo "conf tests $side rc=$?"
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  git -C $J/$side status --porcelain | head -10
}
runtests cand &
runtests base &
wait
echo JOB2 DONE
