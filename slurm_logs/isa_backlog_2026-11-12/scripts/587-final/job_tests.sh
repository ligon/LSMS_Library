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
#SBATCH -J f587-tests
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final/logs/tests-%j.out
# Stage 1 (fail-fast): candidate synthetic/config tests.  Stage 2: the targeted
# selection on base and candidate in parallel, each in its own private root.
set -uo pipefail
J=/local/job$SLURM_JOB_ID; mkdir -p $J
export TMPDIR=$J/tmp; mkdir -p $TMPDIR
[ "$(hostname -s)" != "n0035" ] || exit 9
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-final
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final
LOG=$PRIV/logs; TOUT=$PRIV/tests; mkdir -p $TOUT
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
CAND=${CAND:?}; RUN=${RUN:?}; STAGE2=${STAGE2:-1}
for v in $(env | sed -n 's/^\(LSMS_[A-Z_]*\)=.*/\1/p'); do unset $v; done
unset PYTHONPATH XDG_CACHE_HOME MPLCONFIGDIR
for arm in base cand; do
  case $arm in base) SHA=$BASE;; cand) SHA=$CAND;; esac
  git clone -q --no-hardlinks $MIRROR $J/$arm || exit 2
  git -C $J/$arm checkout -q --detach $SHA || exit 2
  [ "$(git -C $J/$arm rev-parse HEAD)" = "$SHA" ] || exit 2
done
M=$J/mnt; mkdir -p $M/xdg $M/data $M/mpl $M/tmp
( cd $J/cand && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl TMPDIR=$M/tmp LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
envrun () {   # envrun ARM CMD...
  arm=$1; shift
  P=$J/priv-$arm; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $J/$arm && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp MPLCONFIGDIR=$P/mpl \
     LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$arm OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
     LSMS_BUILD_WORKERS=1 "$@" )
}
for arm in base cand; do
  envrun $arm $PY -c "
import os, lsms_library, lsms_library.paths as p
top=os.path.realpath('$J/$arm'); f=os.path.realpath(lsms_library.__file__)
assert f.startswith(top+os.sep), f
d=os.path.realpath(str(p.data_root())); assert d.startswith(os.path.realpath('$J/priv-$arm')+os.sep), d
print('ENV OK', '$arm', f, d)" || exit 4
done
# Stage 1: fail fast on the candidate's synthetic / config tests
envrun cand $PY -m pytest -q -p no:cacheprovider -n 4 --dist=loadfile \
  tests/test_gh587_nigeria_pp_community_serving.py tests/test_gh587_nigeria_pp_price_reports.py \
  tests/test_derivations.py tests/test_build_transform_hash.py tests/test_categorical_mapping_resolution.py \
  tests/test_no_build_input_in_contents_org.py tests/test_gh1537_aggregate_diagnosis.py \
  tests/test_gh1537_partial_curation.py -m "not requires_s3" > $TOUT/$RUN-stage1-cand.log 2>&1
rc=$?; echo "stage1 rc=$rc"; tail -3 $TOUT/$RUN-stage1-cand.log
[ $rc -eq 0 ] || exit 5
[ "$STAGE2" = 1 ] || { echo DONE-STAGE1; exit 0; }
sel () {   # the targeted selection that exists in checkout $1
  ( cd $J/$1 && ls tests/test_gh587*.py tests/test_*nigeria*.py tests/test_community_prices_variety.py \
      tests/test_derivations.py tests/test_build_transform_hash.py tests/test_no_build_input_in_contents_org.py \
      tests/test_categorical_mapping_resolution.py tests/test_gh1537*.py tests/test_gh1578*.py 2>/dev/null | sort -u )
}
sel base > $TOUT/$RUN-selection-base.txt; sel cand > $TOUT/$RUN-selection-cand.txt
envrun base $PY -m pytest -q -p no:cacheprovider -n 4 --dist=loadfile $(cat $TOUT/$RUN-selection-base.txt) > $TOUT/$RUN-stage2-base.log 2>&1 &
envrun cand $PY -m pytest -q -p no:cacheprovider -n 4 --dist=loadfile $(cat $TOUT/$RUN-selection-cand.txt) > $TOUT/$RUN-stage2-cand.log 2>&1 &
wait
for arm in base cand; do echo "== $arm"; tail -3 $TOUT/$RUN-stage2-$arm.log; done
for arm in base cand; do
  echo "$arm in-tree parquets: $(find $J/$arm/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $J/$arm status --porcelain | head -5
done
echo DONE
