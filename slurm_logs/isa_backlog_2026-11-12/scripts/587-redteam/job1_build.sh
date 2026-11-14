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
#SBATCH -J rt587-build
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-redteam/logs/job1-%j.out
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-redteam
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-redteam
SCR=$OUT/scripts
LOG=$PRIV/logs
FR=$PRIV/frames
RES=$PRIV/results
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
CAND=e5940eaa944aeb0555b06f85017e791589f6eceb
J=/local/job$SLURM_JOB_ID
mkdir -p $J $FR $RES $LOG
[ "$(hostname -s)" != "n0035" ] || { echo "refusing to run on n0035"; exit 9; }
# scrub inherited env
for v in $(env | sed -n 's/^\(LSMS_[A-Z_]*\)=.*/\1/p'); do unset $v; done
unset PYTHONPATH XDG_CACHE_HOME MPLCONFIGDIR
export TMPDIR=$J/tmp; mkdir -p $TMPDIR
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
echo "host $(hostname) job $SLURM_JOB_ID"; env | grep -E '^(LSMS_|XDG_|TMPDIR|PYTHONPATH|OPENBLAS|OMP_|MKL_)' | sort

for side in base cand; do
  sha=$BASE; [ $side = cand ] && sha=$CAND
  git clone -q --no-hardlinks $MIRROR $J/$side || exit 2
  git -C $J/$side checkout -q --detach $sha || exit 2
  echo "$side HEAD $(git -C $J/$side rev-parse HEAD)"
  [ "$(git -C $J/$side rev-parse HEAD)" = "$sha" ] || exit 2
done
# the mount verifies an import: give it a node-local private cache, never ~/.cache
M=$J/mnt; mkdir -p $M/xdg $M/data $M/mpl
( cd $J/cand && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
( cd $J/base && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python

renv() {  # side -> env command prefix
  local side=$1 P=$J/priv-$1
  mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  echo env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$side \
      OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
}

# fail-fast stage: import identity + private root + registry load
for side in base cand; do
  ( cd $J/$side && $(renv $side) $PY -c "
import sys; sys.path.insert(0, '$SCR')
from common import assert_env; assert_env('$J/$side')
import importlib; D = importlib.import_module('lsms_library.derivations')
print('registry keys', len(D.derivation_records('Nigeria')))
" ) || { echo "failfast $side failed"; exit 4; }
done

runside() {
  side=$1
  ( cd $J/$side && $(renv $side) $PY $SCR/build.py $J/$side $side $FR $RES > $LOG/build-$side.log 2>&1 )
  echo "build $side rc=$?"
}
runside base &
runside cand &
wait

$PY $SCR/compare.py $FR $RES > $LOG/compare.log 2>&1; echo "compare rc=$?"

( cd $J/cand && $(renv cand) $PY $SCR/probe.py $J/cand $FR $RES > $LOG/probe.log 2>&1 ); echo "probe rc=$?"

for side in base cand; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side:"; git -C $J/$side status --porcelain | head -20
done
du -sh $J/priv-* 2>/dev/null
echo JOB1 DONE
