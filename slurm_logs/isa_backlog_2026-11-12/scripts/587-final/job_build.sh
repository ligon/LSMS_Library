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
#SBATCH -J f587-build
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final/logs/build-%j.out
# Cold builds: base / prev / cand, each in its own private root; then compare.
set -uo pipefail
J=/local/job$SLURM_JOB_ID; mkdir -p $J
export TMPDIR=$J/tmp; mkdir -p $TMPDIR
[ "$(hostname -s)" != "n0035" ] || exit 9
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-final
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final
SCR=$OUT/scripts; LOG=$PRIV/logs; RES=$PRIV/results/${RUN:?}; FR=$PRIV/frames/$RUN
mkdir -p $RES $FR
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
PREV=e5940eaa944aeb0555b06f85017e791589f6eceb
CAND=${CAND:?}
for v in $(env | sed -n 's/^\(LSMS_[A-Z_]*\)=.*/\1/p'); do unset $v; done
unset PYTHONPATH XDG_CACHE_HOME MPLCONFIGDIR
for arm in base prev cand; do
  case $arm in base) SHA=$BASE;; prev) SHA=$PREV;; cand) SHA=$CAND;; esac
  git clone -q --no-hardlinks $MIRROR $J/$arm || exit 2
  git -C $J/$arm checkout -q --detach $SHA || exit 2
  [ "$(git -C $J/$arm rev-parse HEAD)" = "$SHA" ] || exit 2
done
# private roots BEFORE the mount (its verify-import runs the library)
M=$J/mnt; mkdir -p $M/xdg $M/data $M/mpl $M/tmp
( cd $J/cand && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl TMPDIR=$M/tmp LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
TAB_OTHER="sample household_roster food_acquired crop_production plot_inputs plot_features"
run_arm () {
  arm=$1; shift
  P=$J/priv-$arm; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $J/$arm && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp MPLCONFIGDIR=$P/mpl \
     LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$arm OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
     LSMS_BUILD_WORKERS=1 $PY $SCR/build.py $J/$arm $FR $RES $arm "$@" > $LOG/$RUN-build-$arm.log 2>&1 )
  echo "build $arm rc=$?"
}
run_arm base $TAB_OTHER &
run_arm prev community_prices &
run_arm cand community_prices $TAB_OTHER &
wait
( cd $J/cand && env OPENBLAS_NUM_THREADS=1 $PY -I $SCR/compare.py $FR $RES > $LOG/$RUN-compare.log 2>&1 ); echo "compare rc=$?"
for arm in base prev cand; do
  echo "$arm in-tree parquets: $(find $J/$arm/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $J/$arm status --porcelain | head -5
done
echo DONE
