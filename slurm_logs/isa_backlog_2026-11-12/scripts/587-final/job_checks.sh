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
#SBATCH -J f587-checks
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final/logs/checks-%j.out
# Candidate-only cold community_prices build + registry / label checks.
set -uo pipefail
J=/local/job$SLURM_JOB_ID; mkdir -p $J
export TMPDIR=$J/tmp; mkdir -p $TMPDIR
[ "$(hostname -s)" != "n0035" ] || exit 9
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-final
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final
SCR=$OUT/scripts; LOG=$PRIV/logs; RES=$PRIV/results/${RUN:?}; FR=$PRIV/frames/$RUN
mkdir -p $RES $FR
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
CAND=${CAND:?}
for v in $(env | sed -n 's/^\(LSMS_[A-Z_]*\)=.*/\1/p'); do unset $v; done
unset PYTHONPATH XDG_CACHE_HOME MPLCONFIGDIR
git clone -q --no-hardlinks $MIRROR $J/cand || exit 2
git -C $J/cand checkout -q --detach $CAND || exit 2
[ "$(git -C $J/cand rev-parse HEAD)" = "$CAND" ] || exit 2
M=$J/mnt; mkdir -p $M/xdg $M/data $M/mpl $M/tmp
( cd $J/cand && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl TMPDIR=$M/tmp LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
P=$J/priv-cand; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
( cd $J/cand && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp MPLCONFIGDIR=$P/mpl \
   LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/cand OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
   LSMS_BUILD_WORKERS=1 $PY $SCR/build.py $J/cand $FR $RES cand community_prices > $LOG/$RUN-build-cand.log 2>&1 )
echo "checks rc=$?"
echo "in-tree parquets: $(find $J/cand/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $J/cand status --porcelain | head -5
echo DONE
