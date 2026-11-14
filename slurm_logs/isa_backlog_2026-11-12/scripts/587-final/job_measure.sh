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
#SBATCH -J f587-measure
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final/logs/measure-%j.out
set -uo pipefail
J=/local/job$SLURM_JOB_ID; mkdir -p $J
export TMPDIR=$J/tmp; mkdir -p $TMPDIR
[ "$(hostname -s)" != "n0035" ] || exit 9
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/587-final
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/587-final
SCR=$OUT/scripts; LOG=$PRIV/logs; RES=$PRIV/results
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
SHA=${SHA:?}; TAG=${TAG:?}
for v in $(env | sed -n 's/^\(LSMS_[A-Z_]*\)=.*/\1/p'); do unset $v; done
unset PYTHONPATH XDG_CACHE_HOME MPLCONFIGDIR
git clone -q --no-hardlinks $MIRROR $J/src || exit 2
git -C $J/src checkout -q --detach $SHA || exit 2
[ "$(git -C $J/src rev-parse HEAD)" = "$SHA" ] || exit 2
# private roots BEFORE the mount (its verify-import runs the library)
M=$J/mnt; mkdir -p $M/xdg $M/data $M/mpl $M/tmp
( cd $J/src && env XDG_CACHE_HOME=$M/xdg LSMS_DATA_DIR=$M/data MPLCONFIGDIR=$M/mpl TMPDIR=$M/tmp LSMS_ISSUES_LOG=$M/issues.log bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
P=$J/priv; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
( cd $J/src && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp MPLCONFIGDIR=$P/mpl \
   LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/src OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
   LSMS_BUILD_WORKERS=1 $PY $SCR/${SCRIPT:-measure.py} $J/src $RES $TAG > $LOG/${SCRIPT:-measure.py}-$TAG.log 2>&1 ); echo "measure rc=$?"
echo "in-tree parquets: $(find $J/src/lsms_library/countries -name '*.parquet' | wc -l)"; git -C $J/src status --porcelain | head
echo DONE
