#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 3:00:00
#SBATCH -J rt1604-build
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-redteam/logs/job1-%j.out
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-redteam
SCR=$OUT/scripts
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
CAND=7735489cceb0fd68acf273eff850a5974d71cc58
J=/local/job$SLURM_JOB_ID
FR=$OUT/frames
mkdir -p $FR $OUT/results
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND

for side in base cand; do
  sha=$BASE; [ $side = cand ] && sha=$CAND
  git clone -q --no-hardlinks $MIRROR $J/$side || exit 2
  git -C $J/$side checkout -q --detach $sha || exit 2
  echo "$side HEAD $(git -C $J/$side rev-parse HEAD)"
done
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
( cd $J/base && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python

runside() {
  side=$1
  P=$J/priv-$side; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $J/$side && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$side \
      $PY $SCR/build.py $J/$side $side $FR > $OUT/logs/build-$side.log 2>&1 )
  echo "build $side rc=$?"
}
runside base &
runside cand &
wait

$PY $SCR/compare.py $FR $OUT/results/compare.json > $OUT/logs/compare.log 2>&1; echo "compare rc=$?"

P=$J/priv-cand
( cd $J/cand && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
    MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/cand \
    $PY $SCR/probe.py $J/cand $FR $OUT/results/probe.json > $OUT/logs/probe.log 2>&1 ); echo "probe rc=$?"
( cd $J/cand && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
    MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/cand \
    $PY $SCR/warm.py $J/cand $FR $OUT/results/warm.json > $OUT/logs/warm.log 2>&1 ); echo "warm rc=$?"

for side in base cand; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side:"; git -C $J/$side status --porcelain | head -20
done
du -sh $J/priv-* 2>/dev/null
echo JOB1 DONE
