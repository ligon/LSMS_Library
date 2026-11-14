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
#SBATCH -J rt1560-build
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam/logs/job1-%j.out
set -uo pipefail
J=/local/job$SLURM_JOB_ID
mkdir -p $J/tmp
export TMPDIR=$J/tmp
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam
SCR=$OUT/scripts
FR=$OUT/frames
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
CAND=43632ff5b1f21a14845d4cd01766d01b9649459d
mkdir -p $FR $OUT/results $OUT/logs
# Private roots BEFORE any library import (the venv mount runs one).
P0=$J/priv-mount; mkdir -p $P0/data $P0/xdg $P0/tmp $P0/mpl
export LSMS_DATA_DIR=$P0/data XDG_CACHE_HOME=$P0/xdg MPLCONFIGDIR=$P0/mpl LSMS_ISSUES_LOG=$P0/issues.log
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_QUANTITY_STRICT LSMS_GRAIN_STRICT LSMS_READ_STRICT
echo "host $(hostname) job $SLURM_JOB_ID start $(date -Is)"

for side in base cand; do
  sha=$BASE; [ $side = cand ] && sha=$CAND
  git clone -q --shared --no-checkout $MIRROR $J/$side || exit 2
  git -C $J/$side checkout -q --detach $sha || exit 2
  echo "$side HEAD $(git -C $J/$side rev-parse HEAD)"
done
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
( cd $J/base && bin/savio_venv.sh mount ) || true
PY=$J/venv_img/bin/python
export PATH=$J/venv_img/bin:$PATH

envp() {  # envp <root-name> <clone> cmd...
  local P=$J/priv-$1; local C=$2; shift 2; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $C && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$C "$@" )
}

# Stage 0: compile + identity/root asserts
$PY -m py_compile $SCR/*.py || exit 4
for side in base cand; do
  envp $side $J/$side $PY -c "import sys; sys.path.insert(0,'$SCR'); from common import assert_env; assert_env('$J/$side')" || exit 5
done
envp make $J/cand env LSMS_BUILD_BACKEND=make $PY -c "import sys; sys.path.insert(0,'$SCR'); from common import assert_env; assert_env('$J/cand')" || exit 5
touch $OUT/logs/job1.stage0.ok

# Stage 1: cold builds (5 processes)
envp base $J/base timeout 9000 $PY $SCR/build.py $J/base base $FR Niger:ALL > $OUT/logs/build-base-niger.log 2>&1 &
envp base $J/base timeout 9000 $PY $SCR/build.py $J/base base $FR Uganda:crop_production Mali:ALL > $OUT/logs/build-base-mali.log 2>&1 &
envp cand $J/cand timeout 9000 $PY $SCR/build.py $J/cand cand $FR Niger:ALL > $OUT/logs/build-cand-niger.log 2>&1 &
envp cand $J/cand timeout 9000 $PY $SCR/build.py $J/cand cand $FR Uganda:crop_production Mali:ALL > $OUT/logs/build-cand-mali.log 2>&1 &
envp make $J/cand env LSMS_BUILD_BACKEND=make timeout 9000 $PY $SCR/build.py $J/cand make $FR Niger:crop_production Mali:crop_production > $OUT/logs/build-make.log 2>&1 &
wait
echo "builds done $(date -Is)"

# Stage 2: warm post checks, then compare
envp base $J/base timeout 1200 $PY $SCR/post.py $J/base base $FR > $OUT/logs/post-base.log 2>&1 &
envp cand $J/cand timeout 1200 $PY $SCR/post.py $J/cand cand $FR > $OUT/logs/post-cand.log 2>&1 &
wait
envp cand $J/cand timeout 1800 $PY $SCR/compare.py $J/cand $FR $OUT/results/compare.json > $OUT/logs/compare.log 2>&1
echo "compare rc=$?"

for side in base cand; do
  echo "in-tree parquets $side: $(find $J/$side/lsms_library/countries -name '*.parquet' | wc -l)"
  echo "git status $side: $(git -C $J/$side status --porcelain | grep -v '^?? .venv$' | wc -l) changed"
done
du -sh $J/priv-* 2>/dev/null
echo "JOB1 DONE $(date -Is)"
