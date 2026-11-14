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
#SBATCH -J rt1560-oracle
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam/logs/job3-%j.out
set -uo pipefail
J=/local/job$SLURM_JOB_ID
mkdir -p $J/tmp
export TMPDIR=$J/tmp
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam
SCR=$OUT/scripts
RES=$OUT/results
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
CAND=43632ff5b1f21a14845d4cd01766d01b9649459d
mkdir -p $RES $OUT/logs
P0=$J/priv-mount; mkdir -p $P0/data $P0/xdg $P0/tmp $P0/mpl
export LSMS_DATA_DIR=$P0/data XDG_CACHE_HOME=$P0/xdg MPLCONFIGDIR=$P0/mpl LSMS_ISSUES_LOG=$P0/issues.log
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_QUANTITY_STRICT LSMS_GRAIN_STRICT LSMS_READ_STRICT
echo "host $(hostname) job $SLURM_JOB_ID start $(date -Is)"
git clone -q --shared --no-checkout $MIRROR $J/cand || exit 2
git -C $J/cand checkout -q --detach $CAND || exit 2
echo "cand HEAD $(git -C $J/cand rev-parse HEAD)"
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
export PATH=$J/venv_img/bin:$PATH
$PY -m py_compile $SCR/*.py || exit 4
P=$J/priv-oracle; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
( cd $J/cand && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
    MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/cand \
    timeout 9600 $PY $SCR/oracle.py $J/cand $RES/oracle.json > $OUT/logs/oracle.log 2>&1 )
echo "oracle rc=$?"
echo "in-tree parquets: $(find $J/cand/lsms_library/countries -name '*.parquet' | wc -l)"
echo "in-tree raw sources: $(git -C $J/cand status --porcelain --ignored lsms_library/countries | grep -ciE '\.(dta|csv|sav)$')"
echo "git status: $(git -C $J/cand status --porcelain | grep -v '^?? .venv$' | wc -l) changed"
echo "JOB3 DONE $(date -Is)"
