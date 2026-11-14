#!/bin/bash
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 8
#SBATCH --mem=64G
#SBATCH -t 2:00:00
#SBATCH -J rt1604-deriv
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-redteam/logs/job3-%j.out
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-redteam
SCR=$OUT/scripts
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
CAND=7735489cceb0fd68acf273eff850a5974d71cc58
J=/local/job$SLURM_JOB_ID
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND
git clone -q --no-hardlinks $MIRROR $J/cand || exit 2
git -C $J/cand checkout -q --detach $CAND || exit 2
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
P=$J/priv; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
E="env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/cand"
PK=$J/pkl
( cd $J/cand && $E $PY $SCR/coldwarm.py $J/cand cold $PK $OUT/results/cold3.json > $OUT/logs/cold3.log 2>&1 ); echo "cold rc=$?"
( cd $J/cand && $E PROBE_SECTIONS=derivation_join,metric_and_borrow_scope,food_acquired_2024_units PROBE_FA=$PK/food_acquired_all.pkl \
    $PY $SCR/probe.py $J/cand $OUT/frames $OUT/results/probe2.json > $OUT/logs/probe2.log 2>&1 ); echo "probe2 rc=$?"
( cd $J/cand && $E $PY $SCR/coldwarm.py $J/cand warm $PK $OUT/results/warm3.json > $OUT/logs/warm3.log 2>&1 ); echo "warm rc=$?"
echo "in-tree parquets cand: $(find $J/cand/lsms_library/countries -name '*.parquet' | wc -l)"
git -C $J/cand status --porcelain | head
echo JOB3 DONE
