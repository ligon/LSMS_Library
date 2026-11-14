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
#SBATCH -J rt1560-extra
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam/logs/job4-%j.out
set -uo pipefail
J=/local/job$SLURM_JOB_ID
mkdir -p $J/tmp
export TMPDIR=$J/tmp
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-redteam
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
CAND=43632ff5b1f21a14845d4cd01766d01b9649459d
P0=$J/priv-mount; mkdir -p $P0/data $P0/xdg $P0/tmp $P0/mpl
export LSMS_DATA_DIR=$P0/data XDG_CACHE_HOME=$P0/xdg MPLCONFIGDIR=$P0/mpl LSMS_ISSUES_LOG=$P0/issues.log
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND
git clone -q --shared --no-checkout $MIRROR $J/cand && git -C $J/cand checkout -q --detach $CAND || exit 2
( cd $J/cand && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
cd $J/cand && env PYTHONPATH=$J/cand $PY $OUT/scripts/extra.py $OUT/frames $OUT/results/extra.json
echo "extra rc=$?"
echo "JOB4 DONE $(date -Is)"
