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
#SBATCH -J sc-explore3
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections/logs/explore3-%j.out
# Read-only exploration on base development e5fdd2fa8, private roots on /local.
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections
SCR=$OUT/scripts; RES=$OUT/results; LOG=$OUT/logs
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
SHA=${SHA:-e5fdd2fa831c4d03a247d264be7f339da459e6b0}
J=/local/job$SLURM_JOB_ID
mkdir -p $J/tmp
export TMPDIR=$J/tmp
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR
echo "node $(hostname) start $(date -Is) SHA=$SHA"
git clone -q --no-hardlinks $MIRROR $J/base || exit 2
git -C $J/base checkout -q --detach $SHA || exit 2
echo "HEAD $(git -C $J/base rev-parse HEAD)"
( cd $J/base && bin/savio_venv.sh mount ) || exit 3
PY=$J/venv_img/bin/python
[ -x $PY ] || PY=$J/base/.venv/bin/python

envp() {  # envp <subroot> cmd...
  local P=$J/$1; shift; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/base "$@"
}

# four sections in parallel, each in its own private root
for s in A; do
  ( cd $J/base && envp priv-$s $PY $SCR/explore3.py $J/base $RES/explore3.json > $LOG/explore3.log 2>&1; echo "explore $s rc=$?" ) &
done
wait
for s in A; do
  echo "issues log $s: $(wc -l < $J/priv-$s/issues.log 2>/dev/null || echo 0)"
done
echo "in-tree parquets: $(find $J/base/lsms_library/countries -name '*.parquet' | wc -l)"
git -C $J/base status --porcelain | head
du -sh $J/priv-* 2>/dev/null
echo "EXPLORE JOB DONE $(date -Is)"
