# sourced by the job scripts
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam
SCR=$R/scripts
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0
HEAD=6538bc8b88c331835c9f78a316f3bf2092bae2b5
J=/local/job$SLURM_JOB_ID
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR PYTHONPATH
test -r ~/.config/lsms_library/config.yml || { echo "config unreadable"; exit 9; }
setup_clones() {
  for side in "$@"; do
    sha=$BASE; [ $side = head ] && sha=$HEAD
    git clone -q --no-hardlinks $MIRROR $J/$side || exit 2
    git -C $J/$side checkout -q --detach $sha || exit 2
    echo "$side HEAD $(git -C $J/$side rev-parse HEAD)"
    ( cd $J/$side && bin/savio_venv.sh mount ) || exit 3
  done
  PY=$J/venv_img/bin/python
}
# runpy SIDE TAG cmd...   (fresh private root per invocation)
runpy() {
  side=$1; tag=$2; shift 2
  P=$J/priv-$tag; rm -rf $P; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $J/$side && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$side \
      $PY "$@" )
}
