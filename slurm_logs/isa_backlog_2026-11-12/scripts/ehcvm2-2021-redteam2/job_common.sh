# sourced by the job scripts; TMPDIR is set first, before anything else
export TMPDIR=/local/job$SLURM_JOB_ID/tmp; mkdir -p $TMPDIR
set -uo pipefail
O=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-redteam2
SCR=$O/scripts
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
PREV=6538bc8b88c331835c9f78a316f3bf2092bae2b5
HEAD=9b5585cf2dd9264616ee56255aedb5acef22f1f5
J=/local/job$SLURM_JOB_ID
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_DATA_DIR PYTHONPATH
test -r ~/.config/lsms_library/config.yml || { echo "config unreadable"; exit 9; }
echo "node $(hostname) TMPDIR=$TMPDIR"
setup_clones() {
  for side in "$@"; do
    sha=$PREV; [ $side = head ] && sha=$HEAD
    git clone -q --no-hardlinks $MIRROR $J/$side || exit 2
    git -C $J/$side checkout -q --detach $sha || exit 2
    echo "$side HEAD $(git -C $J/$side rev-parse HEAD)"
    ( cd $J/$side && bin/savio_venv.sh mount ) 2>&1 | grep -v FutureWarning | grep -v warnings.warn || true
  done
  PY=$J/venv_img/bin/python
  [ -x $PY ] || { echo "venv mount failed"; exit 3; }
  $PY -m py_compile $SCR/*.py && echo "py_compile OK" || echo "py_compile FAILED"
}
# runpy SIDE TAG cmd...   (fresh private root per invocation)
runpy() {
  side=$1; tag=$2; shift 2
  P=$J/priv-$tag; rm -rf $P; mkdir -p $P/data $P/xdg $P/tmp $P/mpl
  ( cd $J/$side && env PRIVATE_ROOT=$P LSMS_DATA_DIR=$P/data XDG_CACHE_HOME=$P/xdg TMPDIR=$P/tmp \
      MPLCONFIGDIR=$P/mpl LSMS_ISSUES_LOG=$P/issues.log PYTHONPATH=$J/$side \
      $PY "$@" )
}
