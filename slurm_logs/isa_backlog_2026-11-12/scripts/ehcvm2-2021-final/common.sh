# sourced by the sbatch scripts.  Needs: SHA LABEL.  Sets: REPO PY OUT, private env.
set -euo pipefail
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
RD=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/ehcvm2-2021-final
L=/local/job$SLURM_JOB_ID
mkdir -p $L
OUT=$RD/jobs/$LABEL-$SLURM_JOB_ID
mkdir -p $OUT
clone() {  # clone SHA DIR
    git clone -q --shared --no-checkout $MIRROR $2
    git -C $2 checkout -q --detach $1
    test "$(git -C $2 rev-parse HEAD)" = "$1"
}
private_env() {  # private_env NAME
    local R=$L/priv-$1
    mkdir -p $R/data $R/xdg $R/tmp $R/mpl
    export LSMS_DATA_DIR=$R/data XDG_CACHE_HOME=$R/xdg TMPDIR=$R/tmp MPLCONFIGDIR=$R/mpl LSMS_ISSUES_LOG=$R/issues.log
    export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
    unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT || true
}
assert_identity() {  # assert_identity REPO
    ( cd $1 && PYTHONPATH=$1 $PY -W ignore -c "
import lsms_library, os
from lsms_library import paths
top = os.path.realpath('$1')
assert os.path.realpath(lsms_library.__file__).startswith(top + os.sep), lsms_library.__file__
root = os.path.realpath(str(paths.data_root()))
assert root.startswith('$L/'), root
print('identity ok', lsms_library.__file__, root)
" )
}
