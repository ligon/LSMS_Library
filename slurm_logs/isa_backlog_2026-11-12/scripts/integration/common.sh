# Sourced by every integration sbatch script, AFTER TMPDIR is set on the job's
# first line.  Needs LABEL.  Private roots are exported before the venv mount.
set -uo pipefail
case "$(hostname)" in n0035*) echo "refusing to run on n0035"; exit 4;; esac
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE_SHA=e5fdd2fa831c4d03a247d264be7f339da459e6b0
RD=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/integration
S=$RD/scripts
PRIVROOT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/integration
L=/local/job$SLURM_JOB_ID
mkdir -p $L
OUT=$RD/jobs/$LABEL-$SLURM_JOB_ID
mkdir -p $OUT
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_ALLOW_INTREE_SOURCES \
      LSMS_QUANTITY_STRICT LSMS_READ_STRICT LSMS_GRAIN_STRICT LSMS_DATE_STRICT \
      LSMS_CLUSTER_STRICT LSMS_LABELS_STRICT LSMS_BUILD_BACKEND LSMS_EXPECTED_ROOT \
      PYTHONPATH || true
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1

clone() {  # clone SHA DIR   (a --shared clone of the mirror at a FULL sha; no fetch in the mirror)
    [ ${#1} -eq 40 ] || { echo "clone: need a full sha, got $1"; exit 2; }
    git clone -q --shared --no-checkout $MIRROR $2 || exit 3
    git -C $2 checkout -q --detach $1 || exit 3
    [ "$(git -C $2 rev-parse HEAD)" = "$1" ] || { echo "clone $2 not at $1"; exit 3; }
    echo "clone $2 at $(git -C $2 rev-parse HEAD)"
}
private_env() {  # private_env DIR   (a private data root + caches; DIR persists on scratch)
    local R=$1
    mkdir -p $R/data $R/xdg $R/mpl $L/tmp-$(basename $R)
    export LSMS_DATA_DIR=$R/data XDG_CACHE_HOME=$R/xdg MPLCONFIGDIR=$R/mpl
    export LSMS_ISSUES_LOG=$R/issues.log TMPDIR=$L/tmp-$(basename $R)
}
mount_venv() {  # mount_venv REPO   (call after private_env)
    ( cd $1 && bin/savio_venv.sh mount ) || { echo "venv mount failed"; exit 4; }
    PY=$L/venv_img/bin/python
    [ -x $PY ] || { echo "no python at $PY"; exit 4; }
}
assert_identity() {  # assert_identity REPO   (uses the current LSMS_DATA_DIR)
    ( cd $1 && PYTHONPATH=$1 $PY -W ignore -c "
import lsms_library, os, subprocess
from lsms_library import paths
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
root = os.path.realpath(str(paths.data_root()))
assert root.startswith('$PRIVROOT/') or root.startswith('$L/'), root
assert not root.startswith('/global/scratch/fsa/fc_jevons/ligon/cache'), root
assert '.local/share/lsms_library' not in root, root
print('identity ok', subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(), lsms_library.__file__, root)
" ) || { echo "identity FAILED for $1"; exit 5; }
}
hygiene() {  # hygiene REPO
    echo "in-tree parquet $1: $(find $1/lsms_library/countries -name '*.parquet' | wc -l)"
    echo "git status $1: $(git -C $1 status --porcelain | grep -v -e '^?? .venv' | wc -l) lines"
}
