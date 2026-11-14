# Sourced by every job.  Usage: source env.sh <SHA> <root-name>
# Private roots FIRST, then clone, then mount the venv image.
set -euo pipefail
SHA="$1"; ROOTNAME="$2"
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections-redteam
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
export RESULTS=$OUT/results
JOBLOCAL=/local/job${SLURM_JOB_ID}
[ -d "$JOBLOCAL" ] || JOBLOCAL=/tmp/rt-job${SLURM_JOB_ID}
mkdir -p "$JOBLOCAL"
export TMPDIR=$JOBLOCAL/tmp
export LSMS_DATA_DIR=$PRIV/root-$ROOTNAME
export XDG_CACHE_HOME=$JOBLOCAL/xdg
export MPLCONFIGDIR=$JOBLOCAL/mpl
export LSMS_ISSUES_LOG=$JOBLOCAL/issues.log
mkdir -p "$TMPDIR" "$LSMS_DATA_DIR" "$XDG_CACHE_HOME" "$MPLCONFIGDIR"
chmod 700 "$PRIV" "$LSMS_DATA_DIR"
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND || true
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
test -r "$HOME/.config/lsms_library/config.yml" && echo "config.yml readable"

CLONE=$JOBLOCAL/clone-$ROOTNAME
git clone --quiet --shared --no-checkout "$MIRROR" "$CLONE"
git -C "$CLONE" checkout --quiet --detach "$SHA"
test "$(git -C "$CLONE" rev-parse HEAD)" = "$SHA"
echo "clone $CLONE at $(git -C "$CLONE" rev-parse HEAD) on $(hostname)"
cd "$CLONE"
bin/savio_venv.sh mount
PY=$CLONE/.venv/bin/python
export LSMS_CLONE=$CLONE
export PYTHONPATH=$CLONE
export LSMS_COUNTRIES_ROOT=$CLONE/lsms_library/countries
"$PY" - <<'EOF'
import os, subprocess, lsms_library
top = os.environ["LSMS_CLONE"]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.paths import data_root
dr = os.path.realpath(str(data_root()))
assert dr.startswith("/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam/"), dr
assert "/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library" not in dr and ".local/share/lsms_library" not in dr
print("identity ok:", lsms_library.__file__, "data_root:", dr)
EOF
