# Sourced by each burst job:  source common.sh <FULL_SHA> <TAG>
# Clones the mirror at an exact SHA into /local/job$SLURM_JOB_ID, mounts the
# venv image, and points EVERY cache/data root at a private per-TAG directory.
set -euo pipefail
SHA="$1"; TAG="$2"
E=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-reverify
J=/local/job$SLURM_JOB_ID
mkdir -p "$J"
SRC="$J/src-$TAG"
if [ ! -d "$SRC/.git" ]; then
  git clone -q --no-hardlinks /global/home/users/ligon/mirrors/LSMS_Library "$SRC"
fi
git -C "$SRC" checkout -q --detach "$SHA"
test "$(git -C "$SRC" rev-parse HEAD)" = "$SHA"
export LSMS_DATA_DIR="$J/data-$TAG"
export XDG_CACHE_HOME="$J/xdg-$TAG"
export TMPDIR="$J/tmp-$TAG"
export MPLCONFIGDIR="$J/mpl-$TAG"
export LSMS_ISSUES_LOG="$J/issues-$TAG.log"
mkdir -p "$LSMS_DATA_DIR" "$XDG_CACHE_HOME" "$TMPDIR" "$MPLCONFIGDIR"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
unset LSMS_NO_CACHE LSMS_SKIP_AUTH LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_ALLOW_INTREE_SOURCES || true
cd "$SRC"
bin/savio_venv.sh mount
PY="$SRC/.venv/bin/python"
[ -x "$PY" ] || PY="$J/venv_img/bin/python"
[ -x "$PY" ] || { echo "no python"; exit 3; }
export PATH="$(dirname "$PY"):$PATH"
export PYTHONPATH="$SRC"
# Identity + private-root assertion, in a process of its own.
"$PY" - <<'PYEOF'
import os, subprocess, lsms_library
from lsms_library import paths
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
dr = os.path.realpath(paths.data_root())
for bad in ('/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library',
            os.path.realpath(os.path.expanduser('~/.local/share/lsms_library'))):
    assert not (dr == bad or dr.startswith(bad + os.sep)), dr
assert dr.startswith(os.path.realpath(os.environ['LSMS_DATA_DIR'])), dr
print('identity ok:', lsms_library.__file__, '| data_root:', dr)
PYEOF
echo "env ready: SHA=$SHA TAG=$TAG SRC=$SRC host=$(hostname) job=$SLURM_JOB_ID"
