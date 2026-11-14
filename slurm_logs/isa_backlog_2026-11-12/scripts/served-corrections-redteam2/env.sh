# Sourced by every job (inside the Slurm job only).  Defines `setup <SHA> <name>`.
# Private roots are exported BEFORE any clone is mounted.
set -uo pipefail
export OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections-redteam2
export PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam2
export MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
export D=$OUT/scripts
export RESULTS=$OUT/results
export DEV=e5fdd2fa831c4d03a247d264be7f339da459e6b0
export PREV=3ae15b56ceb8093acaf75c8e24369dcd4d012aea
export HEADSHA=06bac74298656d4f76b300ddd3b736f61eaf3f69
export JOBLOCAL=/local/job${SLURM_JOB_ID}
test -d "$JOBLOCAL" || { echo "no $JOBLOCAL"; exit 3; }
export TMPDIR=$JOBLOCAL/tmp
export XDG_CACHE_HOME=$JOBLOCAL/xdg
export MPLCONFIGDIR=$JOBLOCAL/mpl
export LSMS_ISSUES_LOG=$JOBLOCAL/issues.log
mkdir -p "$TMPDIR" "$XDG_CACHE_HOME" "$MPLCONFIGDIR" "$PRIV"
chmod 700 "$PRIV"
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_BUILD_BACKEND || true
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
test -r "$HOME/.config/lsms_library/config.yml" && echo "config.yml readable"

clone_at() {  # clone_at <SHA> <name>  -> $JOBLOCAL/clone-<name>, git only
  local sha="$1" name="$2" c="$JOBLOCAL/clone-$2"
  if [ ! -d "$c" ]; then
    git clone --quiet --shared --no-checkout "$MIRROR" "$c"
    git -C "$c" checkout --quiet --detach "$sha"
  fi
  test "$(git -C "$c" rev-parse HEAD)" = "$sha" || { echo "clone $c not at $sha"; return 4; }
  echo "clone $c at $(git -C "$c" rev-parse HEAD)"
}

setup() {  # setup <SHA> <name>: private root root-<name>, clone, mount, assert identity
  local sha="$1" name="$2"
  export LSMS_DATA_DIR=$PRIV/root-$name
  mkdir -p "$LSMS_DATA_DIR"; chmod 700 "$LSMS_DATA_DIR"
  export LSMS_ISSUES_LOG=$JOBLOCAL/issues-$name.log
  clone_at "$sha" "$name" || return 4
  export CLONE=$JOBLOCAL/clone-$name
  unset PYTHONPATH LSMS_COUNTRIES_ROOT LSMS_CLONE || true
  ( cd "$CLONE" && bin/savio_venv.sh mount ) || return 5
  export PY=$CLONE/.venv/bin/python
  export LSMS_CLONE=$CLONE PYTHONPATH=$CLONE LSMS_COUNTRIES_ROOT=$CLONE/lsms_library/countries
  cd "$CLONE"
  "$PY" - <<'EOF' || return 6
import os, subprocess, lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(top) == os.path.realpath(os.environ["LSMS_CLONE"]), top
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.paths import data_root, countries_root
dr = os.path.realpath(str(data_root()))
assert dr == os.path.realpath(os.environ["LSMS_DATA_DIR"]), dr
assert dr.startswith("/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam2/"), dr
assert "/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library" not in dr and ".local/share/lsms_library" not in dr
assert os.path.realpath(str(countries_root())).startswith(os.path.realpath(top) + os.sep), countries_root()
print("identity ok:", lsms_library.__file__, "data_root:", dr, "head:",
      subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
EOF
}

hygiene() {  # hygiene <name>
  local c="$JOBLOCAL/clone-$1"
  echo "[$1] in-tree parquet: $(find "$c/lsms_library/countries" -name '*.parquet' 2>/dev/null | wc -l)"
  echo "[$1] issues log lines: $(cat "$JOBLOCAL/issues-$1.log" 2>/dev/null | wc -l)"
  cp -f "$JOBLOCAL/issues-$1.log" "$PRIV/issues-$1.log" 2>/dev/null || true
}
