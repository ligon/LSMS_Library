# Sourced by every job.  Private roots FIRST, then clones, then the venv mount.
# Defines: OUT PRIV D MIRROR BASE PREV J PY, and run_in <arm> <rootname> <cmd...>
set -uo pipefail
OUT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections-final2
PRIV=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-final2
D=$OUT/scripts
export RESULTS=$OUT/results
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0      # development
PREV=e338fedbac29b283f987717136bbb8e4020f953b      # branch head before this pass
J=/local/job${SLURM_JOB_ID}
mkdir -p "$J" "$PRIV"; chmod 700 "$PRIV" "$J"
export TMPDIR=$J/tmp
# default private roots, exported BEFORE any mount or import
export LSMS_DATA_DIR=$J/root-default
export XDG_CACHE_HOME=$J/xdg-default
export MPLCONFIGDIR=$J/mpl-default
export LSMS_ISSUES_LOG=$J/issues-default.log
mkdir -p "$TMPDIR" "$LSMS_DATA_DIR" "$XDG_CACHE_HOME" "$MPLCONFIGDIR"
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_BUILD_BACKEND LSMS_ALLOW_INTREE_SOURCES || true
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
test -r "$HOME/.config/lsms_library/config.yml" && echo "config.yml readable"
echo "LSMS_SKIP_AUTH=${LSMS_SKIP_AUTH:-unset}"
echo "node $(hostname) job $SLURM_JOB_ID start $(date -Is)"
[ "$(hostname -s)" != "n0035" ] || { echo "refusing to run on n0035"; exit 9; }

clone_at() {  # clone_at <arm> <full-sha>
  git clone --quiet --shared --no-checkout "$MIRROR" "$J/$1" || return 2
  git -C "$J/$1" checkout --quiet --detach "$2" || return 2
  test "$(git -C "$J/$1" rev-parse HEAD)" = "$2" || return 2
  echo "$1 at $(git -C "$J/$1" rev-parse HEAD)"
}

run_in() {  # run_in <arm> <rootname> cmd...   (fresh private roots per name)
  local arm=$1 r=$J/priv-$2; shift 2
  mkdir -p "$r/data" "$r/xdg" "$r/tmp" "$r/mpl"; chmod 700 "$r"
  ( cd "$J/$arm" && env LSMS_DATA_DIR=$r/data XDG_CACHE_HOME=$r/xdg TMPDIR=$r/tmp MPLCONFIGDIR=$r/mpl \
      LSMS_ISSUES_LOG=$r/issues.log PYTHONPATH=$J/$arm LSMS_COUNTRIES_ROOT=$J/$arm/lsms_library/countries \
      LSMS_CLONE=$J/$arm "$@" )
}

mount_venv() {  # mount_venv <arm>
  ( cd "$J/$1" && bin/savio_venv.sh mount ) || return 4
  PY=$J/venv_img/bin/python
  [ -x "$PY" ] || PY=$J/$1/.venv/bin/python
  echo "PY=$PY"
}

run_tests() {  # run_tests <arm> <label>: the targeted Niger selection from THAT tree
  local arm=$1 label=$2 C=$J/$1
  ( cd "$C" && { grep -l -E '\bNiger\b|ehcvm' tests/test_*.py
      for f in tests/test_derivations.py tests/test_build_transform_hash.py \
               tests/test_no_build_input_in_contents_org.py tests/test_failure_is_not_absence.py \
               tests/test_schema_consistency.py tests/test_gh1578_derivation_duplicates.py \
               tests/test_gh1707_served_corrections.py; do [ -f "$f" ] && echo "$f"; done; } ) \
    | sort -u > "$OUT/results/pytest_${label}_selection.txt"
  echo "selected files ($label): $(wc -l < "$OUT/results/pytest_${label}_selection.txt")"
  mkdir -p "$PRIV/pytest"; chmod 700 "$PRIV/pytest"
  run_in "$arm" "tests-$label" timeout -s INT 4500 "$PY" -m pytest -n 8 --dist=loadfile -p no:cacheprovider -q -rfEs \
      $(cat "$OUT/results/pytest_${label}_selection.txt") > "$PRIV/pytest/pytest_${label}.txt" 2>&1
  echo "pytest $label rc=$?"
  # aggregates only into results: the summary line and failing node ids
  { tail -1 "$PRIV/pytest/pytest_${label}.txt"; grep -E '^(FAILED|ERROR) ' "$PRIV/pytest/pytest_${label}.txt" | cut -d' ' -f2 | sort -u; } \
      > "$OUT/results/pytest_${label}_summary.txt"
}
