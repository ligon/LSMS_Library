# burst-env.sh -- source this INSIDE a co_carleton burst job (never on n0035).
#
#   source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh <stream> <full-sha> [<full-sha> ...]
#
# Sets the private roots FIRST (before any mount or import), clones each SHA
# from the shared mirror into /local/job$SLURM_JOB_ID/src-<sha12>, mounts the
# venv image in the first clone, and asserts import identity + a private data
# root for every clone.  Exports:
#   PY            the venv interpreter (use with PYTHONPATH=<clone> and cd <clone>)
#   SRC_<i>       clone dir of the i-th SHA (SRC_1, SRC_2, ...)
#   DATA_<i>      a private, initially empty LSMS_DATA_DIR for the i-th clone
#   OUT           $REPORTS/<stream>/out (created)
#   ident <clone> <data-root>   re-check identity
# Data roots are node-local by default (fast, wiped at job end).  Set
# KEEP_DATA=1 before sourcing to put them under the batch's private/ on scratch
# instead (survives the job; tell Sue, it is cleaned up only on the user's go).
set -uo pipefail
_STREAM=${1:?stream name}; shift
[ "$#" -ge 1 ] || { echo "burst-env: need at least one full SHA"; return 2; }
case "$(hostname -s)" in n0035) echo "burst-env: refusing the coordinator node n0035"; return 3;; esac
: "${SLURM_JOB_ID:?burst-env: run inside a Slurm job}"
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
MIRROR=/global/home/users/ligon/mirrors/LSMS_Library
JOBLOCAL=/local/job${SLURM_JOB_ID}
[ -d "$JOBLOCAL" ] || { echo "burst-env: no $JOBLOCAL"; return 3; }
OUT=$BATCH/reports/$_STREAM/out; mkdir -p "$OUT"
# --- private roots, before anything imports ---
export TMPDIR=$JOBLOCAL/tmp XDG_CACHE_HOME=$JOBLOCAL/xdg MPLCONFIGDIR=$JOBLOCAL/mpl
export LSMS_ISSUES_LOG=$JOBLOCAL/issues.log
mkdir -p "$TMPDIR" "$XDG_CACHE_HOME" "$MPLCONFIGDIR"
unset LSMS_SKIP_AUTH LSMS_NO_CACHE LSMS_COUNTRIES_ROOT LSMS_QUANTITY_STRICT LSMS_READ_STRICT \
      LSMS_GRAIN_STRICT LSMS_DATE_STRICT LSMS_CLUSTER_STRICT LSMS_LABELS_STRICT || true
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
if [ "${KEEP_DATA:-0}" = 1 ]; then _DBASE=$BATCH/private/$_STREAM/job-$SLURM_JOB_ID; else _DBASE=$JOBLOCAL/private; fi
_i=0
for _sha in "$@"; do
  _i=$((_i+1))
  [ ${#_sha} -eq 40 ] || { echo "burst-env: need full 40-char SHAs ($_sha)"; return 2; }
  _dir=$JOBLOCAL/src-${_sha:0:12}
  if [ ! -d "$_dir/.git" ]; then
    git clone --quiet --shared --no-checkout "$MIRROR" "$_dir" || return 4
    git -C "$_dir" checkout --quiet --detach "$_sha" || return 4
  fi
  [ "$(git -C "$_dir" rev-parse HEAD)" = "$_sha" ] || { echo "burst-env: $_dir not at $_sha"; return 4; }
  mkdir -p "$_DBASE/data-$_i"
  export "SRC_$_i=$_dir" "DATA_$_i=$_DBASE/data-$_i"
done
export LSMS_DATA_DIR=$DATA_1
( cd "$SRC_1" && bin/savio_venv.sh mount ) || { echo "burst-env: venv mount failed"; return 5; }
PY=$SRC_1/.venv/bin/python; export PY
ident() {  # ident <clone> <data-root>
  ( cd "$1" && PYTHONPATH=$1 LSMS_DATA_DIR=$2 "$PY" - <<'EOF'
import lsms_library, os, subprocess
from lsms_library import paths
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
r = os.path.realpath(str(paths.data_root()))
assert r.startswith('/local/job') or '/isa-sm-20261113/private/' in r, r
assert '/cache/lsms_library' not in r and '.local/share' not in r, r
print('identity ok', subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(), r)
EOF
  )
}
for _j in $(seq 1 $_i); do _s=SRC_$_j; _d=DATA_$_j; ident "${!_s}" "${!_d}" || return 6; done
echo "burst-env ready: job $SLURM_JOB_ID on $(hostname -s), $SLURM_CPUS_ON_NODE cpus, PY=$PY, OUT=$OUT"
# Hash census, base vs candidate (config reads only, no data built):
#   (cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $BATCH/tools/hash_census.py $OUT/census-cand.json)
#   (cd $SRC_2 && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $BATCH/tools/hash_census.py $OUT/census-base.json)
#   $PY $BATCH/tools/hashdiff.py $OUT/census-base.json $OUT/census-cand.json $OUT/hashdiff.json
# Tests: cd $SRC_1; PYTHONPATH=$SRC_1 $PY -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -p no:cacheprovider <files> > $OUT/pytest-<x>.txt 2>&1
