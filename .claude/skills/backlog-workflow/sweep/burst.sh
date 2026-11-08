#!/bin/bash
# burst.sh --source <shared-git-dir> --expected-source-sha <full-sha>
#   --python <prepared-absolute-interpreter> --coordinator-job <id>
#   [--work-root <private-parent>] [--lsms-import-check] -- <command>...
# Private empty roots ALWAYS. No credential loading, data acquisition, or shared
# blob projection is granted by this wrapper. Task-specific source admission is
# separate. This environment wrapper is NOT universal I/O confinement.
set -euo pipefail
source_repo= sha= runtime= coord= work_root= import_check=0
declare -A seen
die() { echo "burst: needs coordinator: $*" >&2; exit 3; }
while [ "$#" -gt 0 ] && [ "$1" != -- ]; do
  [ -z "${seen[$1]:-}" ] || die "duplicate option $1"
  seen[$1]=1
  case $1 in
    --source|--expected-source-sha|--python|--coordinator-job|--work-root)
      [ "$#" -ge 2 ] || die "missing value for $1"
      case $1 in
        --source) source_repo=$2;; --expected-source-sha) sha=$2;;
        --python) runtime=$2;; --coordinator-job) coord=$2;; --work-root) work_root=$2;;
      esac; shift 2;;
    --lsms-import-check) import_check=1; shift;;
    *) die "unknown option $1";;
  esac
done
[ "${1:-}" = -- ] || die 'explicit -- and command required'
shift
[ "$#" -gt 0 ] || die 'empty command'
[[ $sha =~ ^[0-9a-f]{40}$ ]] || die 'full expected source SHA required'
[[ $source_repo = /* && -d $source_repo ]] || die 'explicit shared local Git directory required (no URL)'
[[ $runtime = /* && -x $runtime ]] || die 'explicit prepared interpreter required'
job=${SLURM_JOB_ID:?run in a separate Slurm allocation}
cpus=${SLURM_CPUS_PER_TASK:?allocation cpus-per-task required}
[[ $job =~ ^[0-9]+$ && $coord =~ ^[0-9]+$ && $cpus =~ ^[1-9][0-9]*$ ]] || die 'invalid job/coordinator/task CPU binding'
[ "$job" != "$coord" ] || die "refusing coordinator allocation $coord"
work_root=${work_root:-/local/job$job}
[[ $work_root = /* ]] || die 'private parent must be absolute'
mkdir -p "$work_root"
root=$(mktemp -d "$work_root/sweep.XXXXXXXX")
mkdir -p "$root/config/lsms_library" "$root/data/dvc-cache" "$root/state/dvc-site" \
         "$root/cache" "$root/tmp" "$root/mpl"
# Establish ALL roots and remove inherited credentials/acquisition/cache bypasses
# before Git, optional identity import, or task command (ledger 1423 §4).
# HOME and CODEX_HOME retain their exact supplied values. No native state is read.
clean=(env -i "HOME=$HOME" "PATH=$(dirname "$runtime"):$PATH"
       "USER=${USER:-}" "LOGNAME=${LOGNAME:-}" "LANG=C.UTF-8"
       "SLURM_JOB_ID=$job" "SLURM_CPUS_PER_TASK=$cpus"
       "PROJECT_PYTHON=$runtime" "PYTHONNOUSERSITE=1"
       "GIT_CONFIG_NOSYSTEM=1" "GIT_CONFIG_GLOBAL=/dev/null" "GIT_TERMINAL_PROMPT=0"
       "LSMS_CONFIG_DIR=$root/config/lsms_library" "XDG_CONFIG_HOME=$root/config"
       "LSMS_DATA_DIR=$root/data" "LSMS_SKIP_AUTH=1" "LSMS_BUILD_WORKERS=1"
       "XDG_STATE_HOME=$root/state" "XDG_RUNTIME_DIR=$root/state"
       "DVC_SITE_CACHE_DIR=$root/state/dvc-site" "XDG_CACHE_HOME=$root/cache"
       "LSMS_ISSUES_LOG=$root/cache/issues.log" "TMPDIR=$root/tmp" "TMP=$root/tmp" "TEMP=$root/tmp"
       "MPLCONFIGDIR=$root/mpl" "OPENBLAS_NUM_THREADS=1" "OMP_NUM_THREADS=1" "MKL_NUM_THREADS=1"
       "NUMEXPR_NUM_THREADS=1" "BLIS_NUM_THREADS=1" "VECLIB_MAXIMUM_THREADS=1")
if [ "${CODEX_HOME+x}" = x ]; then clean+=("CODEX_HOME=$CODEX_HOME"); fi
exec "${clean[@]}" /bin/bash -s -- "$source_repo" "$sha" "$runtime" "$root" "$import_check" "$@" <<'ISOLATED'
set -euo pipefail
source_repo=$1 sha=$2 runtime=$3 root=$4 import_check=$5; shift 5
W=$root/source
export W PYTHONPATH=$W LSMS_COUNTRIES_ROOT=$W/lsms_library/countries
# Source must have been admitted as full committed code/metadata, no raw blobs
# or credentials. No --shared, alternates or writable cache links are introduced.
git -c core.hooksPath=/dev/null clone -q --no-checkout --no-hardlinks "$source_repo" "$W"
cd "$W"
git -c core.hooksPath=/dev/null checkout -q --detach "$sha"
actual=$(git rev-parse --verify HEAD)
[ "$actual" = "$sha" ] || { echo 'burst: source SHA mismatch; needs coordinator' >&2; exit 3; }
if git symbolic-ref -q HEAD; then
  echo 'burst: source must be detached; needs coordinator' >&2; exit 3
fi
printf 'BURST job=%s cpus-per-task=%s source=%s private-root=%s\n' \
       "$SLURM_JOB_ID" "$SLURM_CPUS_PER_TASK" "$actual" "$root"
if [ "$import_check" = 1 ]; then
  # Optional: use only when the task admits LSMS import; source-free tasks omit it.
  "$runtime" -c 'import lsms_library,os; p=os.path.realpath(lsms_library.__file__); w=os.path.realpath(os.environ["W"]); assert p.startswith(w+os.sep),p; print("BURST import",p)'
fi
exec "$@"
ISOLATED
