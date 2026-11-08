#!/bin/bash
# submit.sh [-c CPUS] [-m MEM] [-t TIME] [-a ACCOUNT] [-p PART] [-q QOS]
#   [-l LOG] --coordinator-job ID --exclude-nodes NODELIST
#   [--billed-account-authorized] -- <burst.sh options> -- <command>...
# Defaults are the free, pre-emptible condo account. A billed-account override
# requires existing user authorization AND explicit acknowledgement here.
# Runs foreground srun in a clean environment; preserves its actual exit code.
# Ledger 1423 §§3–4: resources/exclusions are explicit, billing is separately authorized.
set -euo pipefail
here=$(dirname "$(readlink -f "$0")")
c=4 m=24G t=1:00:00 a=co_carleton p=savio4_htc q=carleton_htc4_normal log=
coord= excluded= billed=0
declare -A seen
die() { echo "submit: needs coordinator: $*" >&2; exit 3; }
while [ "$#" -gt 0 ] && [ "$1" != -- ]; do
  [ -z "${seen[$1]:-}" ] || die "duplicate option $1"
  seen[$1]=1
  case $1 in
    --billed-account-authorized) billed=1; shift;;
    -c|-m|-t|-a|-p|-q|-l|--coordinator-job|--exclude-nodes)
      [ "$#" -ge 2 ] || die "missing value for $1"
      case $1 in
        -c) c=$2;; -m) m=$2;; -t) t=$2;; -a) a=$2;; -p) p=$2;; -q) q=$2;; -l) log=$2;;
        --coordinator-job) coord=$2;; --exclude-nodes) excluded=$2;;
      esac; shift 2;;
    *) die "unknown option $1";;
  esac
done
[ "${1:-}" = -- ] || die 'explicit -- required'
shift
[ "$#" -gt 0 ] || die 'burst arguments required'
# The coordinator exclusion is owned by this wrapper, not a forwarded override.
for arg in "$@"; do
  [ "$arg" != --coordinator-job ] || die 'forwarded coordinator override refused'
done
[[ $c =~ ^[1-9][0-9]*$ && $coord =~ ^[0-9]+$ && -n $excluded ]] || die 'explicit CPUs, coordinator job and excluded nodes required'
[ -z "${SLURM_JOB_ID:-}" ] || [ "$coord" = "$SLURM_JOB_ID" ] || die 'coordinator job disagrees with current allocation'
[ "$a" = co_carleton ] || [ "$billed" = 1 ] || die 'non-free account requires user authorization acknowledgement'
# Allowlisted environment: no inherited SLURM_*, LSMS/cache roots, credential,
# acquisition, Python, tool-cache or native local-root overrides reach srun.
# PATH is an operator input: review/pin its executables with the task plan.
clean=(env -i "HOME=$HOME" "PATH=$PATH" "USER=${USER:-}" "LOGNAME=${LOGNAME:-}" "LANG=C.UTF-8")
if [ "${CODEX_HOME+x}" = x ]; then clean+=("CODEX_HOME=$CODEX_HOME"); fi
cmd=("${clean[@]}" srun -A "$a" -p "$p" -q "$q" -N1 -n1 -c "$c" --mem="$m" -t "$t"
     --exclude="$excluded" -J sweep-burst "$here/burst.sh" --coordinator-job "$coord" "$@")
if [ -n "$log" ]; then
  mkdir -p "$(dirname "$log")"
  set +e
  "${cmd[@]}" 2>&1 | tee "$log"
  codes=("${PIPESTATUS[@]}")
  set -e
  [ "${codes[0]}" = 0 ] || exit "${codes[0]}"
  exit "${codes[1]}"
fi
exec "${cmd[@]}"
