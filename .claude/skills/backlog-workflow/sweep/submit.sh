#!/bin/bash
# submit.sh -- run a command against a PUBLISHED branch in a SEPARATE Slurm job.
#
#   submit.sh [-c CPUS] [-m MEM] [-t TIME] [-a ACCOUNT] [-p PART] [-q QOS] [-l LOG] -- [--cold] <branch> <cmd...>
#
# Defaults: -c 8 -m 32G -t 1:00:00 -a co_carleton -p savio4_htc -q carleton_htc4_normal.
# co_carleton is free but pre-emptible above the condo share; use e.g. -a fc_jevons -q savio_normal
# for runs that must not be interrupted.  Runs in the foreground (srun) and tees to -l LOG.
#
# Why a wrapper: `srun` issued from inside an allocation becomes a STEP of that allocation
# (the coordinator's own slice) unless the caller's SLURM_* environment is removed.  The
# coordinator's node-local paths (TMPDIR, private LSMS_DATA_DIR, PYTHONPATH...) must not leak
# into the job either.  The job runs burst.sh, which must live on shared storage next to
# this script (copy this directory to the sweep's shared dir first; see ../savio-sweep.md).
set -euo pipefail
here=$(dirname "$(readlink -f "$0")")
c=8 m=32G t=1:00:00 a=co_carleton p=savio4_htc q=carleton_htc4_normal log=
while [ $# -gt 0 ] && [ "$1" != "--" ]; do
  case $1 in -c) c=$2;; -m) m=$2;; -t) t=$2;; -a) a=$2;; -p) p=$2;; -q) q=$2;; -l) log=$2;;
             *) echo "submit.sh: unknown flag $1" >&2; exit 2;; esac; shift 2; done
[ "${1:-}" = "--" ] && shift
export SWEEP_COORD_JOB=${SLURM_JOB_ID:-none}   # burst.sh refuses to run inside this job
unset $(env | grep -o '^SLURM[A-Z_]*') 2>/dev/null || true
unset TMPDIR UV_CACHE_DIR PIP_CACHE_DIR UV_TOOL_DIR npm_config_cache SUCODER_LOCAL_ROOT \
      LSMS_DATA_DIR LSMS_COUNTRIES_ROOT PYTHONPATH
cmd=(srun -A "$a" -p "$p" -q "$q" -N1 -n1 -c "$c" --mem="$m" -t "$t" -J sweep-burst "$here/burst.sh" "$@")
if [ -n "$log" ]; then mkdir -p "$(dirname "$log")"; "${cmd[@]}" 2>&1 | tee "$log"; exit "${PIPESTATUS[0]}"
else exec "${cmd[@]}"; fi
