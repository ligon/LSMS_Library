#!/usr/bin/env bash
# Sweep driver: run repro_micro.py under each candidate DVC config, with
# a per-trial cache wipe so every trial starts cold for the chosen
# targets.
#
# Usage:
#   slurm_logs/dvc_lock_repro/run_sweep.sh [--trials N] [--n N] [--configs c1,c2,...]
#
# Recognised config keys (apply additive `dvc config` flags relative to
# baseline; restored to baseline at sweep end):
#   baseline          - no extra config
#   shared            - cache.shared = group
#   cachetype         - cache.type = reflink,hardlink,symlink,copy
#   shared_cachetype  - shared + cachetype (the rumor combo)
#   hardlink_lock     - core.hardlink_lock = true
#   shared_hl         - shared + cachetype + hardlink_lock
#
# Output: slurm_logs/dvc_lock_repro/sweep_<timestamp>/
#   <config>_t<N>.log    raw stdout per trial
#   summary.tsv          one row per trial: config, trial, n, elapsed,
#                        successes, failures, fail_rate, exc_summary
set -uo pipefail
# NB: no `-e` -- the reproducer intentionally returns non-zero whenever
# any child fails (which is *every* trial under contention).  We want to
# record the failures, not bail.

REPO=/global/home/users/ligon/mirrors/LSMS_Library
SCRIPT_DIR="$REPO/slurm_logs/dvc_lock_repro"
DVC_CFG="$REPO/lsms_library/countries/.dvc/config"
DVC_DIR="$REPO/lsms_library/countries"

# Defaults
TRIALS=3
N=12
CONFIGS_RAW="baseline,shared_cachetype,hardlink_lock,shared_hl"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --trials) TRIALS="$2"; shift 2 ;;
    --n)      N="$2"; shift 2 ;;
    --configs) CONFIGS_RAW="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

IFS=',' read -ra CONFIGS <<< "$CONFIGS_RAW"
TS=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/sweep_${TS}"
mkdir -p "$OUTDIR"
echo "config	trial	n	elapsed	successes	failures	fail_rate	exc" > "$OUTDIR/summary.tsv"

# Snapshot the original .dvc/config so we can restore.  The library does
# not write to .dvc/config at runtime; the cache.dir override is via the
# DVCFileSystem(config=...) constructor in local_tools.py.  Toggling the
# repo-level config is safe and atomic.
ORIG_CFG="$OUTDIR/dvc_config.original"
cp -p "$DVC_CFG" "$ORIG_CFG"

restore_config () {
  cp -p "$ORIG_CFG" "$DVC_CFG"
}
trap restore_config EXIT

apply_config () {
  local cfg="$1"
  cp -p "$ORIG_CFG" "$DVC_CFG"
  case "$cfg" in
    baseline)         : ;;  # no-op
    shared)           ( cd "$DVC_DIR" && dvc config cache.shared group ) ;;
    cachetype)        ( cd "$DVC_DIR" && dvc config cache.type 'reflink,hardlink,symlink,copy' ) ;;
    shared_cachetype) ( cd "$DVC_DIR" \
                         && dvc config cache.shared group \
                         && dvc config cache.type 'reflink,hardlink,symlink,copy' ) ;;
    hardlink_lock)    ( cd "$DVC_DIR" && dvc config core.hardlink_lock true ) ;;
    shared_hl)        ( cd "$DVC_DIR" \
                         && dvc config cache.shared group \
                         && dvc config cache.type 'reflink,hardlink,symlink,copy' \
                         && dvc config core.hardlink_lock true ) ;;
    *) echo "unknown config: $cfg" >&2; return 1 ;;
  esac
}

for cfg in "${CONFIGS[@]}"; do
  apply_config "$cfg"
  echo "=========================================================="
  echo "config=$cfg  trials=$TRIALS  n=$N"
  echo "  .dvc/config:"; sed -n '1,40p' "$DVC_CFG" | sed 's/^/    /'
  echo "=========================================================="
  for t in $(seq 1 "$TRIALS"); do
    LOG="$OUTDIR/${cfg}_t${t}.log"
    echo "[$cfg trial=$t] starting at $(date +%H:%M:%S)" | tee "$LOG"
    # Disable pipefail just for this pipe -- the python script returns
    # nonzero by design when any child fails, and we want the run to
    # continue.
    set +o pipefail
    "$REPO/.venv/bin/python" "$SCRIPT_DIR/repro_micro.py" \
        --n "$N" \
        --clear-cache \
        --label "${cfg}_t${t}" \
        2>&1 | tee -a "$LOG"
    set -o pipefail
    # Extract the RESULT_JSON line and append a summary row
    "$REPO/.venv/bin/python" - <<PY >> "$OUTDIR/summary.tsv"
import json, re
log = open("$LOG").read()
m = re.search(r"^RESULT_JSON (.*)$", log, re.M)
if not m:
    print("\t".join(["$cfg","$t","$N","NA","NA","NA","NA","NO_RESULT"]))
else:
    r = json.loads(m.group(1))
    print("\t".join(["$cfg", str("$t"), str(r["n"]),
                     f"{r['elapsed']:.2f}",
                     str(r["successes"]), str(r["failures"]),
                     f"{r['fail_rate']:.3f}",
                     json.dumps(r["exc_counts"])]))
PY
  done
done

restore_config
trap - EXIT

echo ""
echo "Sweep complete.  Summary:"
column -t -s $'\t' "$OUTDIR/summary.tsv"
echo ""
echo "Artifacts in: $OUTDIR"
