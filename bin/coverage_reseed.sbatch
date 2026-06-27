#!/bin/bash
#SBATCH --job-name=cov-reseed
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --time=00:15:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%j.out
#SBATCH --error=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%j.err
#
# MONTHLY authoritative reseed CONTROLLER. A tiny self-resubmitting job (no heavy
# work here): keeps the monthly cadence alive, then fans the clean-cache cube out
# as a parallel array + a dependent finalizer that commits/pushes. This catches
# stale-cache hazards (GH #479) that the weekly warm refresh can carry.
#
# Same robustness contract as coverage_refresh.sbatch (resubmit-first, expiry,
# kill-switch, connectivity gate, dedicated clone). Seed:
#   sbatch --export=ALL,COV_EXPIRES=2026-12-25 bin/coverage_reseed.sbatch
set -u
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
source bin/coverage_lib.sh

SELF="$REPO/bin/coverage_reseed.sbatch"
BRANCH="${COV_BRANCH:-development}"
WD="${COV_WORKDIR:-/global/scratch/fsa/fc_jevons/ligon/coverage-reseed}"
SHARDROOT="${COV_SHARDROOT:-/global/scratch/fsa/fc_jevons/ligon/coverage-reseed-shards}"
INTERVAL="${COV_RESEED_DAYS:-30}"
STOP="${COV_STOP:-$HOME/.lsms_coverage_refresh.STOP}"
EXPIRES="${COV_EXPIRES:-}"
RETRY="${COV_RETRY:-0}"
TODAY="$(date -u +%F)"
echo "=== cov-reseed controller: node=$(hostname) branch=$BRANCH retry=$RETRY $(date) ==="

expired=0
if [ -n "$EXPIRES" ] && [ "$TODAY" \> "$EXPIRES" ]; then expired=1; fi

# resubmit-first (monthly)
if [ "${COV_NO_RESUBMIT:-0}" != "1" ] && [ "$RETRY" = "0" ]; then
    if [ -f "$STOP" ]; then echo "kill-switch present -> not resubmitting."
    elif [ "$expired" = "1" ]; then
        echo "EXPIRED ($TODAY > $EXPIRES) -> not resubmitting. Renew: sbatch --export=ALL,COV_EXPIRES=<future> $SELF"
    else
        sbatch --begin="now+${INTERVAL}days" --export=ALL,COV_RETRY=0 "$SELF" \
            && echo "scheduled next reseed in ${INTERVAL}d"
    fi
fi
[ "$expired" = "1" ] && { echo "expired; exiting."; exit 0; }

if ! cov_provision_venv "$REPO"; then echo "venv failed"; exit 1; fi
if ! cov_have_internet; then
    echo "no internet on $(hostname)."
    if [ "$RETRY" -lt 3 ] && [ "${COV_NO_RESUBMIT:-0}" != "1" ]; then
        sbatch --begin="now+20minutes" --export=ALL,COV_RETRY=$((RETRY+1)) "$SELF"
    fi
    exit 0
fi

if ! cov_ensure_clone "$WD" "$BRANCH" "${COV_CLONE_SRC:-file://$REPO}"; then echo "clone failed"; exit 1; fi

mkdir -p "$SHARDROOT/shards"; rm -f "$SHARDROOT/shards/"*.csv
LSMS_COUNTRIES_ROOT="$WD/lsms_library/countries" \
    "$PY" -c "import lsms_library as ll; print(chr(10).join(ll.countries()))" > "$SHARDROOT/countries.txt"
N=$(wc -l < "$SHARDROOT/countries.txt")
[ "$N" -ge 1 ] || { echo "no countries enumerated"; exit 1; }
echo "reseeding $N countries (authoritative, parallel)"

AID=$(sbatch --parsable --array=0-$((N-1))%10 \
    --export=ALL,COV_WORKDIR="$WD",COV_SHARDROOT="$SHARDROOT" \
    "$REPO/bin/coverage_reseed_shard.sbatch")
echo "  shard array: $AID"
FID=$(sbatch --parsable --dependency=afterany:"$AID" \
    --export=ALL,COV_WORKDIR="$WD",COV_SHARDROOT="$SHARDROOT",COV_BRANCH="$BRANCH",COV_EXPIRES="$EXPIRES",COV_RESEED_DAYS="$INTERVAL",COV_DRYRUN="${COV_DRYRUN:-0}" \
    "$REPO/bin/coverage_reseed_finalize.sbatch")
echo "  finalize: $FID (afterany:$AID)"
echo "=== controller done; heavy work runs in $AID + $FID ==="
