#!/bin/bash
#SBATCH --job-name=cov-reseed-finalize
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=2
#SBATCH --mem=12G
#SBATCH --time=00:30:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%j.out
#SBATCH --error=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%j.err
#
# Dependent (afterany) finalizer for the monthly reseed: merge the per-country
# shards into the clone's snapshot, write status, commit + push. Pure pandas +
# git (no heavy build) -> single job, squashfs mount is fine.
set -u
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
source bin/coverage_lib.sh
WD="${COV_WORKDIR:?}"
SHARDS="${COV_SHARDROOT:?}/shards"
BRANCH="${COV_BRANCH:-development}"
DRYRUN="${COV_DRYRUN:-0}"
INTERVAL="${COV_RESEED_DAYS:-30}"
EXPIRES="${COV_EXPIRES:-}"
echo "=== reseed finalize: node=$(hostname) branch=$BRANCH $(date) ==="

cov_provision_venv "$REPO" || { echo "venv failed"; exit 1; }
cov_have_internet || { echo "no internet; cannot push -> abort finalize (shards kept)"; exit 0; }
cov_ensure_clone "$WD" "$BRANCH" "${COV_CLONE_SRC:-file://$REPO}" || { echo "clone failed"; exit 1; }

n=$(ls "$SHARDS"/*.csv 2>/dev/null | wc -l)
echo "  aggregating $n shards"
[ "$n" -eq 0 ] && { echo "no shards; nothing to do"; exit 0; }
( cd "$WD" && LSMS_COUNTRIES_ROOT="$WD/lsms_library/countries" \
    "$PY" -m bench.matrix_aggregate "$SHARDS" \
          --snapshot "$WD/.coder/coverage/latest.csv" --html /tmp/reseed_matrix.html )

cov_write_status "$WD" "$BRANCH" "$INTERVAL" "$EXPIRES"
cov_notify_expiry "$BRANCH" "$EXPIRES"
cov_commit_push "$WD" "$BRANCH" "$DRYRUN"
echo "=== reseed finalize done $(date) ==="
