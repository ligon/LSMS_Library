#!/bin/bash
#SBATCH --job-name=cov-refresh
#SBATCH --account=fc_jevons
#SBATCH --partition=savio3_htc
#SBATCH --qos=savio_normal
#SBATCH --cpus-per-task=4
#SBATCH --mem=24G
#SBATCH --time=01:30:00
#SBATCH --output=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%j.out
#SBATCH --error=/global/scratch/fsa/fc_jevons/ligon/coverage-refresh-logs/%x_%j.err
#
# WEEKLY warm/incremental coverage-matrix refresh — a self-resubmitting Slurm
# job (scrontab + crontab are disabled on Savio, so the scheduler IS the cron).
#
# Robustness:
#   - resubmit-FIRST (before any work) so a mid-run failure doesn't break the chain
#   - self-EXPIRES at COV_EXPIRES (human check-in; renew by re-seeding) so it
#     can't run unattended forever
#   - CHANGE-GATED: skips the build unless countries/** or the matrix code changed
#     since the last snapshot (don't regenerate datasets pointlessly)
#   - CONNECTIVITY-gated: most-but-not-all nodes have internet; on a bad node it
#     skips work + fires a bounded near-term retry. Resubmit needs only slurmctld,
#     so a bad node never breaks the chain.
#   - operates in a DEDICATED clone ($COV_WORKDIR); never touches the main checkout.
#
# Env knobs (propagated across the chain via --export=ALL):
#   COV_BRANCH=development          branch to refresh + push
#   COV_WORKDIR=<scratch>/coverage-refresh   dedicated clone
#   COV_EXPIRES=YYYY-MM-DD          stop resubmitting after this date
#   COV_INTERVAL_DAYS=7             weekly cadence
#   COV_DRYRUN=0                    1 -> git push --dry-run (no write)
#   COV_FORCE=0                     1 -> build even if change-gate says no
#   COV_NO_RESUBMIT=0               1 -> one-shot (no chain) — used by the dry run
#   COV_COUNTRIES=""                scope the build (default: all)
#   COV_RETRY=0                     internal: no-internet retry counter
#
# Seed the chain (production):
#   sbatch --export=ALL,COV_EXPIRES=2026-12-25 bin/coverage_refresh.sbatch
# Stop it:  touch ~/.lsms_coverage_refresh.STOP   (or let it expire)

set -u
REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
cd "$REPO"
source bin/coverage_lib.sh

SELF="$REPO/bin/coverage_refresh.sbatch"
BRANCH="${COV_BRANCH:-development}"
WORKDIR="${COV_WORKDIR:-/global/scratch/fsa/fc_jevons/ligon/coverage-refresh}"
INTERVAL="${COV_INTERVAL_DAYS:-7}"
DRYRUN="${COV_DRYRUN:-0}"
FORCE="${COV_FORCE:-0}"
RETRY="${COV_RETRY:-0}"
STOP="${COV_STOP:-$HOME/.lsms_coverage_refresh.STOP}"
EXPIRES="${COV_EXPIRES:-}"
TODAY="$(date -u +%F)"
echo "=== cov-refresh: node=$(hostname) branch=$BRANCH retry=$RETRY dryrun=$DRYRUN $(date) ==="

# --- 0. stop conditions (kill-switch / expiry) ---------------------------------
expired=0
if [ -n "$EXPIRES" ] && [ "$TODAY" \> "$EXPIRES" ]; then expired=1; fi

# --- 1. resubmit-FIRST (keep the weekly chain alive) ---------------------------
if [ "${COV_NO_RESUBMIT:-0}" != "1" ] && [ "$RETRY" = "0" ]; then
    if [ -f "$STOP" ]; then
        echo "kill-switch $STOP present -> not resubmitting (chain stops)."
    elif [ "$expired" = "1" ]; then
        echo "chain EXPIRED ($TODAY > $EXPIRES) -> not resubmitting."
        echo "  renew with: sbatch --export=ALL,COV_EXPIRES=<future> $SELF"
    else
        sbatch --begin="now+${INTERVAL}days" --export=ALL,COV_RETRY=0 "$SELF" \
            && echo "scheduled next refresh in ${INTERVAL}d"
    fi
fi
[ "$expired" = "1" ] && { echo "expired; exiting without work."; exit 0; }

# --- 2. venv -------------------------------------------------------------------
if ! cov_provision_venv "$REPO"; then echo "venv provisioning failed"; exit 1; fi
echo "  PY=$PY"

# --- 3. connectivity gate (+ bounded retry onto a connected node) --------------
if ! cov_have_internet; then
    echo "no internet on $(hostname)."
    if [ "$RETRY" -lt 3 ] && [ "${COV_NO_RESUBMIT:-0}" != "1" ]; then
        sbatch --begin="now+20minutes" --export=ALL,COV_RETRY=$((RETRY+1)) "$SELF" \
            && echo "  fired retry $((RETRY+1))/3"
    fi
    exit 0
fi

# --- 4. dedicated clone --------------------------------------------------------
if ! cov_ensure_clone "$WORKDIR" "$BRANCH" "${COV_CLONE_SRC:-file://$REPO}"; then
    echo "clone/fetch failed"; exit 1
fi

# --- 5. expiry warning (always, even on no-change polls) -----------------------
cov_notify_expiry "$BRANCH" "$EXPIRES"

# --- 6. change-gate: only regenerate when something relevant changed -----------
if [ "$FORCE" != "1" ] && ! cov_config_changed "$WORKDIR"; then
    echo "no config/code change since last snapshot -> skipping build (poll only)."
    exit 0
fi

# --- 7. warm build + status + commit + push -----------------------------------
COUNTRIES_ARG=()
[ -n "${COV_COUNTRIES:-}" ] && COUNTRIES_ARG=(--countries ${COV_COUNTRIES})
echo "--- warm build ---"
cov_warm_build "$WORKDIR" "${COUNTRIES_ARG[@]}"
cov_write_status "$WORKDIR" "$BRANCH" "$INTERVAL" "$EXPIRES"
echo "--- commit + push ---"
cov_commit_push "$WORKDIR" "$BRANCH" "$DRYRUN"
echo "=== cov-refresh done $(date) ==="
