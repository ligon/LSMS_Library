#!/bin/bash
# Shared helpers for the coverage-matrix auto-refresh (weekly warm + monthly
# authoritative). Sourced by bin/coverage_refresh.sbatch and the reseed jobs.
#
# Design constraints (Savio):
#   - scrontab + user crontab are BOTH disabled -> the schedule is a
#     self-resubmitting Slurm job (the wrapper does `sbatch --begin`).
#   - MOST but NOT ALL compute nodes have outbound internet -> gate on
#     connectivity; resubmitting the chain needs only slurmctld (no internet),
#     so a bad node never breaks the chain.
#   - The job must NEVER mutate the human's main checkout: all git work happens
#     in a DEDICATED clone ($COV_WORKDIR), built with the installed package's
#     CODE but the clone's CONFIG (LSMS_COUNTRIES_ROOT, per CLAUDE.md GH#436) and
#     the clone's snapshot path (--snapshot).
set -u

GH_URL="https://github.com/ligon/LSMS_Library.git"

cov_have_internet() { curl -sf -m 15 -o /dev/null https://github.com; }

# cov_ensure_clone <workdir> <branch> <src>
#   <src> = a fast local source for the initial clone (e.g. file://$REPO) or the
#   GitHub URL. The remote is always reset to GitHub afterwards (for push).
cov_ensure_clone() {
    local wd="$1" branch="$2" src="$3"
    if [ ! -d "$wd/.git" ]; then
        echo "  cloning $src -> $wd"
        git clone -q "$src" "$wd" || return 1
    fi
    git -C "$wd" remote set-url origin "$GH_URL"
    git -C "$wd" fetch -q origin "$branch" || return 1
    git -C "$wd" checkout -q "$branch" 2>/dev/null \
        || git -C "$wd" checkout -q -b "$branch" "origin/$branch" || return 1
    git -C "$wd" reset --hard -q "origin/$branch"
}

# cov_warm_build <workdir> [extra matrix.py args...]
#   Warm (incremental) build: the v0.8.0 content-hash cache rebuilds only cells
#   whose inputs changed. Writes the clone's snapshot; no HTML (docs render it).
cov_warm_build() {
    local wd="$1"; shift
    LSMS_COUNTRIES_ROOT="$wd/lsms_library/countries" \
        "$PY" "$wd/bench/matrix.py" --no-html \
              --snapshot "$wd/.coder/coverage/latest.csv" "$@"
}

# cov_commit_push <workdir> <branch> <dryrun>
#   Commits the refreshed snapshot iff it changed; pushes to <branch> via the gh
#   credential helper. dryrun=1 -> `git push --dry-run` (full auth, no write).
# cov_write_status <workdir> <branch> <interval_days> <expires>
#   Maintains .coder/coverage/refresh_status.json (rendered as a freshness +
#   expiry banner on the docs page). Committed alongside the snapshot, so it adds
#   no churn beyond actual data-change commits.
cov_write_status() {
    local wd="$1" branch="$2" interval="$3" expires="$4"
    cat > "$wd/.coder/coverage/refresh_status.json" <<EOF
{
  "last_refreshed": "$(date -u +%FT%TZ)",
  "node": "$(hostname)",
  "branch": "$branch",
  "interval_days": $interval,
  "expires": "$expires"
}
EOF
}

# cov_notify_expiry <branch> <expires>
#   Within 21 days of expiry, open a GitHub issue ONCE (GitHub emails the human)
#   so the chain warns before it retires. Idempotent via gh issue search.
cov_notify_expiry() {
    local branch="$1" expires="$2"
    [ -z "$expires" ] && return 0
    command -v gh >/dev/null 2>&1 || return 0
    local now exp_s days title
    now=$(date -u +%s); exp_s=$(date -u -d "$expires" +%s 2>/dev/null) || return 0
    days=$(( (exp_s - now) / 86400 ))
    { [ "$days" -gt 21 ] || [ "$days" -lt 0 ]; } && return 0
    title="Coverage auto-refresh expires $expires"
    if gh issue list --repo ligon/LSMS_Library --state open \
         --search "in:title $title" --json number --jq '.[0].number' 2>/dev/null | grep -q .; then
        echo "  expiry issue already open; not duplicating."; return 0
    fi
    echo "  filing expiry-warning GitHub issue (~$days days left)"
    gh issue create --repo ligon/LSMS_Library --title "$title" --body \
"The coverage-matrix auto-refresh chain stops on **$expires** (~$days days).
Renew it (one command) or it quietly retires:

\`\`\`
sbatch --export=ALL,COV_EXPIRES=<new future date> bin/coverage_refresh.sbatch
\`\`\`

Deliberate stop: \`touch ~/.lsms_coverage_refresh.STOP\`.

— filed automatically by \`bin/coverage_refresh.sbatch\` from $(hostname)." 2>&1 | tail -1
}

cov_commit_push() {
    local wd="$1" branch="$2" dryrun="$3"
    git -C "$wd" add .coder/coverage/latest.csv .coder/coverage/refresh_status.json
    if git -C "$wd" diff --cached --quiet; then
        echo "  snapshot unchanged; nothing to commit"; return 0
    fi
    git -C "$wd" -c user.name="Sue the Coder" -c user.email="coder@sucoder.dev" \
        commit -q -m "data(coverage): auto-refresh matrix snapshot $(date -u +%F)" \
        -m "Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
    local flag=""; [ "$dryrun" = "1" ] && flag="--dry-run"
    echo "  pushing${flag:+ (dry-run)} to origin/$branch"
    git -C "$wd" -c credential.helper='!gh auth git-credential' push $flag origin "$branch"
}

# cov_config_changed <workdir> <branch>
#   True (rc 0) iff config/code that affects the matrix changed on <branch> since
#   the last commit that touched the snapshot. Lets the chain SKIP the build when
#   nothing relevant changed (don't regenerate datasets pointlessly). No prior
#   snapshot -> treat as changed (build once).
cov_config_changed() {
    local wd="$1"
    local base
    base=$(git -C "$wd" log -1 --format=%H -- .coder/coverage/latest.csv 2>/dev/null)
    [ -z "$base" ] && return 0
    ! git -C "$wd" diff --quiet "$base" HEAD -- \
        lsms_library/countries lsms_library/coverage_matrix.py bench/matrix.py
}

# cov_provision_venv <repo>  -> sets PY (squashfs mount; fallback Lustre master)
cov_provision_venv() {
    local repo="$1"
    if bash "$repo/bin/savio_venv.sh" mount >/dev/null 2>&1 && [ -x "$repo/.venv/bin/python" ]; then
        PY="$repo/.venv/bin/python"
    else
        PY="$repo/.venv.lustre/bin/python"
    fi
    export PY
    "$PY" -c 'import lsms_library' >/dev/null 2>&1
}
