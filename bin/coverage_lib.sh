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
#     in a DEDICATED clone ($COV_WORKDIR), built with the clone's CODE
#     (PYTHONPATH, since GH #957 removed the editable install that used to
#     impose the mirror's code) AND the clone's CONFIG (LSMS_COUNTRIES_ROOT,
#     per CLAUDE.md GH#436) and the clone's snapshot path (--snapshot).
#     Before #957 only the config came from the clone; the code came from the
#     mirror, so a refresh never exercised library changes on the branch.
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

# cov_provision_venv <repo> <code_root>  -> sets PY + PYTHONPATH; nonzero on failure
#
#   <repo>       owns .venv.sqfs / .venv.lustre (the shared mirror)
#   <code_root>  the checkout whose lsms_library MUST be imported (the clone)
#
# <code_root> is REQUIRED and is deliberately not defaulted to <repo>.  A default
# makes the identity check self-certifying -- set PYTHONPATH=<repo>, then assert
# the import came from <repo> -- so a caller that forgot the argument would build
# from the mirror and pass green, which is the exact regression the check exists
# to catch.
#
# This no longer calls `savio_venv.sh mount`.  That repoints "$repo/.venv", a
# symlink SHARED by every job using this mirror: a coverage job on one node
# would point it at its own /local path and break a condo slice holding a mount
# on another (GH #958).  Mount at this job's own path and use it directly, as
# slurm_logs/release_v0.10.0/burst_pytest.sh has always done.
cov_provision_venv() {
    local repo="$1" code_root="${2:-}" mnt shim out
    local squashfuse=/usr/libexec/apptainer/bin/squashfuse_ll
    if [ -z "$code_root" ]; then
        echo "cov_provision_venv: code_root (argument 2) is required" >&2
        return 2
    fi
    # Never inherit PY.  It is exported, and the reseed chain launches children
    # with --export=ALL, so a child receives /local/job<PARENT>/... -- a path that
    # does not exist on the child's node.  Resetting here is what keeps the
    # fallback below reachable in exactly the case it exists for.
    PY=""
    mnt="/local/job${SLURM_JOB_ID:-none}/venv_img"
    shim="/local/job${SLURM_JOB_ID:-none}/.fuseshim"
    if [ -n "${SLURM_JOB_ID:-}" ] && [ -f "$repo/.venv.sqfs" ]; then
        if [ -x "$mnt/bin/python" ]; then
            :                                  # already materialised in this job
        elif [ -c /dev/fuse ] && [ -x "$squashfuse" ]; then
            mkdir -p "$shim" "$mnt"
            ln -sf /usr/bin/fusermount "$shim/fusermount3"
            out=$(PATH="$shim:$PATH" "$squashfuse" "$repo/.venv.sqfs" "$mnt" 2>&1) \
                || echo "cov_provision_venv: squashfuse mount failed: $out" >&2
        elif command -v unsquashfs >/dev/null 2>&1; then
            # No FUSE: extract instead (still ONE Lustre inode read).  Without
            # this branch the fallback is .venv.lustre, whose imports round-trip
            # Lustre -- one `import lsms_library` there did not complete in 120s,
            # against walltimes of 30 and 90 minutes.
            echo "cov_provision_venv: no FUSE; extracting image -> $mnt" >&2
            out=$(unsquashfs -f -d "$mnt" "$repo/.venv.sqfs" 2>&1) \
                || echo "cov_provision_venv: unsquashfs failed: $out" >&2
        fi
        [ -x "$mnt/bin/python" ] && PY="$mnt/bin/python"
    fi
    if [ -z "$PY" ]; then
        PY="$repo/.venv.lustre/bin/python"
        echo "cov_provision_venv: falling back to the Lustre master $PY (slow imports)" >&2
    fi
    export PY PYTHONPATH="$code_root"
    # code_root travels by ENVIRONMENT, not string interpolation: COV_WORKDIR is
    # operator-supplied and a quote or backslash in it would become a SyntaxError
    # reported as "venv provisioning failed".
    # The prefix test appends os.sep so it compares PATH COMPONENTS -- this
    # deployment has real sibling pairs (coverage-refresh / coverage-refresh-logs,
    # coverage-reseed / coverage-reseed-shards).
    out=$(LSMS_COV_CODE_ROOT="$code_root" "$PY" -c '
import lsms_library, os, sys
want = os.path.realpath(os.environ["LSMS_COV_CODE_ROOT"]).rstrip(os.sep) + os.sep
got = os.path.realpath(lsms_library.__file__)
if not got.startswith(want):
    sys.exit("imported %s, expected under %s" % (got, want))
' 2>&1) || {
        echo "cov_provision_venv: $PY cannot import lsms_library from $code_root: $out" >&2
        return 1
    }
    return 0
}
