#!/bin/bash
# prep_branch.sh <branch> <commit>...  -- PR branch from FRESH GitHub development + cherry-picks.
#
# Run from the coordinator's clone (remote `github` = the GitHub repo over HTTPS).  Creates a
# worktree under $SWEEP_PR_DIR (default $SUCODER_LOCAL_ROOT/prs), cherry-picks the author's
# commits with -x, and the post-commit hook publishes the branch to the shared mirror, so
# reviewers and burst jobs can use it before anything reaches GitHub.  A cherry-pick that
# conflicts stops here: resolve it in the worktree (append-only conflicts in CONTENTS.org are
# the common case -- keep only the entries that belong to this branch), then
# `git cherry-pick --continue`.
set -euo pipefail
br=${1:?usage: prep_branch.sh <branch> <commit>...}; shift
git fetch -q github development:refs/remotes/github/development
wt=${SWEEP_PR_DIR:-${SUCODER_LOCAL_ROOT:?set SWEEP_PR_DIR}/prs}/${br//\//_}
git worktree add -q "$wt" -b "$br" github/development
cd "$wt"; for c in "$@"; do git cherry-pick -x "$c" >/dev/null; done
git log --oneline github/development..HEAD
