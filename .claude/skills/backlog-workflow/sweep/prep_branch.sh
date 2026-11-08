#!/bin/bash
# prep_branch.sh <branch> <reviewed-development-sha> <full-commit-sha>...
# Uses already admitted local objects; does not fetch or choose a moving base.
# Ledger 1423 §§3–4: preserve authority on conflicts; bind the reviewed base.
set -euo pipefail
[ "$#" -ge 3 ] || { echo 'usage: prep_branch.sh <branch> <reviewed-base-sha> <commit-sha>...' >&2; exit 2; }
br=$1 base=$2; shift 2
die() { echo "prep_branch: needs coordinator: $*" >&2; exit 3; }
[[ $base =~ ^[0-9a-f]{40}$ ]] || die 'base must be a full SHA'
git check-ref-format --branch "$br" >/dev/null
for c in "$base" "$@"; do
  [[ $c =~ ^[0-9a-f]{40}$ ]] || die 'every commit must be a full SHA'
  [ "$(git rev-parse --verify "$c^{commit}")" = "$c" ] || die 'missing or unexpected local object'
done
remote=$(git ls-remote --refs github refs/heads/development)
[ "$remote" = "$base"$'\trefs/heads/development' ] || die 'reviewed base drifted or remote representation unsupported'
wt=${SWEEP_PR_DIR:?set an explicit private SWEEP_PR_DIR}/${br//\//_}
git -c core.hooksPath=/dev/null worktree add -q "$wt" -b "$br" "$base"
cd "$wt"
[ "$(git rev-parse HEAD)" = "$base" ] || die 'new worktree base mismatch'
for c in "$@"; do
  if ! git -c core.hooksPath=/dev/null cherry-pick -x "$c"; then
    die "cherry-pick stopped in $wt; preserve all prior authority, CONTENTS/log/TODO and semantic conflicts for review; do not blindly append or discard notes"
  fi
done
git log --oneline "$base..HEAD"
echo 'Re-review the resulting full candidate SHA; cherry-picking changes identity.'
