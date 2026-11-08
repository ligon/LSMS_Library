#!/bin/bash
# open_pr.sh <branch> <reviewed-head-sha> <reviewed-base-sha> <title> <body-file>
# Coordinator only, after publication authority. Never force-push or mask failure.
# Ledger 1423 §4: exact reviewed identities gate publication, including after push.
set -euo pipefail
[ "$#" = 5 ] || { echo 'usage: open_pr.sh <branch> <head-sha> <development-sha> <title> <body-file>' >&2; exit 2; }
br=$1 head=$2 base=$3 title=$4 body=$5
die() { echo "open_pr: needs coordinator: $*" >&2; exit 3; }
[[ $head =~ ^[0-9a-f]{40}$ && $base =~ ^[0-9a-f]{40}$ ]] || die 'full reviewed head/base SHAs required'
git check-ref-format --branch "$br" >/dev/null
body=$(realpath -e "$body")
[ -f "$body" ] || die 'body must be an explicit regular report artifact'
cd "${SWEEP_PR_DIR:?set an explicit private SWEEP_PR_DIR}/${br//\//_}"
[ "$(git symbolic-ref --short HEAD)" = "$br" ] || die 'worktree branch mismatch'
[ "$(git rev-parse HEAD)" = "$head" ] || die 'local head drift'
[ "$(git merge-base "$head" "$base")" = "$base" ] || die 'reviewed base is not candidate ancestor'
expected_base="$base"$'\trefs/heads/development'
[ "$(git ls-remote --refs github refs/heads/development)" = "$expected_base" ] || die 'remote base drift'
# A failed push exits here. No pipeline, ignored error or subsequent create.
git push github "$head:refs/heads/$br"
[ "$(git ls-remote --refs github "refs/heads/$br")" = "$head"$'\t'"refs/heads/$br" ] || die 'remote head does not equal reviewed SHA'
[ "$(git ls-remote --refs github refs/heads/development)" = "$expected_base" ] || die 'remote base moved during push'
gh pr create -R "${SWEEP_REPO:-ligon/LSMS_Library}" --base development --head "$br" --title "$title" --body-file "$body"
