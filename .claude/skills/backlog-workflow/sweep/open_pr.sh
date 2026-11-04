#!/bin/bash
# open_pr.sh <branch> <title> <body-file>  -- push a prepared branch to GitHub and open the PR.
# The body file should carry the reviewer's must-state items (see ../savio-sweep.md).
set -euo pipefail
br=$1 title=$2 body=$3
cd "${SWEEP_PR_DIR:-${SUCODER_LOCAL_ROOT:?set SWEEP_PR_DIR}/prs}/${br//\//_}"
git push -q github "$br" 2>&1 | grep -v '^remote:' || true
gh pr create -R "${SWEEP_REPO:-ligon/LSMS_Library}" --base development --head "$br" --title "$title" --body-file "$body"
