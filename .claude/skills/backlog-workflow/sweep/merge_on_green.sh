#!/bin/bash
# merge_on_green.sh <pr>...  -- ONLY with the maintainer's explicit merge authorization.
#
# Polls each PR; merges it (merge commit, pinned to the head it saw: --match-head-commit) once
# every non-skipped check has succeeded and GitHub reports it CLEAN.  Never squashes, rebases,
# force-pushes or deletes branches.  A failed check, a conflict or a refused merge is printed
# as "needs coordinator" and that PR is left alone.  One line per state change; suitable as a
# Claude Code Monitor command.
R=${SWEEP_REPO:-ligon/LSMS_Library}; declare -A last; pending="$*"
while [ -n "$pending" ]; do
  next=""
  for n in $pending; do
    j=$(gh pr view -R "$R" "$n" --json state,headRefOid,mergeable,mergeStateStatus,statusCheckRollup 2>/dev/null) || { next="$next $n"; continue; }
    st=$(jq -r .state <<<"$j"); head=$(jq -r .headRefOid <<<"$j"); ms=$(jq -r .mergeStateStatus <<<"$j"); mg=$(jq -r .mergeable <<<"$j")
    fail=$(jq -r '[.statusCheckRollup[]? | select((.conclusion//"")|test("FAILURE|CANCELLED|TIMED_OUT|ACTION_REQUIRED"))|.name]|join(",")' <<<"$j")
    pend=$(jq -r '[.statusCheckRollup[]? | select((.conclusion//"")=="")|.name]|join(",")' <<<"$j")
    if [ "$st" = MERGED ]; then echo "PR $n already MERGED"; continue; fi
    if [ -n "$fail" ]; then s="FAILED:$fail"; elif [ "$mg" = CONFLICTING ]; then s=CONFLICTING
    elif [ -n "$pend" ]; then s="pending:$pend"; else s="$ms"; fi
    if [ "$s" = CLEAN ]; then
      if gh pr merge -R "$R" "$n" --merge --match-head-commit "$head" >/dev/null 2>&1; then
        echo "PR $n MERGED $(gh pr view -R "$R" "$n" --json mergeCommit --jq .mergeCommit.oid | cut -c1-9) (head ${head:0:9})"; continue
      else s="merge-refused($ms)"; fi
    fi
    if [ "${last[$n]:-}" != "$s" ]; then last[$n]=$s; case $s in pending*) ;; *) echo "PR $n $s";; esac; fi
    case $s in FAILED*|CONFLICTING|merge-refused*) echo "PR $n needs coordinator: $s"; continue;; esac
    next="$next $n"
  done
  pending=$next; [ -n "$pending" ] && sleep 60
done
echo "merge_on_green: done"
