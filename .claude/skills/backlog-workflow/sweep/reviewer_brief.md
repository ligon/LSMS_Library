<!-- Red-team reviewer brief (see ../savio-sweep.md).  Give the reviewer the issue, the
     branch and the author's repro COMMAND -- not the author's conclusions.  Requested changes
     go back to the author as one new commit; the coordinator then re-checks or re-reviews. -->

# Red-team review: {COUNTRY} #{ISSUE}

You are an independent skeptic reviewing one candidate fix in the LSMS Library
before it becomes a PR. You did not write it. Decide whether it should ship.

- Issue: https://github.com/ligon/LSMS_Library/issues/{ISSUE} (read it with
  `gh issue view -R ligon/LSMS_Library {ISSUE} --comments`)
- Candidate branch: `{BRANCH}` (published in the shared mirror
  {MIRROR} and in the coordinator clone)
- Base: development `{BASE}`
- Author's repro command (verify it yourself; do not trust its conclusion):
  {REPRO}

## Do

1. Read `lsms_library/countries/{COUNTRY}/_/CONTENTS.org` (incl. TODO/WAITING/LOGBOOK)
   and the CLAUDE.md sections the diff touches. Name the idiosyncrasy that matters.
2. Read the full diff (`git diff {BASE}..{BRANCH}`).
3. Does it resolve the issue *as stated*, and only that? Look for: silent row loss
   or invention, grain changes, a declared index broken or a reducer added, values
   derived but not registered, a wave silenced with `optional: true`, other waves or
   tables of this country moved, tests that pin today's numbers rather than the rule.
4. Run the repro on base and on candidate yourself. Use a burst job, not this node:
   `{SWEEP_DIR}/submit.sh -c 4 -m 24G -t 1:00:00 -l <log> -- --cold <branch> <cmd...>`
   (`--cold` = private empty harmonized cache, shared source blobs; `development` for base).
   Also run the tests the diff adds or touches the same way.
5. Do not edit the branch, do not write to GitHub, no `dvc` CLI, never `pkill`/`killall`
   by pattern, and only read-only git (`log`, `diff`, `show`; do not fetch refs into the
   coordinator's clone).  Temp files under `{SWEEP_DIR}/reports/review-{ISSUE}/`.

## Report (short)

- Verdict: APPROVE / CHANGES (list each, concretely) / REJECT (why)
- What you ran and the numbers you got (base vs candidate)
- Collateral changes found (or "none found", saying what you checked)
- Anything the PR body should state that the author left out
