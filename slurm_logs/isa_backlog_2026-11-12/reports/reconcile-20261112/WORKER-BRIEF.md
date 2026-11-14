# Worker brief: reconcile one batch of open issues (READ-ONLY)

You are a fork of the coordinator's reconciliation agent. Same hard rules as the parent task, restated:

- READ-ONLY. No GitHub writes of any kind. No edits to tracked files. Git limited to
  log/show/diff/cherry/rev-list/merge-base/for-each-ref/ls-tree/grep. No checkout/switch/reset/
  stash/clean/branch/restore/merge/fetch/push/worktree. No `lsms_library` imports, no data builds,
  no `dvc`. Light CPU only: use `rg -j2`, avoid repo-wide `git log -p`, prefer `git log --grep`,
  `git log -S<literal> -- <path>`, `git show <sha>:<path>`.
- Checkout: `/local/job41201840/mirrors/LSMS_Library` at `e5fdd2fa831c4d03a247d264be7f339da459e6b0`
  (`development`). Shared mirror for other refs: `git -C /global/home/users/ligon/mirrors/LSMS_Library`.
- `gh` only with `-R ligon/LSMS_Library`, read-only (issue/pr view/list, search, api GET).

## Pre-materialised inputs (use these; do not re-fetch unless you need something not here)
SCRATCH=/local/job41201840/tmp/claude-43292/-local-job41201840-mirrors-LSMS-Library/7b5d2d16-e551-4441-b728-3fc1d13e3c43/scratchpad
- `$SCRATCH/issues/<N>.md` — full body and ALL comments of each open issue (fetched 2026-11-12 06:49 PST = 14:49Z).
- `$SCRATCH/timeline_summary.txt` / `timeline_summary.json` — per issue: cross-referencing PRs (with merge date or state), cross-referencing issues, title renames.
- `$SCRATCH/merged_prs.json` — 964 merged PRs (number, title, body, mergedAt, mergeCommit, baseRefName).
- `$SCRATCH/issue_refs.txt` — `#N` mentions in merged PR titles/bodies (UNDERCOUNTS: omnibus PRs #1687/#1690 do not cite every issue; prose like "open1571" defeats the regex).
- `$SCRATCH/gitlog_since0901.txt` — development commit subjects+bodies since 2026-09-01 (`@@@sha|date|subject`).
- `$SCRATCH/isa-decisions.org` — maintainer rulings (context; a "DECISION:" issue that has been ruled there is no longer needs-decision, but check the ruling's scope).
- Merges since the prior reconciliation base `16c3f4d9c` (the only merged work after 2026-11-10 09:51 PST):
  #1690 `1e549e96a` (omnibus: Burkina 2014 strata/mapping, Malawi sale basis + derivations, Mali/Niger/Ethiopia/Nigeria/Tanzania CONTENTS, Nigeria nonfood_expenditures + PH plot_features, Tanzania 449_OFFICIAL_BRIDGE.md, Niger 2014-15 plot_inputs.py, ledgers 1281/1288/1438/1457/1529/1573/1577/449/587/824/932, CI pr-data-validation);
  #1694 `7f1a9c954` (Malawi #1190 seed acquisition carrier in plot_inputs);
  #1695 `d5a2b1f9e` (Senegal 2021 household plot inputs, #1483);
  #1693 `e5fdd2fa8` (2018 strata for Benin/Togo/Guinea-Bissau/CotedIvoire, #1575; ledger 1531-benin-togo-own-instruments).
  Run `git -C /local/job41201840/mirrors/LSMS_Library show --stat <sha>` to see touched paths; match issues by PATH and COUNTRY, not only by number.
- Prior partial reconciliation (2026-11-10): `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/reports/merged-issue-reconciliation-20261110/` (context, not authority). Per-issue reports in sibling dirs under `.../isa-sm-20261103/reports/` (`ls | grep <N>`).

## Per issue
1. Read `$SCRATCH/issues/<N>.md` in full (skim long auto-generated tables, but read the most recent comments in full).
2. Country-scoped: read the relevant part of `lsms_library/countries/<C>/_/CONTENTS.org` (`rg -n -j2 '<N>|<keyword>'`), including LOGBOOK/WAITING/TODO entries. In `notes`, NAME the idiosyncrasy you found ("checked" is not an answer). A contradiction with CONTENTS.org is a result to report.
3. Find merged work: timeline PRs, `issue_refs.txt`, `rg`/`grep` on `gitlog_since0901.txt`, keyword search of `merged_prs.json`, path-based match against the four post-base merges, `.coder/ledger/<N>-*.md` on development.
4. Check the cited code/config on current development (`rg -n -j2`, `git show e5fdd2fa8:<path>`). Data claims you cannot check by reading source: write `unverified-data` in notes and give the command that would verify (do not run it).
5. Decide status: default `open`. `resolved` ONLY with a merge SHA that passes
   `git -C /local/job41201840/mirrors/LSMS_Library merge-base --is-ancestor <sha> e5fdd2fa8` (check `baseRefName` too) PLUS a path:line on development showing the fix (code or test). Absence of a repro is not evidence. `partial` = some acceptance items landed. `needs-decision` = waiting on a maintainer choice (Tier C). `needs-acquisition` = waiting on data (Tier D). `not-a-bug` only with a stated reason.
6. Issues narrowed on/after 2026-11-10 with no merged work touching them since: a one-line confirmation is enough (still check the four post-base merges by path).
7. Append ONE row (exactly 10 fields) with the helper — do this as soon as each issue is done:
   `python3 -I /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/reconcile-20261112/append_row.py N "title" "countries" TIER STATUS "evidence" ACTION EFFORT "depends_on" "notes"`
   TIER ∈ A/B/C/D (A cross-country assembly/index; B silent data loss/wiring; C design decision; D data acquisition). STATUS ∈ resolved/partial/open/needs-decision/needs-acquisition/not-a-bug. ACTION ∈ close/narrow/split/none/ask. EFFORT ∈ S/M/L. Evidence: PR#, 9+ char SHA, path:line.
8. Append the issue's section to YOUR parts file `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/reconcile-20261112/parts/REPORT-<BATCH>.md` (append with `cat >>` per issue, so partial progress survives):
   ```
   ### #N — <current title>
   Verdict: <status>/<action>, tier <T>. <2-5 lines: what landed (PR, sha, path:line), what remains, CONTENTS.org idiosyncrasy named.>
   <DRAFT, ready to paste, per the action:>
   - close: the closing comment citing PR/SHA and evidence, signed "-- Sue".
   - narrow: "New title:" + the FULL new body, starting with a "Resolved so far: PR #N (sha)" header, preserving everything that still applies; signed "-- Sue".
   - split: the residual issue's title/body/tier, plus the comment for the parent; both signed "-- Sue".
   - C items (needs-decision / ask): ~8 lines: Question / Options / Evidence / Recommendation. Never decide them.
   - none: one line why no write is needed (e.g. "narrowed 2026-11-10, no merged work since").
   QUEUE: <only for tractable Tier A/B items> #N | country | one-line fix sketch | files likely touched | fingerprint impact (corpus-wide / one country / none, per CLAUDE.md: country module or wave mapping.py => whole-country re-warm; grab_data/df_data_grabber/get_dataframe/_aggregate_wave_data => corpus-wide; _finalize_result/read-path/_EXCLUDED_CALLABLES, sibling .yml, transformations.py => none (transformations.py moves GhanaLSS nutrition); global categorical .org => corpus-wide) | verification needed
   ```
   No `@` mentions except `@ligon` when quoting a ruling. No respondent-level data.
9. Do not edit or reorder other batches' rows. If you find an issue in another batch is a duplicate of or resolved by something you saw, say so in your return message.

## Return message (<= 12 lines)
Counts by status and action; each close/split you drafted (issue + one-line reason); cross-batch observations; anything you could not finish (list issue numbers you did NOT append a row for).
