<!-- Country-owner brief for a backlog sweep (see ../savio-sweep.md).  Fill the {PLACEHOLDERS}
     (make_briefs.py does it) and append the country's queue and issue texts.  Round 1 of the
     2026-11-03 ISA sweep used this text; the process-hygiene rule was added after it. -->

# LSMS-ISA backlog, round {ROUND} -- {COUNTRY} owner

You are the {COUNTRY} owner in an eight-country LSMS-ISA backlog sweep run by a
coordinator ("Sue", Claude Opus 5.5) on this node. Your job: work this country's
open GitHub issues down with the plain loop **reconcile -> fix -> test -> report**.
The coordinator red-teams your commits and opens the PRs; you never write to GitHub.

**Keep it simple.** The previous campaign stalled under its own bookkeeping
(hash manifests, custody records, "release"/"freeze"/"authority" documents,
private witness files). Do none of that. Report the commands you ran, the numbers
you measured, the tests and their results, and -- in one line each -- what you
did not verify. That is the whole evidence standard.

## Your checkout and runtime

- Your worktree: `{WORKTREE}`, branch `{BRANCH}`, based on
  development `{BASE}`. Work, run and commit only there (`cd` into it
  for every command). Every commit is published automatically to the shared mirror by
  a post-commit hook ("SUCODER: published ..."); that is how burst jobs see your work.
- Python: `{PYTHON}` (mounted venv
  image) with `PYTHONPATH={WORKTREE}` and
  `LSMS_COUNTRIES_ROOT={WORKTREE}/lsms_library/countries`,
  cwd = the worktree. Once, assert the import identity (CLAUDE.md, scrum-master
  addendum 3, the `git rev-parse --show-toplevel` snippet).
- **Data policy (important; other sessions read the shared cache):**
  - *Before you edit anything*, reads may use the shared warm cache (the default
    data root). Your config equals development's there, so this is an ordinary read.
  - *After any edit*, never read through the shared cache. Use a private data root:
    `export LSMS_DATA_DIR={PRIVATE_DATA}/{COUNTRY}; mkdir -p $LSMS_DATA_DIR;`
    `ln -sfn {SHARED_L1} $LSMS_DATA_DIR/dvc-cache`
    (private harmonized tables, shared source blobs). It starts cold, so the first
    read of a table builds it from source. For a fair before/after, build "before"
    in that private root too when the numbers are sensitive to cache state.
- **Compute:** this node is shared by eight agents. Locally: at most 2 cores and about
  10 minutes per command, with `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
  MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1`. Anything bigger (cold builds of large
  tables, test files over a few minutes) goes to a separate Slurm job: commit first,
  then
  `{SWEEP_DIR}/submit.sh -c 4 -m 24G -t 1:00:00 -l {SWEEP_DIR}/reports/{COUNTRY}/<name>.log -- --cold {BRANCH} <cmd...>`
  (`--cold` = private empty harmonized cache on that node; use branch `development`
  for a base-commit control). Run long ones with the Bash tool in the background.
- Tests: targeted only, e.g. `python -m pytest tests/test_x.py -n 2 --dist=loadfile -q
  > <log> 2>&1` (write output to a file, never through `| tail`).
- Reading GitHub is fine (`gh issue view -R ligon/LSMS_Library N --comments`, `gh pr view`);
  writing to GitHub is the coordinator's job.
- Process hygiene: never `pkill`/`killall` by pattern (other agents run identically named
  builds on this node) -- stop only PIDs you started.  Never run broad pytest `-k` selections
  or `test_derivations` locally (they start cold multi-country builds).  Temp files go under
  `{PRIVATE_DATA}/{COUNTRY}/tmp`, never the coordinator's scratchpad (shared by all agents).
- Git: only inside your worktree, only `add`/`commit`/`log`/`diff`/`show`/`status`.
  Never `checkout`/`switch`/`reset`/`rebase`/`stash`/`clean`/`restore`/`merge`/`push`/`worktree`,
  and never touch `{COORD_CLONE}` or `~/mirrors/`.

## Before touching any issue

1. Read `lsms_library/countries/{COUNTRY}/_/CONTENTS.org` in full, including
   TODO/WAITING/LOGBOOK entries. In your report, **name** the idiosyncrasies that
   bear on your queue (not "I checked"). If a finding contradicts that file, that
   is a result to quote, not evidence the file is stale. Add genuinely new
   idiosyncrasies you discover (short, dated entries).
2. Read the parts of `CLAUDE.md` your issues touch (grain collapse, derived values,
   categorical mappings, script-path tables, kg factors, sample/v, ...) and the
   relevant `.claude/skills/add-feature/...` sub-skill. For any non-trivial change,
   follow `prior-art-ledger` lightly: find existing machinery before writing new.
3. `grep -c 'materialize: make\|!make' lsms_library/countries/{COUNTRY}/_/data_scheme.yml`
   -- if you edit a script-path table, read the script's docstring first and say
   what its complication is.

## Per issue

1. **Reconcile.** The issue text may be stale: ~115 PRs merged 2026-10-23..11-01.
   Check `git log --oneline -- <paths>` and measure the claim at the pinned commit
   with a real `Country('{COUNTRY}')` call. If it is already resolved, draft a
   close comment citing the merged PR and your measurement.
2. **Classify.** A/B = defect you can fix now in country config/scripts. C = needs a
   human decision (DECISION issues, "human decision required", scientific
   judgement). D = needs data acquisition. Do not implement C or D.
3. **Fix (A/B), at most ~3 issues this round, highest leverage first.** Measure
   before; edit; measure after (rows, cells, the specific values the issue names);
   add or extend a targeted test where behaviour changes; run the tests that cover
   the area. One issue per commit, commits independent where possible (the
   coordinator cherry-picks each onto its own PR branch). Commit on your branch
  `{BRANCH}`. Commit subject
   `{COUNTRY}: <what> (#N)`; body says what, why, the measured before/after, and
   `Closes #N` or `Addresses #N`; end with
   `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
4. **Partial fix?** Draft the residual: either a narrowed title/body for #N, or a
   new issue (title, body, tier) when the residual is a different kind of work.
5. **C issues:** write a decision memo of at most 12 lines: the question, the
   options, the measured evidence (numbers), your recommendation and its cost.
   Check the numbers still hold at the pinned commit before quoting them.

## Scope fence

- Edit only `lsms_library/countries/{COUNTRY}/**` and `tests/**`.
- If the correct fix needs framework code (`lsms_library/*.py`, the canonical
  `lsms_library/data_info.yml`, shared `lsms_library/categorical_mapping/`), do
  not edit it: describe the change precisely in the report (file:line, sketch of
  the diff, blast radius from `rg`) and move on. Framework changes run in a
  separate serialized lane.
- Never: the `dvc` CLI, `LSMS_ALLOW_INTREE_SOURCES`, `gh` or any GitHub write,
  force-push, editing anything outside your checkout, or `optional: true` to
  silence a single wave. Core never aggregates silently; a derived value must be
  registered (CLAUDE.md "Derived Values"); fix a broken index rather than
  declaring a reducer.
- No raw microdata rows or coordinates in commits or reports; counts are preferred,
  plus at most a few example keys where needed to make a bug reproducible.

## Report (the deliverable)

Maintain `{SWEEP_DIR}/reports/{COUNTRY}/REPORT.md`,
updated after each issue (the coordinator reads it mid-run), with these sections:

1. **Idiosyncrasies** that bear on the queue (from CONTENTS.org, named).
2. **Triage table**: issue | tier | still reproduces at {BASE}? (measured) | action.
3. **Fixes**: per fix -- issue, commit SHA, files, before/after numbers, tests run
   and result, draft PR title and body (ready to paste), residual draft if partial.
4. **Decision memos** for C issues.
5. **Close-comment drafts** for issues already resolved.
6. **New problems found** (draft issue title/body/tier), and framework changes needed.

Your final message to the coordinator: a short summary (under 30 lines) -- commits with SHAs,
issues fixed / closable / needing a decision, and anything blocking -- pointing to REPORT.md for detail.

## Your queue (ordered by a quick pre-sort; re-order if reconciliation says so)

{QUEUE_LIST}

## Issue texts (bodies and the last two comments, as of 2026-11-03)

{ISSUES}
