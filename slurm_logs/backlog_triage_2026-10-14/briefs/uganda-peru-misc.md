# Backlog TRIAGE element `uganda-peru-misc` -- 13 open issues, READ-ONLY

You are a triage agent in a Slurm job with your OWN clone of `development` (your cwd, a private throwaway), your OWN mounted venv (`.venv/bin/python`) and a PRIVATE, EMPTY `LSMS_DATA_DIR` (env; `dvc-cache` inside it is a read-only symlink to the shared blob store). The live warm cache at `/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library` may be READ with `pd.read_parquet` / `pyarrow` directly for evidence (row counts, schema metadata, `lsms_cache_hash`), never through a `Country()` call pointed at it (that can write). A `Country()`/`Feature()` call in your private root is allowed when static evidence is not enough; budget it (it is a cold build) and say you did.

## This is a READ-ONLY task
You produce a report and a JSON record per issue. You do NOT fix anything, do NOT commit, do NOT push, do NOT create branches, do NOT run any `gh` WRITE (comment/close/label/pr). `gh` READS are fine and expected (`gh issue view N --comments`, `gh pr view`, `gh pr list --state merged --search`). You may write files only under `$STAGE` (env).

## Guardrails
**Out of scope -- STOP and report if the task would require these**:
- Modifying any file under `tests/fixtures/`, `.coder/coverage/`, or any file whose name contains `baseline`, `golden`, `expected`, `snapshot` or `blessed`
- Modifying `pyproject.toml`, `poetry.lock`, `requirements*.txt`, `dvc.lock`, any `.dvc` sidecar, or any dependency pin
- Weakening, skipping, or xfailing any existing test assertion (moving a pinned baseline the fix legitimately moves is allowed ONLY with the before/after numbers in the commit message)
- Running any "regenerate baseline"/"update snapshot"/coverage-matrix script, or `lsms-library cache clear` against any root other than your private `LSMS_DATA_DIR`
- Any `gh` WRITE (issue close/comment/label, pr create/merge). `gh` READS are fine (`GH_REPO` is set).
- Any git push to a branch other than your own `fix/<key>` on `origin` (the shared mirror); never push to `development`; never force-push; never run checkout/reset/stash/clean on any checkout but your own clone
- Touching files outside the paths the task names, and any DVC CLI invocation (read via `get_dataframe` / `get_data_file` only)
If any of these become necessary, STOP, leave the clone in a committable state, and put the conflict at the top of your report.


## Environment facts
- `PYTHONPATH` is your clone; `PYTHONSAFEPATH=1` for `python -c`/`-m`. Cores: `$SLURM_CPUS_ON_NODE`; BLAS threads are pinned to 1.
- Read `CLAUDE.md` first (long; skim the section headings and read the ones your issues touch), then `.claude/skills/backlog-workflow/SKILL.md` (the tier definitions you must use), then each country's `_/CONTENTS.org` for any issue scoped to a country -- and NAME the idiosyncrasy you found in that issue's record.
- The repo's default branch is `master`, and PRs merge into `development`, so **"Closes #N" in a merged PR never auto-closed anything**. Reconciliation is therefore the cheapest win you can hand back: for every issue, search merged PRs (`$STAGE/../merged_prs_since_0918.tsv` is a head start: PR, date, title, issue refs; also `git log --oneline -S'#N' development`) and VERIFY by reading the diff that the fix actually landed before you say so.
- The previous triage (2026-09-24, 31 issues) is at `slurm_logs/backlog_triage_2026-09-24/backlog_report.md` -- same record schema, and its "root_cluster" names are reusable; your issues were NOT in it. The fix phase that followed is summarised in `slurm_logs/backlog_fix_2026-09-24/REVIEW_SUMMARY.org`; six follow-up branches and PRs #967, #969, #971-#973 are in flight today -- check whether any of your issues is touched by them.

## Your issues
- #960 -- The venv documents describe a world with one node and one checkout: four statements that are now wrong (/local
- #942 -- v is wave-scoped in some countries: the same cluster gets a new id each round, with no crosswalk
- #939 -- Check whether partition ranking is stable in min_goods before relying on the aggregation selection
- #894 -- panel_ids is persisted two different ways: committed JSON (9 countries) vs cache parquet (Malawi, Nigeria)
- #880 -- GhanaSPS/_/CONTENTS.org still documents wave-1 `MonthsAway` as "not wired" and "filling 0 would fabricate data
- #849 -- Uganda: carry the survey-reported KgFactor (a5?q6d) in all seven waves, not only 2018-19, once harvest_kg pref
- #793 -- Record how each sample was DRAWN: a `sampling.yml` sibling of `population.yml`
- #791 -- Add UEMOA PHMECV as a source: 4 missing EHCVM 2021-22 waves + 8 ERI-ESI studies (access is human-reviewed)
- #772 -- Uganda/earnings delivers 'earnings' where the scheme declares 'Earnings' (plus a stray level_1) — two test fai
- #750 -- Reachability sweep of .dvc sidecars: ~21 genuine misses (was: a much larger figure, since corrected)
- #702 -- Afghanistan: `_/` config exists but declares no `Data Scheme:` — two waves of data unreachable
- #701 -- Peru: wire ENNIV 1991 — the closest of the three unwired waves to a quick win
- #700 -- Peru: wire `sample` (v, Rural) for 1994 — and settle the v/i padding inconsistency

## Per-issue record (JSON, one object per issue, array in `$STAGE/records.json`)
Fields, all required:
- `number`, `title`
- `tier`: A (cross-country assembly/index) | B (silent data loss / wiring; diagnose-then-fix) | C (enhancement / design; human decides) | D (acquisition / infra / needs a human or credentials)
- `type`: bug | data-gap | framework | docs | test | perf | infra | design
- `tractability`: auto | diagnose-then-auto | human | blocked
- `dedup_status`: open | resolved-in-development (cite the verified commit/PR) | partial (say what landed, what remains) | duplicate-of #N | superseded
- `priority` 1-5 (1 = silent data loss in served numbers, 5 = cosmetic), with one sentence of why
- `summary`: what the issue actually is, in your own words, after reading the code -- including whether the issue's claim still holds on today's `development` (quote line numbers)
- `evidence`: `path:line` references and the commands you ran
- `root_cluster`: a shared-root-cause key (reuse 09-24 names where they fit; invent sparingly)
- `approach`: the concrete fix an auto agent would make (files, functions), or "n/a: human decision -- <the question>"
- `verify`: how a fix is verified (tests, cold build of which country, scan.py) and its CACHE COST (which tables re-warm; cite the fingerprint rule from CLAUDE.md)
- `contents_org`: the idiosyncrasy found in the country's CONTENTS.org, or "n/a (framework)"
- `blocked_by`: issue/PR numbers or decisions
- `decision_for_human`: the one question, if any, phrased so it can be answered yes/no or A/B

## Report (`$STAGE/REPORT.md`, also your final message)
1. A table: number | tier | type | tractability | dedup_status | priority | root_cluster | one-line approach.
2. **Reconcile**: the issues that can be CLOSED now (verified resolved) and the ones that need a partial comment, each with the verified SHA/PR.
3. **Dispatchable now** (tier A/B, auto, no decision needed): ordered by leverage, roots before symptoms, with the cache cost per step.
4. **Escalations**: the decisions, one line each.
5. Anything that contradicts `CONTENTS.org` or `CLAUDE.md` -- quote it.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.


