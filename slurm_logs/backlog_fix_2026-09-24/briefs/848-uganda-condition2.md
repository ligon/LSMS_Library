# Backlog fix element `848-uganda-condition2` -- dispatch-plan step 6: issue #848

You are a worker agent in a Slurm job with its OWN clone of `development` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) into which Uganda's warm parquets were copied; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else, and the live cache must not be touched.

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
- `PYTHONPATH` is your clone. For `python -c` / `-m` runs add `PYTHONSAFEPATH=1` so the clone is the imported package; do NOT export `PYTHONSAFEPATH` into `make`/wave-script subprocesses (338 wave scripts import a sibling by bare name; commit 6df53a5c6). Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`.
- Cores: `$SLURM_CPUS_ON_NODE` (8). Test recipe, verbatim: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <targets> -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > <logfile> 2>&1` -- write pytest output to a file, never through a pipe. Never `-n auto`.
- A rebuild of one of your countries' tables is allowed and lands only in your private root. `find lsms_library/countries -name '*.parquet'` must stay empty (GH #803).
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/848-uganda-condition2` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Country rule
BEFORE editing anything, read `lsms_library/countries/Uganda/_/CONTENTS.org` -- prose AND the LOGBOOK/WAITING/TODO entries -- and NAME, in your report, the idiosyncrasy that bears on this task (the 2018-20 WIDE-vintage CROP_COLMAPS and the condition-slot design). A finding that contradicts CONTENTS.org is a result to quote, not a licence to assume the file is stale. crop_production is a script-path table: read the wave scripts' docstrings first.

## Scope note
The #962 draft that also edits uganda.py has been parked on `fix/962-value-count-draft`; `development` is clean, and you must not touch the food_acquired code paths in uganda.py. The #647 part-harvest question (`_11`/`_21` slots) has NO verdict: do NOT wire those slots; leave the one-sentence documentation as is. A Uganda crop_production re-warm in your private root IS in scope for verification (it is the point of the task).

## The task (from the triage's dispatch plan)
**What:** Uganda: add a second `conditions` entry to CROP_COLMAPS['2018-19']['B'] (uganda.py:2037-2041) and ['2019-20']['B'] (:2046-2050), harvest + sold sides + kg_factor s5bq06d_2; unit-vs-condition by value-label vocabulary (uganda.py:1703). If @ligon's #647 questionnaire verdict is 'include', add the `_11`/`_21` family in the same PR; if 'exclude', one sentence at uganda.py:1846.

**Verify:** Before/after: +1,436 (2018-19) / +1,307 (2019-20) rows, 14 / 3 additive collisions, harvest_kg delta reported; move tests/test_uganda_crop_condition.py:59 baseline (133,683) and the CONTENTS.org:2076 TODO. Cost: full Uganda re-warm (uganda.py in every Uganda table hash).

**Blocked by (as triaged):** step 1 (park #962 draft); #647 verdict optional. -- the #962 park has been done.

## Triage record
### #848 -- tier B, auto, dedup open
- country: Uganda
- approach: Add a second `conditions` entry to CROP_COLMAPS['2018-19']['B'] and ['2019-20']['B'] (harvest + sold sides, plus kg_factor s5bq06d_2 for 2018-19), deciding unit-vs-condition columns by value-label vocabulary per uganda.py:1703; before/after: +1,436/+1,307 rows, 14/3 additive collisions, harvest_kg delta, then move the CONTENTS.org TODO and test baseline; full Uganda re-warm (uganda.py is in every Uganda table hash).
- verify cost: one-country-rebuild
- CONTENTS.org: CONTENTS.org:1297-1314 'The 5B second slot IS a condition slot, not a season slot -- the old note was wrong … Still not wired: it ADDS ~1 435 rows, which is a mass change requiring its own before/after'; CONTENTS.org:2076-2091 TODO 'Second condition slot dropped for 2019-20 season B … 1 306 non-null conditions'; CONTENTS.org:1318-1321 mixed naming vintages within one file (slot 1 legacy a5bq6{b,c,d}, slot 2 WB s5bq06{b,c,d}_2)
- evidence: lsms_library/countries/Uganda/_/uganda.py:2037-2041, :2046-2050, :2029-2036, :1703-1708 (value-label rule), :1846-1851; raw L1 blobs 62f4e464… (2018-19 AGSEC5B, 84 cols) and 5a62f8e7… (2019-20 agsec5b, 78 cols) column-name + collision probe 2026-09-24; commits b28b50242 (#323), 7a4279da0 (#842); merged_prs.json/open_prs.json: no PR references #848; tests/test_uganda_crop_condition.py:59 (133,683-row baseline that will move)
- summary: Uganda's 2018-20 WIDE-vintage colmaps read one `conditions` slot where the source ships two. At HEAD `CROP_COLMAPS['2018-19']['B']` (uganda.py:2037-2041) and `['2019-20']['B']` (uganda.py:2046-2050) wire slot 1 only, while `['2019-20']['A']` (uganda.py:2029-2036) already wires both slots (commit b28b50242, #323). Raw column probe of the L1 blobs confirms the unread slots: 2018-19 AGSEC5B ships `s5bq06{a..f}_2` + `s5bq07{a,b,c}_2` (1,436 non-null quantities), 2019-20 agsec5b ships the same family labelled '(2019, condition2)' (1,307 non-null) -- a second cell the issue only asked to check and which has no issue number of its own, only `CONTENTS.org:2076` TODO; 2018-19 AGSEC5A has NO `_2` slot (33 columns). So the fix is exactly two cells, harvest and sold sides each. Collision count measured on the raw files (the 'first experiment' CONTENTS.org:2639-2646 asks for): slot-2 records sharing BOTH unit and condition with slot 1 -- 14 of 1,436 (2018-19), 3 of 1,307 (2019-20) -- would be summed by the additive collapse; the rest land as new `condition` rows (slot-2 modal condition 45 vs slot-1 modal 24). That also means slot 2 cannot explain the pre-existing 433 duplicate groups, since it is not read today. Not touched by any merged/open PR; the uncommitted uganda.py diff in the working tree is #962 (food units), unrelated.

Read the full issue and its comments first: `gh issue view 848 --comments`.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
