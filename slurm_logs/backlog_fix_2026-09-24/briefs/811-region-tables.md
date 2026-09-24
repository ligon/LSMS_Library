# Backlog fix element `811-region-tables` -- dispatch-plan step 5: issues #811

You are a worker agent in a Slurm job with its OWN clone of `development` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) into which the warm parquets of your countries were copied; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else, and the live cache must not be touched.

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/811-region-tables` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Country rule
BEFORE editing anything, read `lsms_library/countries/Guinea-Bissau/_/CONTENTS.org`, `lsms_library/countries/Burkina_Faso/_/CONTENTS.org`, `lsms_library/countries/Senegal/_/CONTENTS.org` -- prose AND the LOGBOOK/WAITING/TODO entries -- and NAME, in your report, the idiosyncrasy that bears on this task. A finding that contradicts CONTENTS.org is a result to quote, not a licence to assume the file is stale. For any script-path table (`materialize: make`) also read the script's docstring first.

## Scope note
Guinea-Bissau is the required deliverable; Burkina_Faso and Senegal siblings only if the same mechanical shape applies (say so per country).

## The task (from the triage's dispatch plan)
**What:** Guinea-Bissau: add `#+name: Region` table to _/categorical_mapping.org with the strata table's rows verbatim (sab -> SAB, BOLAMA_BIJAGOS -> Bolama-Bijagos), mirroring efe0199a3. Burkina_Faso (new table 22 -> 13) and Senegal (case-normalise 14) as the sibling issue from step 0.

**Verify:** Served cluster_features.Region vocabulary == served sample.strata vocabulary (9 == 9); tests/test_guinea_bissau_cluster_features.py passes. Cost: one Guinea-Bissau re-warm (one wave, small) -- NOT cache-free.

**Blocked by (as triaged):** nothing

## Triage records for the issues in scope

### #811 -- tier A, auto, dedup open
- country: Guinea-Bissau (siblings measured: Burkina_Faso, Niger, Senegal)
- approach: Add a `#+name: Region` table to Guinea-Bissau/_/categorical_mapping.org with the strata table's rows verbatim (BOLAMA_BIJAGOS -> Bolama-Bijagos, sab -> SAB, lower-case -> title case), mirroring efe0199a3; verify served cluster_features.Region == served sample.strata vocabulary (9 == 9) and that tests/test_guinea_bissau_cluster_features.py still passes; budget the Guinea-Bissau re-warm. Then file or widen to the measured siblings: Burkina_Faso (new table, 22 -> 13), Niger (add lower-case rows to the existing table, 16 -> 8), Senegal (case-normalise, 14 -> 14 title case).
- verify cost: one-country-rebuild
- CONTENTS.org: Guinea-Bissau/_/CONTENTS.org:408-440 ('Cluster Features -- the many-to-one projection (GH #323)'): Region (s00q01) and Rural (s00q04) are 'PERFECTLY CONSTANT WITHIN GRAPPE at source', so re-spelling is safe for the collapse; :139-150 ('SAB is urban-only by design') names SAB as the one region a pooled analysis trips over, which is also the label whose case differs most ('sab' vs 'SAB'); :276-300 leaves 'one vague per grappe, or two?' unresolved -- unrelated to spelling but worth knowing before touching the cluster table. No entry mentions the Region/strata split.
- evidence: lsms_library/countries/Guinea-Bissau/_/categorical_mapping.org:34-44 (strata table, Alternate Spelling -> Preferred Label; no Region table: grep -il '^#+name: *region$' lists Ethiopia, Malawi, Peru, Tanzania, Mali, Nigeria, Niger, GhanaLSS only); Guinea-Bissau/2018-19/_/data_info.yml:16 (strata: s00q01) and :48 (Region: s00q01); raw parquets Guinea-Bissau/var/cluster_features.parquet and var/sample.parquet both hold {'sab','bafata','oio','gabu','cacheu','tombali','quinara','biombo','BOLAMA_BIJAGOS'}; served via Country(c, assume_cache_fresh=True): GB Region raw, GB strata normalised, Niger Region 16 labels, Burkina_Faso 22, Senegal 14; country.py:2885-2894 (_apply_categorical_mappings: table name matched to column name case-insensitively, applied when a Preferred Label column exists); precedent commit efe0199a3 (#832, in merged PR #949); CLAUDE.md cache section lists categorical_mapping.org among hashed `_/*.org` build inputs; open PR #955 (#953, mapping resolution model) is docs+tests and does not change by-name resolution; gh: issue OPEN, 0 comments.
- summary: Confirmed at HEAD on served values: Guinea-Bissau cluster_features.Region is served raw ('sab', 'bafata', ..., 'BOLAMA_BIJAGOS') while sample.strata is served normalised ('SAB', 'Bafata', ..., 'Bolama-Bijagos'), because _/categorical_mapping.org has carried a `#+NAME: strata` table since bc70d3af0 (2026-04-09) and has no `Region` table; both columns read the same source variable s00q01 (2018-19/_/data_info.yml:16,48) and the raw parquet holds identical lower-case values in both. Nothing merged or open addresses it, but the identical fix landed for Ethiopia under #832 (efe0199a3, PR #949, 2026-09-18: a `#+name: Region` table read off the same file's `strata` table). Two corrections to the issue: the fix is NOT cache-free (categorical_mapping.org is among the `_/*.org` build inputs folded into Wave._input_hash per CLAUDE.md's cache section, so every Guinea-Bissau table re-warms -- one wave, small), and the canonical spelling should mirror the existing strata table ('SAB' upper-case, not title case) or the strata<->Region join it wants still misses. The sweep the issue asks for, done on SERVED values: Burkina_Faso 22 labels for 13 regions (no table), Niger 16 for 8 (its Region table at categorical_mapping.org:230-242 lacks the lower-case variants), Senegal 14 with 'Saint-Louis' beside 13 lower-case names (no table); Benin, Togo, CotedIvoire clean.

Read the full issue and its comments first: `gh issue view 811 --comments`.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
