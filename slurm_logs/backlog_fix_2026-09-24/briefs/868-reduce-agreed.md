# Backlog fix element `868-reduce-agreed` -- dispatch-plan step 3: issues #868

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/868-reduce-agreed` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Country rule
BEFORE editing anything, read `lsms_library/countries/Uganda/_/CONTENTS.org`, `lsms_library/countries/Togo/_/CONTENTS.org` -- prose AND the LOGBOOK/WAITING/TODO entries -- and NAME, in your report, the idiosyncrasy that bears on this task. A finding that contradicts CONTENTS.org is a result to quote, not a licence to assume the file is stale. For any script-path table (`materialize: make`) also read the script's docstring first.

## Scope note
Fixture-level verification; a Uganda/Togo re-warm is NOT required here -- report the cost, do not pay it.

## The task (from the triage's dispatch plan)
**What:** Route reduce_to_agreed(on_conflict='na') conflicts through _record_grain_report with a distinct report kind (cells_blanked, naming groups+columns) so test_uganda_323_grain.py:258's destroyed/nan_key filter is untouched; inject country/table/wave at the framework boundary; for the make-run path make wave-script to_parquet(grain_audit=) stamps fold into the country stamp at country.py:4600-4632. Must precede any recommendation of the Uganda pattern to Tanzania/Ethiopia in #837.

**Verify:** Fixture test: conflicting frame through reduce_to_agreed(on_conflict='na'), write/re-read parquet, assert grain_reports() non-empty and replay fires. Cost: fixture only; budget a Uganda + Togo re-warm because whether the in-function imports at uganda.py:201/1696 reach framework_imports_fingerprint is UNVERIFIED.

**Blocked by (as triaged):** nothing

## Triage records for the issues in scope

### #868 -- tier B, auto, dedup open
- country: framework (callers: Uganda; Togo)
- approach: Route the 'na' branch through the same ledger: either intercept GrainConflictWarning at the framework boundary where the df_edit hook / grab_data runs and convert it to `_record_grain_report` with a distinct report kind (e.g. cells_blanked, naming groups+columns) so test_uganda_323_grain.py:258's destroyed/nan_key filter is untouched, or add country=/table=/wave= kwargs the framework injects; honour `_grain_strict` (already does). For the make-run path (Uganda crop_production) the wave script must pass grain_audit= to to_parquet AND the consolidation at country.py:4600-4632 must fold wave-parquet stamps into the country stamp (absent today). Fixture test: build a conflicting frame through reduce_to_agreed(on_conflict='na'), write/re-read the parquet, assert the report replays and grain_reports() is non-empty. No allowlist; no change to what the reducer returns.
- verify cost: one-country-rebuild
- CONTENTS.org: Uganda/_/CONTENTS.org:2653-2657: "*the =GrainConflictWarning= is a COLD-BUILD signal only*: =reduce_to_agreed= files no grain report and nothing stamps it into the wave parquet, so a warm read says nothing about the 11 groups. ... it is a property of =reduce_to_agreed= itself -- Uganda's =cluster_features= hook has the same gap". Consistent with the issue; also records the 11-of-433 disagreeing crop_production groups as a MISSING LEVEL (#848 condition-2 slot), which is the data question the issue separates out.
- evidence: lsms_library/build_transforms.py:422-511 (warn only, no report); lsms_library/country.py:6356-6373 (_GRAIN_LEDGER/_record_grain_report), :4584 and :4632 (stamp from ledger), :4511 and :4672 (replay reads country cache_path only); callers: lsms_library/countries/Uganda/_/uganda.py:201-226 and :1696-1698, Togo/2018/_/mapping.py:88; tests/test_uganda_323_grain.py:258-266 filters reports on destroyed/nan_key_rows; tests/test_build_transform_hash.py:36-42 says the helper is deliberately untagged and versioned via framework_imports_fingerprint -- whether the in-function imports at uganda.py:201/1696 are caught is UNVERIFIED, so budget a Uganda (and Togo) re-warm rather than asserting none.
- summary: Confirmed at HEAD: `reduce_to_agreed` (build_transforms.py:422) on a conflict with on_conflict='na' only calls warnings.warn (lines 498-509); it never calls `_record_grain_report`, so `_GRAIN_LEDGER` (country.py:6356) is empty for it and the L2-country stamp written at country.py:4584/4632 carries nothing -- the warm replay at 4511/4672 has nothing to replay. Scope correction: at HEAD `collapse_to_cluster_grain` is reached only by Togo (Togo/2018/_/mapping.py:88) with the default 'raise', which is loud by construction; the silent gap is Uganda's two on_conflict='na' sites -- the in-process cluster_features df_edit hook (uganda.py:226) and the out-of-process crop_production de-dup (uganda.py:1697, run by make via Uganda/_/Makefile:106-107). Two wrinkles the issue does not name: the reducer receives a bare frame with no country/table/wave context, and for the make-run path the child's `_GRAIN_LEDGER` is a different process, so a wave-script `to_parquet(grain_audit=...)` stamp would still never reach the country parquet because `read_parquet_grain_audit` is only ever called on the country cache_path (country.py:4511, 4672) -- wave-stamp propagation is absent. No merged or open PR touches it (merged_prs.json / open_prs.json: no #868 ref; `git log -S'_record_grain_report' -- lsms_library/build_transforms.py` is empty).

Read the full issue and its comments first: `gh issue view 868 --comments`.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
