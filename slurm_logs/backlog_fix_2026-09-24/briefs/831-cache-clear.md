# Backlog fix element `831-cache-clear` -- dispatch-plan step 12: issues #831

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
- Commit as you go with `git commit` (author is preconfigured); end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/831-cache-clear` (origin is the shared mirror; there is no GitHub push from here and no PR is opened -- the human reviews the branch).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org` (that is what the file is for).

## Scope note
Mechanical half only (cache clear listing in-tree source files with/without sidecars). The read-policy half is an open escalation; do not change what get_dataframe serves.

## The task (from the triage's dispatch plan)
**What:** Extend `cache clear` (cli.py:228-240) to list in-tree .dta/.sav/.csv with and without sidecars. The read-policy half (refuse / warn-but-serve sidecar-less in-tree files, local_tools.py:1195-1214) waits on the escalation.

**Verify:** Unit test on a tmp countries_root with a sidecar-less .dta; hash-neutral (no cli/data_access symbol in a build fingerprint, per #917). Cost: nil.

**Blocked by (as triaged):** read-policy decision for the other half.

## Triage records for the issues in scope

### #831 -- tier B, human, dedup partial
- country: framework
- approach: Decision needed from @ligon: may a sidecar-less data file inside countries_root() be READ at all? Refusing it (as #803 does for parquets) closes the shadowing hole but breaks the add_wave workflow, where a maintainer develops wave config against files extracted before `dvc add`. A middle path: warn (InTreeDataWarning) but serve, and extend `cache clear` to list in-tree .dta/.sav/.csv with and without sidecars -- that half is mechanical and hash-neutral (no data_access/cli symbol is in a build fingerprint, per #917).
- verify cost: none
- CONTENTS.org: n/a
- evidence: lsms_library/data_access.py:84-120 (_fetch_destination); lsms_library/local_tools.py:1195-1214 (sidecar-less in-tree file accepted); lsms_library/cli.py:228-240 (parquet-only stray report); PR #917 body 'Not fixed here'; PR #922; merged_prs.json 917/922/918 closes lists; find lsms_library/countries -name '*.parquet' -> 0; ls -d lsms_library/countries/[0-9]* -> none; git ls-files shows Panama/1997/Data/unittable.csv tracked
- summary: The write side is fixed: PR #917 (6e4dc5b68, merged 2026-09-14) routes every read-path fetch through data_access._fetch_destination (data_access.py:84-120), which lands under data_root() unless populate_cache=True (the add_wave/populate_and_push acquisition branch), with a static guard and tests/test_gh831_no_runtime_writes_into_config_tree.py; PR #922 (575413801) added the sidecar fast path so get_data_file no longer walks the DVC index. At HEAD the clone has 0 stray countries/<wave>/ dirs and 0 in-tree parquets; the one in-tree data file (Panama/1997/Data/unittable.csv) is git-tracked, not a stray. Two of the issue's three bullets remain, and #917's body names them under 'Not fixed here': (a) get_dataframe still serves a sidecar-less file inside countries_root() -- local_tools.py:1195-1214 deliberately allows it ('new data being prepped for dvc add, user scratch data'); (b) `cache clear` reports only in-tree parquets (cli.py:228-240), not .dta/.sav. Severity has dropped because the runtime no longer creates the dangerous class, but the reader behaviour that let a stray shadow the DVC blob is unchanged.

Read the full issue and its comments first: `gh issue view 831 --comments`.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.
