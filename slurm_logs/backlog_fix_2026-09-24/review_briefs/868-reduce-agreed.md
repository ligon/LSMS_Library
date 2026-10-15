# RED-TEAM review of branch `fix/868-reduce-agreed` (element `868-reduce-agreed`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/868-reduce-agreed` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Uganda,Togo built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

## Mandate: REFUTE
Your job is to find the reason this branch must NOT be merged. Default to `verdict: fail` where you are uncertain. A fix agent wrote this branch and its report; the report is a self-report and its "SCOPE DEVIATIONS: none" is a claim, not a proof. Check, by running, not by reading:
1. **Does it fix the issue?** Reproduce the issue's symptom on `origin/development` code (a second clone: `git worktree add /local/job$SLURM_JOB_ID/base origin/development` is allowed -- that is your own scratch, not the shared mirror) and show it gone on the branch, in your private root, cold where the change is a build-path change (`.venv/bin/python -m lsms_library.cli cache clear --country X` against YOUR root; NEVER against the live one).
2. **Do the claimed numbers reproduce?** Every before/after figure in the report and commit messages: re-measure and tabulate claimed vs measured. A number you cannot reproduce is a finding.
3. **Data loss / laundering.** Row counts, index uniqueness, `grain_reports()`, `null_index_reports()`, `null_read_reports()` on every touched table before vs after; any `find lsms_library/countries -name '*.parquet'` hit; any write outside the private root.
4. **Healthy countries intact.** For a framework change: the full suite under the verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest tests -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > /local/job$SLURM_JOB_ID/pytest.log 2>&1`, then read the file), and `bench/feature_audit/scan.py --features <touched feature> --countries <2-3 warm ones>` where a feature is touched. For a country change: that country's tests plus `tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py`.
5. **Scope and stop-list.** `git diff --stat origin/development..HEAD` versus the task's named files; any baseline/golden/blessed/lockfile/dependency edit; any weakened, skipped or xfailed assertion (a moved pin is acceptable ONLY with before/after numbers in the commit message -- check they are there and that they match your measurement).
6. **Are the new tests meaningful?** Would each new test fail on `origin/development`? Run them there (the worktree) and say which do not.
7. **CONTENTS.org**: did the agent name the idiosyncrasy, and is what it wrote true? Anything it claims contradicts CONTENTS.org -- verify against the raw data.
8. **Cache cost claim**: is the stated re-warm scope right? (A `_/*.org`, `_/*.py`, `data_scheme.yml`, wave-module edit moves that country; `get_dataframe`/`_normalize_dataframe_index`-reachable framework edits move the corpus.)

Environment: `PYTHONPATH` is your clone; use `PYTHONSAFEPATH=1` for `-c`/`-m` runs only. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`. `GH_REPO` is set; `gh` reads only, no writes. Read `lsms_library/countries/<C>/_/CONTENTS.org` for each country concerned.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- You commit NOTHING and push NOTHING: this is a review. `git status --short` must be empty at the end (delete any scratch you made inside the clone).
- Write the report to `$STAGE/VERDICT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.


## Verdict format (write to `$STAGE/VERDICT.md` AND repeat as your final message)
Begin with one line: `VERDICT: pass | pass-with-fixes | fail | needs-human` and one sentence why. Then:
1. **Refutations** -- each a claim from the report/commit, what you ran, what you got (a claim that survives is listed as "held", with the command).
2. **Claimed vs measured** table.
3. **Regression** -- suite/scan summary lines verbatim.
4. **Scope findings**.
5. **Residual risks and what a human must decide** (the branch may be right and still need a decision).
6. **Recommended follow-up commits** (concrete, with paths), if `pass-with-fixes`.

## The task the fix agent was given
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


## The branch: diffstat (vs development)
```
 lsms_library/build_transforms.py                   | 128 +++++-
 lsms_library/countries/Togo/_/CONTENTS.org         |  11 +
 .../countries/Uganda/2009-10/_/crop_production.py  |   7 +-
 .../countries/Uganda/2010-11/_/crop_production.py  |   7 +-
 .../countries/Uganda/2011-12/_/crop_production.py  |   7 +-
 .../countries/Uganda/2013-14/_/crop_production.py  |   7 +-
 .../countries/Uganda/2015-16/_/crop_production.py  |   7 +-
 .../countries/Uganda/2018-19/_/crop_production.py  |   7 +-
 .../countries/Uganda/2019-20/_/crop_production.py  |   7 +-
 lsms_library/countries/Uganda/_/CONTENTS.org       |  44 ++-
 lsms_library/countries/Uganda/_/crop_production.py |  16 +-
 lsms_library/countries/Uganda/_/uganda.py          |  10 +-
 lsms_library/country.py                            | 120 +++++-
 tests/test_gh868_reduce_to_agreed_ledger.py        | 434 +++++++++++++++++++++
 14 files changed, 792 insertions(+), 20 deletions(-)
```
## The branch: commit messages
--- dda202b51 gh868: reduce_to_agreed(on_conflict='na') files a cells_blanked grain report that survives the cache
`build_transforms.reduce_to_agreed` disclosed a blanking with warnings.warn()
and nothing else: no ledger entry, no `lsms_grain_audit` stamp, so the
GrainConflictWarning fired once on the cold build and every warm read was
silent -- grain_reports() empty, nothing for the audit harness to drain.
The exact failure GH #323 fixed for core's collapse, one site over.

The reducer now FILES a report of its own kind, `cells_blanked`, naming the
groups and the columns, with `destroyed` and `nan_key_rows` both ZERO (no row
is dropped), under the (country, table) key the cache writer stamps.  It is
filed ledger-only (`country._file_grain_report`, split out of
`_record_grain_report`), so the cold-path signal is unchanged: still the
reducer's own GrainConflictWarning, still GrainConflict under
LSMS_GRAIN_STRICT (pinned by test_gh323_explicit_reducers).  The warm replay
goes through core's one emit choke point (GrainCollapseWarning /
GrainCollapseError) with a `cells_blanked` branch in `_format_grain_report`
-- first, because with zero rows lost the existing branches would have
printed "SILENTLY NARROWED", and their "do NOT declare a reducer" tail is the
wrong advice for a report filed BY the sanctioned reducer.

Who is reducing what: a bare df_edit alias receives a frame and nothing else.
`Wave.grab_data` now sets `build_transforms.build_context(country, table,
wave)` (a ContextVar; runtime state, not folded by the fingerprint walk --
measured identical inside/outside a context) around both df_edit call sites;
explicit country=/table=/wave= kwargs on the reducer win; the wave falls back
to the frame's single `t`.

Three places read a wave parquet back instead of rebuilding it, and a report
filed INSIDE a wave build exists in no other frame, so all three replay the
wave stamp (`_replay_wave_stamp`, wave filled only where the report has
none): the YAML L2-wave hit and the script-path read in grab_data, and
run_make_target's read.  The YAML L2-wave write stamps this wave's in-build
reports (`_wave_grain_audit`: this wave, `_WAVE_BUILD_SITES`, never a
from_cache copy) -- without it a country rebuilt from warm wave parquets (the
#891 union rebuild, where the hook never runs) wrote an empty country stamp.

Uganda, the only 'na' caller: crop_production_for_wave runs out of process
(make -> wave script) and passes country/table/wave explicitly; each of the
seven wave scripts hands its ledger to to_parquet(grain_audit=
grain_reports('Uganda', 'crop_production')); the country concatenator folds
the wave stamps for a reader of ITS output (LSMS_BUILD_BACKEND=make).  The
cluster_features hook is left bare and reached through the framework
context.  Togo's alias keeps on_conflict='raise': nothing filed, nothing
changed.  What the reducer RETURNS is unchanged everywhere; no allowlist.

Verified on fixtures only (tests/test_gh868_reduce_to_agreed_ledger.py, 24
tests incl. an end-to-end grab_data cold -> stamp -> L2-wave hit -> replay on
a synthetic country, and static checks on the seven Uganda scripts);
tests/test_gh323_explicit_reducers.py + test_grain_collapse*.py +
test_gh323_togo_cluster_features.py etc.: 313 passed unchanged.

Cache cost, MEASURED and NOT PAID: corpus-wide cold re-warm, not Uganda +
Togo.  `_format_grain_report` / `_record_grain_report` / `_replay_grain_audit`
sit in every table's build fingerprint (reached from the tagged
`_normalize_dataframe_index`), so every probed `_table_cache_hash` moved
(Uganda x4, Togo x2, Malawi household_roster, Ethiopia sample) and
`build_transforms_fingerprint('household_roster')` moved.  Uganda's
`framework_imports_fingerprint` also moved, which settles the brief's
UNVERIFIED question: the in-function `from lsms_library.build_transforms
import reduce_to_agreed` at uganda.py IS reached (ast.walk covers any scope).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


## The fix agent's report (a self-report -- refute it)
Both background tasks (the wider test run and the push to the mirror) are still in flight. There's nothing independent left to request until they report back, so I'm waiting on their notifications before the final advisor review.
