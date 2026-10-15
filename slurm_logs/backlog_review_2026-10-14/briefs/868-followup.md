# RED-TEAM review of branch `fix/868-followup` @ 9dfb83394 (element `868-followup`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/868-followup` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Uganda,Togo; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/868-followup/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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

## Rules the fix agent was under (2026-10-14): an open issue must describe exactly the remaining work -- if the branch resolves part of an issue, the report must DRAFT the residual; check that it did.

## The task the fix agent was given
## The task

RULING (@ligon 2026-10-14): the corpus-wide, both-tier re-warm is ACCEPTED, bundled with #810 and #970 (paid once, not here). Apply the reviewer's follow-ups on `fix/868-reduce-agreed` (verdict: needs-human, now resolved):
1. `lsms_library/country.py` `_replay_grain_audit` docstring and `_resolve_build_context` in `build_transforms.py`: one sentence each stating that under the framework context a report's `wave` is the wave FOLDER (`self.year`); a two-round folder (Nigeria PP/PH) must pass `wave=` explicitly to name the round.
2. `lsms_library/build_transforms.py` `_cells_blanked_report`: rename `dropped` to `collapsed_rows` (it is the projection/de-dup count, never a loss) so it cannot be read as Site 1's `dropped`; update `tests/test_gh868_reduce_to_agreed_ledger.py` accordingly and any formatter that prints it.
3. `Uganda/_/CONTENTS.org` and the commit message: replace "the cold-path signal is unchanged" with the measured statement: in-process hook unchanged; a cold `make` build emits the child's `GrainConflictWarning` on stderr and the parent's `GrainCollapseWarning` replay in-process.
4. In your report, restate the cache cost in the merge-note form the reviewer asked for: corpus-wide, both tiers, so the next person to warm a root does not attribute the cold builds to something else.
**Verify:** `tests/test_gh868_*`, `tests/test_gh323_*`, `tests/test_uganda_323*` if present, then the full suite via the recipe. Confirm with a stash-probe that YOUR edits (docstrings, rename) move no ADDITIONAL hashes beyond what the base branch already moves (the base moves all of them; say so).


## The branch: diffstat (vs development)
```
 lsms_library/build_transforms.py                   | 138 ++++++-
 lsms_library/countries/Togo/_/CONTENTS.org         |  11 +
 .../countries/Uganda/2009-10/_/crop_production.py  |   7 +-
 .../countries/Uganda/2010-11/_/crop_production.py  |   7 +-
 .../countries/Uganda/2011-12/_/crop_production.py  |   7 +-
 .../countries/Uganda/2013-14/_/crop_production.py  |   7 +-
 .../countries/Uganda/2015-16/_/crop_production.py  |   7 +-
 .../countries/Uganda/2018-19/_/crop_production.py  |   7 +-
 .../countries/Uganda/2019-20/_/crop_production.py  |   7 +-
 lsms_library/countries/Uganda/_/CONTENTS.org       |  51 ++-
 lsms_library/countries/Uganda/_/crop_production.py |  16 +-
 lsms_library/countries/Uganda/_/uganda.py          |  10 +-
 lsms_library/country.py                            | 123 +++++-
 tests/test_gh868_reduce_to_agreed_ledger.py        | 436 +++++++++++++++++++++
 14 files changed, 814 insertions(+), 20 deletions(-)
```
## The branch: commit messages
--- 9dfb83394 gh868 follow-up: name the wave-folder context, rename the cells_blanked collapse count, correct the cold-path sentence
Reviewer follow-ups on fix/868-reduce-agreed (verdict section 6), applied after
@ligon accepted the corpus-wide, both-tier re-warm on 2026-10-14 (bundled with
#810 and #970; paid once, not here).

1. `_replay_grain_audit` (country.py) and `_resolve_build_context`
   (build_transforms.py): under the framework's `build_context` a report's
   `wave` is the wave FOLDER (`Wave.year`); a two-round folder (Nigeria PP/PH)
   must pass `wave=` explicitly to the reducer to name the round.  The old
   docstring claimed a PP/PH wave parquet's reports "each already name their
   round", which is true only for a script that passed `wave=`.
2. `_cells_blanked_report`: `dropped` -> `collapsed_rows`.  It is `rows -
   groups`, the projection / de-dup count (2,801 for Uganda cluster_features
   2005-06 is the household-to-cluster projection), never a loss, and it
   shared a field name with Site 1's `dropped`, which counts removed rows.
   No formatter prints it: `_format_grain_report`'s cells_blanked branch
   returns before the `report.get("dropped")` test, and
   `bench/grain_census_871.py`'s `audit.get("dropped", 0)` now reads 0 on
   such a report, which is the correct reading.  Test literals updated.
3. Uganda/_/CONTENTS.org: "the cold-build signal is still the reducer's own
   GrainConflictWarning" was half true.  Measured (red team R8): the
   in-process cluster_features hook is unchanged (8 GrainConflictWarning, 0
   GrainCollapseWarning); a cold `make` build of crop_production emits the
   child's GrainConflictWarning on the child's stderr AND the parent's
   GrainCollapseWarning replay in-process (4), because the parent reads back
   the wave stamp the child just wrote.  The sentence in dda202b51's message
   ("the cold-path signal is unchanged") is superseded by this one.

Cache cost of THIS commit, stash-probed against HEAD (15 values: 10
`_table_cache_hash`, 4 `build_transforms_fingerprint`, Kosovo 2000 housing
`Wave._input_hash`): 6 moved -- Uganda x4 and Togo x2, the two countries whose
modules import from build_transforms.py and so fold its file hash; Malawi,
Ethiopia, Kosovo, Albania and all four fingerprints byte-identical.  No
ADDITIONAL hash beyond what the base branch already moves: against
development the base moves all of them (verdict R7), so the re-warm is
corpus-wide, both L2 tiers, and this commit adds nothing to it.

Verification: tests/test_gh868_*.py tests/test_gh323_*.py
tests/test_uganda_323*.py -> 229 passed, 2 skipped in 73.56s.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

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


