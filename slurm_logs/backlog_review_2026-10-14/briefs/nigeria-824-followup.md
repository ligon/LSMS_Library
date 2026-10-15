# RED-TEAM review of branch `fix/nigeria-824-followup` @ eeb445ebf (element `nigeria-824-followup`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/nigeria-824-followup` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Nigeria,Mali; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/nigeria-824-followup/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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

Apply the rulings and follow-ups on `fix/nigeria-824` (verdict: pass-with-fixes):
1. RULING (@ligon 2026-10-14): do NOT sign the 0.20 nitrogen share for Mali "Other inorganic" or Nigeria W1-W3 "other (specify)". REMOVE the `'other inorganic fertilizer': 0.20` alias (and any `_normalise_input_label` change that only exists to reach it) from `lsms_library/transformations.py` so the branch is #824 only; #867 carries the nitrogen vocabulary with its own disclosure. Measure and report Mali `plot_inputs` kg N before/after your removal (should return to the development figure; the reviewer measured +122,434 kg N / +13.5% from the alias) and Nigeria's (+4,402 kg N).
2. `Nigeria/_/CONTENTS.org` "DONE Unit_sold": replace the "built-table counts are lower than raw because..." sentence with the reviewer's measured decomposition (0 unresolved crop labels; rows dropped by the `(t,i,plot_id,crop)` dedup; 159 positive-sale rows discarded by the country script's `drop_duplicates(keep='first')` in W1/W2, 40 of them the only sale on their key -- take the exact numbers from the verdict §5.3 / §6.2).
3. `Nigeria/_/nigeria.py` `crop_production_for_wave` docstring: NA means the frame names no sold-unit column, the row records no sale, OR the sale's unit was not recorded (41 / 6 positive-sale rows).
4. `tests/test_nigeria_sold_unit.py::TestOtherInorganicSpelling`: invert it -- pin that `'Other inorganic'` (Mali) and `'other (specify)'` do NOT resolve to a nitrogen share (so #867 cannot land the alias by accident without a test moving). Optionally make `TestBuiltTable` assert on the API frame rather than the raw parquet.
5. Report (do not fix) the two pre-existing findings for filing: the silent `drop_duplicates` grain collapse, and the unread other-buyer sale columns `sa3q16a/b`, `sa3q17`.
**Verify:** `tests/test_nigeria_sold_unit.py`, `tests/test_gh867*` if any, full suite via the recipe; Nigeria `crop_production` cold-rebuilt in your PRIVATE root with `Unit_sold` counts per wave pasted; the Nigeria re-warm (24 tables) is the branch's known cost -- state, do not pay on the live root.


## The branch: diffstat (vs development)
```
 lsms_library/countries/Nigeria/_/CONTENTS.org      | 125 ++++++-
 .../countries/Nigeria/_/crop_production.py         |  25 +-
 lsms_library/countries/Nigeria/_/data_scheme.yml   |  27 ++
 lsms_library/countries/Nigeria/_/nigeria.py        |  41 ++-
 tests/test_nigeria_sold_unit.py                    | 410 +++++++++++++++++++++
 5 files changed, 617 insertions(+), 11 deletions(-)
```
## The branch: commit messages
--- eeb445ebf Nigeria CONTENTS.org / test comment: state the s11dq3 provenance per wave as measured
The previous wording attributed the {npk, urea, composite manure, other
(specify)} value-label set to W1-W3 on a W1/W2 measurement.  W3's
secta11d_harvestw3.dta (s11dq3 / s11dq15 / s11dq27) re-measured
2026-10-14: same four options, and the question stem reads "MAIN TYPE OF
... INORGANIC FERTILIZER" while option 3 is manure -- which is the
ambiguity that makes the share a #867 decision rather than an alias.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 110a73f12 Nigeria #824 follow-up: withdraw the #867 nitrogen alias by ruling; measured docs
RULING (@ligon 2026-10-14): do not sign a 0.20 nitrogen share for Mali
'Other inorganic' or Nigeria W1-W3 'other (specify)'.  Remove the
'other inorganic fertilizer': 0.20 alias from _NITROGEN_CONTENT so this
branch is #824 only; transformations.py is byte-identical to development
again (_normalise_input_label was never changed on this branch).

kg N via nitrogen_kg on the warm plot_inputs parquets, before (alias) ->
after (removed):
  Mali    1,028,795.6 -> 906,361.8  (-122,433.8, -11.9% of the aliased
          figure = the reviewer's +13.5% over development); matched rows
          38,569 -> 28,929; plots 3,029 -> 2,869; 'Other inorganic'
          9,640 rows back in unmatched_labels
  Nigeria   178,074.5 -> 173,672.0  (-4,402.5); matched rows
          10,184 -> 10,014; plots 4,472 -> 4,359; 'Other Inorganic
          Fertilizer' 170 rows back in unmatched_labels
Both "after" figures equal the development figures the red team measured.

tests/test_nigeria_sold_unit.py::TestOtherInorganicSpelling inverted:
pins that 'Other Inorganic Fertilizer', 'Other inorganic' and
'other (specify)' resolve to NO key, and that nitrogen_kg names the row
unmatched and warns.  TestBuiltTable gains an API-frame variant
(assume_cache_fresh, never cold-builds).

Nigeria/_/CONTENTS.org: the raw->built gap decomposed per the verdict
(0 unresolved crop labels; 93/66 rows dropped by the (t,i,plot_id,crop)
dedup, 151 with a different Quantity_sold, 40 the only sale on their
key; 4/1 unlabelled unit codes -> NA); the #867 rider rewritten as
withdrawn, with the Mali reach and the s11dq3 provenance; the three
Unit_sold NA causes (41/6 unrecorded sold units); the unread other-buyer
columns sa3q16a/b, sa3q17.  nigeria.py docstring says the same.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- d568ab428 Nigeria CONTENTS.org: Unit_sold reproduced on the API frame; #867 alias measured on plot_inputs
The served frame matches the parquet counts exactly (3,451/3,374 and
3,411/3,283; Unit_sold - u = {Olodo}), so the read-path mapping passes do
not separate u from Unit_sold.  The `other inorganic fertilizer` alias
measured on Nigeria's warm plot_inputs: 170 rows unmatched -> matched,
nitrogen_kg matched rows 10,014 -> 10,184, plots 4,359 -> 4,472, total
173,672 -> 178,074 kg N.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 8cc7eed2b Nigeria: carry the sale's own unit as crop_production.Unit_sold (W1/W2, GH #824)
The GHS-Panel post-harvest A3 module asks q6 (harvest) and q11 (sale) each
with its OWN quantity and PROD. UNIT CODE.  _/crop_production.py passed the
harvest unit (u) and the sold quantity/value but never sa3q11b, and
nigeria.crop_production_for_wave documented an `unit_sold` parameter the
code never read -- so Value_sold / Quantity_sold was filed under the
HARVEST unit by assumption on every row (Nigeria/_/CONTENTS.org:976-1000).

- crop_production_for_wave reads `unit_sold` on plot-grain sale frames,
  decodes it via `dec` and canonicalises through the SAME size-agnostic
  _canon_unit as `u`; emits Unit_sold as an NA-bearing `string` column
  (pinned to string on the all-NA W3-W5 waves, the KgFactor concat rule).
- _/crop_production.py passes unit_sold='sa3q11b' for W1 and W2 only.
  W3-W5 are untouched: their sale sits in secta3ii at hh-crop grain with no
  plot linkage (GH #843), the same reason their Quantity_sold is NaN.
- data_scheme.yml declares Unit_sold {type: str, optional: true}; the
  canonical data_info.yml already declares it.

Identified by the value-label vocabulary, not the variable label: sa3q11b's
Stata value-label set is IDENTICAL to the harvest unit's in both waves (W1
36 labels, W2 48); W1's varlabel reads "(11a)...(unit)", a slip.  No
sold-condition column exists in W1/W2 A3, so no Condition_sold.

Measured (isolated cold build vs the warm parquet): 62,844 rows unchanged,
every pre-existing column byte-identical; Unit_sold non-null 2011Q1 3,486 /
2013Q1 3,430 / W3-W5 0.  Agreement on positive-sale rows recording both
units: raw codes 3,438/3,548 (96.9%) and 3,316/3,478 (95.3%) -- CONTENTS.org
reproduced exactly; served size-collapsed labels 3,466 (97.7%) / 3,349
(96.3%); on the built table 3,374/3,451 (97.8%) / 3,283/3,411 (96.2%).
Consumer rule is the canonical one: a disagreement is a FLAG, not an
identification of the price denominator.

Framework read verified on the isolated root: the stale var parquet is
evicted (GH #866), make fails on the rule-less target, try_script runs the
country script and the read serves Unit_sold.

GH #867 rider (spelling only): `other inorganic fertilizer` added to
_NITROGEN_CONTENT as an alias of the existing `inorganic fertilizer` class
share (0.20).  Nigeria's 'Other Inorganic Fertilizer' label normalised to
`other inorganic` and matched nothing (170 rows).  Not a new number; the
NPS / D Compound / organic content decisions stay open on #867.
transformations.py reaches no build fingerprint, so no cache cost there.

Cache cost: nigeria.py and data_scheme.yml are in every Nigeria table's
L2-country and L2-wave hash, so all 24 Nigeria tables re-warm cold; only
crop_production's content changes.  Not paid on the shared cache here.

Tests: tests/test_nigeria_sold_unit.py (wiring, config via ast, value-label
identity, agreement counts, #867 alias).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


