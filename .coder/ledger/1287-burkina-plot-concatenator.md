# Prior-Art Ledger — GH #1287: Burkina plot_features refuses a missing declared wave

**Search tier:** git + ripgrep floor. This is a country-only extension of the
reviewed Niger refusal; no shared framework changes. Complete Burkina
CONTENTS.org was the first country source read, including every dated/TODO/
WAITING entry. Scope authorized by the 2026-10-28 expanded-country steering.

## §1 Task
The country `_/plot_features.py` catches **every** reader exception and skips
that wave, even though both listed EHCVM waves have a configured script. It
can therefore write a partial country table after a missing/corrupt/unreadable
wave. Preserve 2014's genuine absence, the two EHCVM native household formats,
the 2021 Panel/Cross_Section split and all payloads. Refuse missing declared
outputs before writing; propagate other reader failures. No source data read.

## §2 Existing machinery
- `Burkina_Faso/_/plot_features.py`: one country concatenator; listed waves
  2018-19 / 2021-22. Country data_scheme declares materialize: make; no
  country plot_features recipe exists in its Makefile, so framework may run
  this script. Current `except Exception: continue` is the defect.
- `Niger/_/niger.py:concat_wave_parquets` and
  `tests/test_niger_concatenator_refusal.py`: tested source-script premise,
  explicit `data_root(country)` path and missing-wave refusal. Reuse the
  bounded inline pattern for Burkina's ONE concatenator; do not import Niger
  or add a framework helper/country-module symbol used by other tables.
- `lsms_library.paths.data_root`: existing configured output root. Current
  get_dataframe relative-path resolution reaches the same private output
  tree; explicit path makes the refusal inspect the writer's destination.
- `local_tools.get_dataframe` / `to_parquet`: sanctioned leaves, unchanged.
- `Country.run_make_target`: shared failed-recipe refusal already changed by
  #1288; this task does not reimplement it or claim Make backend acceptance.
- `tests/test_gh1276_burkina_two_components.py` / #1244 tests: existing real
  data guards and source checks; real checks withheld. Current country paths
  equal recovered bc233435a source; old runtime scalars remain qualified.

## §3 Definitions
Inherit STANDING.md §3/§4 and CLAUDE.md's read-only config, two build paths and
exactly-once id_walk contracts. For this script-path table a configured wave
has `Burkina_Faso/<wave>/_/plot_features.py`. Both listed waves do; 2014 does
not. CONTENTS.org records: 2014 is a different instrument, 2018 uses ehcvm_i,
2021 Panel uses country i and Cross_Section uses CS_. This concatenator must
not apply id_walk or infer a new source population. A parquet is an output,
not a declaration; its absence cannot mean the feature was never wired.

## §4 Invariants
- Keep the explicit current EHCVM wave order; guard the source-script premise.
  A source-only test compares that list with actual configured wave scripts,
  so a newly configured wave cannot silently disappear behind the list.
- Read outputs only below data_root; never in-tree or real providers.
- Never write when any declared wave is missing or a reader fails. Do not
  catch data corruption, permission or other errors as an unwired-wave skip.
- Healthy concat retains native keys, row order, dtypes, NA masks and attrs
  exactly as pd.concat did before. Keep uniqueness assertion and writer.
- No Makefile chaining edit: this country has no chained plot_features rule.
- Existing stale outputs and framework/API cold/warm behavior remain outside
  this missing-output repair; do not claim cache freshness from existence.
- Script is a country build input: expect all Burkina country hashes to move
  and wave hashes through country script inclusion. No runtime hash census
  authorized; impact is source-inspected, not measured.

## §5 Reuse decision and bounded verification
Extend Niger's inline refusal in the single Burkina script, retaining its
source premise and output path convention. No new reader/writer/ID helper or
reducer. Tests follow the existing runpy harness but stub both source-reader
and output-writer leaves: fabricated in-memory frames only, dummy path
markers under private tmp_path, no real Country/Feature or data builds.

Before runtime publish exact candidate/test/launcher hashes. Use the approved
PROJECT_PYTHON, one CPU, BLAS/OMP/MKL/BUILD=1 and exact package/config/data
identity assertions. Pytest selection is the new file only; no full suite or
xdist, maximum 120 s/8 GiB. Inspect root/tests conftests first (done); ordinary
run does not purge caches. No rebuild/cache flags. tmp_path stays inside the
private runtime. Baseline control executes the initial country SCRIPT TEXT
with the SAME stubs, preserving source checkout/history. Healthy input must
pass on both versions; absent-each-wave tests must expose original partial
write and fail candidate closed. Additional controls: corruption/permission
reader errors, duplicate index, missing configured script, no unconfigured
2014 read, caller outside country cwd, full expected typed/NA/attrs frame.
No raw/API/hash-census/scientific assertion update authorized.

## §6 Open questions and withheld checks
No new source or scientific decision is required for a missing configured
wave refusal. Root must independently review the source and meaningful
synthetic results before integration. Actual Default/Make builds, cold/warm,
Country/Feature and source payload conservation remain unexecuted. Broader
country work (#1226/#1187/#1148/#1060/#1057/#864/#816) needs source authority,
shared dependencies or a concrete decision; report those rather than invent
bindings or reuse old counts as fresh measurements.

## Coordinator extraction -- 2026-10-28

The new script/test repair is extracted onto development d65e8b136. The
original script blob is identical on d65 and the owner initial ce9bf7df6;
country.py, local_tools.py and paths.py also match. No inherited two-frame
sidecar, population or scientific change is included. Only the dated 1287
CONTENTS note is carried. Independent original-owner checks: candidate11pass,
originalscript7fail/4pass; metadata postcheck continuation is separately
qualified. Recheck the exact extracted head before opening a PR.

Sue
