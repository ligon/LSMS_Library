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


## Current declaration correction and public-head extraction (2026-10-28)

Earlier source-script-only definitions/invariants above describe the first
repair and are superseded for table eligibility: actual `Country.waves`,
`Country.__getitem__` folder mapping and `Wave.data_scheme` (native script
AND YAML keys) govern. Reuse these APIs; no YAML-null override or new helper.
Undeclared/retired tables skip before output existence/readers, declared
absence/errors stay loud, healthy/empty/native IDs and concat attrs unchanged.
STANDING §2/§4 and current single-pass/exactly-once `local_tools.id_walk`
govern; this country compiler does not walk. No namespace/panel/science edit.

Public parent0270c293fc5fa001b96782226d8d397ec48d010a (d65e8b136 base)
is the sole extraction parent. Product bytes from privatef86213a3 are
SHA256d6b0425afae59cc74137552114d5acb9ed7949fb6c06c304eedcc9cdae98af9d.
Only compiler, two targeted tests, this ledger and own CONTENTS change.
All inherited scientific/component/factor changes and peer review ledgers
are excluded. No public ref, original checkout or shared cache mutation.

Author19PASS versus historical-script10FAIL/9PASS is meaningful: failure
causes are retired/YAML/map/native-only eligibility and new-wave omission;
9healthy/error/empty cases remain healthy. The public0270 script is exactly
historicalf853918f; all5 recorded framework hashes match0270. Source-run
HEAD109e7e1 had separate pre/post-bound product/test bytes before commit,
so do not relabel that run exactf862/extracted-head acceptance. No existing
retirement-red cohort needs replay solely for extraction.

The extracted candidate requires a newly released bounded31-case run:
19 unchanged metadata tests +11 adapted prior tests +1 genuine id_walk
post-compiler boundary case. The last case uses invented chained identities
and actual single-pass id_walk, no production panel JSON/provider. Existing
wrong CONFIGERROR test/result bytes are retained as historical evidence.
Preparation performs source/AST/hash checks only; no tests/import/data actor
or scientific/whole-Country/cache/hash-census acceptance is claimed.

## Independent extracted-candidate verification

Root ran all31candidate cases once on the public0270-based extraction:31PASS,0errors/skips/guarddenials, unchanged bound source/config, empty data/L1, and77zero-byte output markers. This includes19author declaration cases,11adapted earlier cases and one new actual id_walk boundary on invented chained identities. No old retirement-red cohort replay; this proves neither real-source nor full Country/Feature/cache/scientific closure. Guard and JUnit/source-before/after receipts are kept in the private root-runtime-v1 record.
