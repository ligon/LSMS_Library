SCOPE DEVIATIONS

No prohibited source, baseline, dependency, shared-cache or GitHub changes.
The requested existing assets invariance check is unavailable: the committed
Uganda baseline manifest contains zero assets entries. This is reported as a
coverage limit, not a passing test. Exact private raw/API equality supplies the
real-data comparison. Original structural/sanity test methods were invoked
directly on the private cache, retaining their assertions; the credential-free
pytest collection marker would otherwise skip these data-dependent classes.

Frozen source: `fix/uganda-assets-concat` at
`515864c993050c73e8add567b0a36e22f55af3ef`, after merging current development
`dd647130802b348eb142370da312e5b651cc1b1e` (approved #967). The fix commit is
`84db87ed90952e385c4ac31e9918e085756c86ae`; prior-art commit `c008327a9` precedes it. Worktree `/local/job39919092/release-work/framework/uganda-assets`.
Only four paths differ from development: assets compiler, focused regression
file, task ledger, and dated Uganda CONTENTS addendum. No pushes or GitHub writes.

The cause and why corpus/API validation did not expose it

The corrected credentialed CI explicitly ran Uganda's `make -j2`, which ran
all eight wave assets scripts and then `python assets.py` at
2026-10-15T07:16:53.6897430Z. The country compiler stacked each wave, squeezed it,
aligned the resulting Series as dictionary columns, then stacked again. Under
pandas 3 that constructs the union of household/item keys crossed with waves:
98,836 × 8 = 790,688 rows, only 116,808 with a non-null value. It also moves the
item `j` out of the index and leaves the measure-axis residue `level_2`, renames
Value to assets, and bakes m into the index. The 673,841 duplicate `(i,t,m)` keys
are therefore not a cache-root mix-up or an assets aggregation policy decision.
The singleton synthetic control separately proves scalar broadcast.

The real script is old: last substantive index change `eadaacb6f` (2026-02-18)
is an ancestor of v0.14.0. The recorded Uganda idiosyncrasy predicts this exact
failure. CONTENTS says “assets still disagrees with its own script” and contrasts
wave `Value, (t,i,j)` with script `assets, (i,t,m)`. The September 116,808-row
measurement was the canonical API path. The current #809 country gate treats
this script's hashless output as stale, rebuilds from waves and writes a valid
raw cache. An API check can therefore erase the faulty artifact before raw
checks inspect it. The actual failing CI raw table-structure checks saw the
Makefile artifact and did not call the API. This evidence needs no session-root
or #941 attribution.

The repair keeps both builders. It concatenates the intact wave DataFrames
under t, keeps Value and j, and retains the existing `id_walk` exactly once.
Mapped concat equals the canonical raw cache exactly; plain concat differs in
36.2% of household ID positions, so dropping id_walk would be incorrect.
Assets remains the gsec14/h14q05 stock, not the unwired 365-day durables flow.
Existing ownership/zero filters, item labels, reducers and schemas are untouched.
The general #788 authority/divergence-detection issue remains open.

Evidence and results

- `replay-result.json`: untouched actual script on physical private copies
  reproduces both original CI failures exactly: 790,688 rows and 673,841 duplicate
  keys, columns j/level_2/assets. Both original structural/sanity test methods
  fail. Canonical API then rebuilds the raw cache to 116,808 Value rows, zero
  raw duplicates, and both original checks pass.
- `synthetic-result.json`: actual old script on disjoint two-row waves makes
  8 rows from 4. With one singleton wave, it makes 4 rows from 3 and broadcasts
  the singleton value. No source patch was needed for this diagnostic control.
- `negative-control.log`: all five new actual-script tests fail against the
  unmodified script, 8.62 s. Cases: disjoint waves, singleton, multiple items,
  all-null wave, wave-specific/transitive household crosswalk.
- `final-unit.log`: final frozen head31 passed, zero skips, 13.70 s, covering new tests plus
  existing assets-additive and sibling Uganda earnings script tests. Four
  expected synthetic grain warnings; no assertions weakened.
- `fixed-replay-result.json`: actual unmodified Makefile target rebuilt all
  eight source wave scripts in 56.20 s. Output is 116,808 Value/float64 rows on
  (t,i,j), zero duplicate raw keys and zero null values. Explicit `pd.testing.assert_frame_equal(..., check_exact=True)`
  equality (including values, index, dtype, order) plus attrs equality against the
  prior canonical cache passes. All eight rebuilt wave frames equal their
  inputs. Both original structural/sanity methods pass on this Makefile output.
- Subsequent canonical API rebuild: 52.75 s, exact equality with the prior
  served API including attrs and the existing 4,237 post-label duplicate keys.
  Rewritten raw cache also equals the prior canonical frame. Zero in-tree
  parquet files. No change to the post-label duplicate/reducer policy.

Per-wave real input rows, all Value/float64 on (i,j), zero duplicate raw keys
and zero null values: 2005-06 22,994; 2009-10 16,306; 2010-11 14,394;
2011-12 14,846; 2013-14 14,477; 2015-16 15,073; 2018-19 9,227; 2019-20 9,491.

Cache scope and isolation

`hash-scope-result.json` compares unchanged base7c6265de7 source with the fix:
all 28 Uganda country-table hash calculations and all 224 wave/table-name hash
calculations change. The latter includes names without a materialized wave
output; it is not a claim of 224 existing caches. Every country `.py` input is
hashed by the existing machinery. GhanaSPS control: 13 country and 39 wave/name
hashes unchanged. Code inspection confines this change to Uganda. The country
script still writes no hash or wave manifest; the canonical API rebuild stamps
both. No manual stamping or global cache clear was used.

Original corpus API was never called. `copy-manifest.json` records nine physical
files copied from the completed corpus (1,005,412 bytes, matching hashes and
separate inodes). Diagnosis root `framework/uganda-assets-diagnosis-data` and
fixed root `framework/uganda-assets-fixed-data` are separate physical copies;
`fixed-copy-manifest.json` records the second. Only their dvc-cache paths are
symlinked to the shared content-addressed raw cache. Private survey frames and
raw logs remain in this mode700 control directory and are not publishable.

Reproduction

All Python commands used `/local/job39919092/venv_img/bin/python`, taskset CPUs
16–23, BLAS/OMP/MKL=1, LSMS_BUILD_WORKERS=1, explicit source PYTHONPATH and
LSMS_COUNTRIES_ROOT, private LSMS_DATA_DIR and empty LSMS_CONFIG_DIR with
LSMS_SKIP_AUTH=1; LSMS_NO_CACHE unset. Drivers assert package/config/root identity.
`replay.py`, `synthetic_probe.py`, `compare_reference.py`, `fixed_replay.py` and
`hash_scope.py` record the actual operations. The pytest command was:

    python -m pytest -q -n 6 --dist=loadfile tests/test_uganda_assets_script.py tests/test_assets_additive.py tests/test_gh772_uganda_earnings_shape.py

Corrected CI result is preserved, not called green

https://github.com/ligon/LSMS_Library/actions/runs/38697245468
source9d5bf200f: 3 failed, 4,919 passed, 2,768 skipped, 4 xfailed, 1 xpassed,
1,004 warnings in 3,140.09 s. Two assets failures above; third is the nutrition
23,801→23,777 baseline mismatch now explicitly approved by the user and handled
separately by the country agent. Log 461,456 bytes, SHA256
`e1895513df52c2c99b7c2cf8b0840b60890f04a469fe6f6afd64992627fe621e`.
Parent verified EHCVM163 pass markers/no skips, currency20 passes, Ghana
regression57.85 s. These do not establish a cause for historical #941.

Old unpinned4f job and monitor closure

https://github.com/ligon/LSMS_Library/actions/runs/38687547945/job/116265764223
ended failure at 2026-10-15T08:54:10Z. `old-job-annotations.json` reports “The
hosted runner lost communication with the server.” The annotation lists generic
possible process/resource/network causes; none is established here. The full
pytest step is left in_progress in final job metadata. Repeated terminal-job log
fetches returned HTTP404, so no final pytest summary or test counts are claimed.
Earlier in-progress job-log access also returned404.

All monitored jobs were terminal. Only the owned detached monitor PID4145642
was stopped at 2026-10-15T13:20:59.809619Z, after verifying its command and no
capture child; no Actions run was cancelled by this agent. Durable record:
`../credentialed-ci/monitor-stopped.json`. Future availability of the missing
old job log is unknown; manual recapture remains possible. No further polling
process is running from this agent.

Final-head replay and focused tests completed successfully. The reviewer noted
that pandas float comparisons default to tolerances; the final replay was
therefore repeated with explicit `check_exact=True` at every frame comparison.
The final aggregate sets `comparison_check_exact: true`. Earlier default-tolerance
results remain private and do not carry the strict-equality claim. Private
`fixed_make_cache.pkl` / `fixed_api.pkl` snapshots permit independent exact checks
against `before_cache.pkl` / `before_api.pkl`; none is included in the public bundle.
The source remains frozen and clean. The lsms_library/ source subtree differs
from integration 17308b3d4 only in the assets compiler and CONTENTS documentation;
whole-tree differences also include the approved #967 fixture/docs/ledger and
new tests. The final isolated
hash comparison in `hash-scope-final-result.json` confirms the same Uganda-wide
scope and unchanged Ghana control after the #967 merge.
