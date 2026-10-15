SCOPE DEVIATIONS

None. This independent review read frozen source and saved private evidence;
it did not call a Country/Feature API, rebuild surveys, access original/shared
caches, modify tracked files, mutate Git, or contact GitHub.

Verdict: APPROVE

Reviewed `fix/uganda-assets-concat` at
`515864c993050c73e8add567b0a36e22f55af3ef`, clean, against development
`dd647130802b348eb142370da312e5b651cc1b1e` (merged #967).
Exact diff SHA256:
`03a793d3ef1de819eeca0c5a0817d1d2031f195b8580614246749cd35cd63e61`.
Only assets.py, its new regression file, the task ledger and Uganda CONTENTS
change. No schema, framework, wave, fixture, dependency or policy changes.

Country contract and disposition

Read Uganda CONTENTS, including LOGBOOK/WAITING/TODO. Relevant idiosyncrasy:
the wave-concatenation and country-script builders share a country cache path
but can disagree. The existing text explicitly says “assets still disagrees
with its own script” and contrasts `Value/(t,i,j)` with `assets/(i,t,m)`.
The dated addendum preserves that historical evidence and records the narrow
convergence repair; #788 remains open for authoritative-builder and general
divergence/sibling-write policy. Another relevant distinction is the present
asset stock versus the unwired gsec15d 365-day durables flow. No ownership,
zero-value, stock/flow or label decision changes here. Other recorded Uganda
weight/GPS, crop-unit and recall caveats remain outside scope.

Implementation and tests

The compiler concatenates the supplied DataFrames under t, retains Value and
item j, then calls the existing wave-specific id_walk once. It neither aligns
wave Series into absent combinations nor squeezes a singleton into a scalar.
The id_converted attribute survives. No new reducer is introduced.
The old script blob `0da4929d149831c4f5a3351643b9377e5ebf26e0` is identical
at v0.14.0 and current development, independently confirming the source defect
predates this release; this is not a claim about unmeasured historical inputs.

The five tests execute the actual shipped script and real id_walk with only IO
and module/wave selection isolated. Expected frames are constructed directly
from observations, not from the implementation's concat. They discriminate
sparse disjoint waves, singleton broadcast, multiple items, genuine all-null
observations, and wave-specific/transitive ID mapping. All five fail on the
old script. The final-head focused run passes 31 tests, zero skips, four
expected synthetic warnings, in 13.70 s. Existing assertions remain intact.

Strict real-data evidence

The original Makefile route reproduced 790,688 rows and 673,841 duplicate
(i,t,m) keys; its eight wave inputs total only 116,808 rows. Final-head Make
actually executes all eight wave scripts and the country compiler, yielding
116,808 Value/float64 rows on (t,i,j), zero raw duplicates and zero null values.
The two original raw structure/sanity assertions pass before any API can
replace that output. Both final replay stages completed: Make 56.199 s,
API 52.750 s.

One review finding was resolved: initial assert_frame_equal calls allowed
float tolerance despite claiming exactness. The final replay explicitly uses
check_exact=True at all four comparison sites (Make raw, each of eight waves,
served API, and subsequent canonical raw cache). All pass on the frozen head.
I independently loaded only saved before/after raw and API snapshots and
repeated check_exact=True plus attrs equality. Both pairs pass and are also
byte-identical pickle pairs. Raw SHA256:
`5ab36a3df3f40f1da4955574476fc1c733af3783c42a306afbf4d6546b7a465f`;
served SHA256:
`577678df8aea2b2adafd36bed3119535944913dad110f47598b18d4a81252f3c`.
The served API retains all 116,808 rows and the pre-existing 4,237 duplicate
keys after item-label mapping. Those are deliberately not reinterpreted as a
new failure or repaired by aggregation.

Limits and cache scope

The Uganda baseline manifest has zero assets entries, independently checked;
no baseline-test coverage is claimed for assets. No full-suite or new CI-green
claim is made by this review. The script remains hashless; the ordinary API
rebuild stamps the expected hash and eight-wave coverage. Existing hash logic
makes this a Uganda-wide invalidation (28 country-table calculations and 224
wave/name calculations), not an assets-only cache change. The latter count
includes unmaterialized names. The control country is unchanged. Shared-cache
promotion and final release gates remain coordinator work.

Evidence

- source-validation.json and reviewed.diff: exact source scope and hashes.
- evidence-validation.json: independent exact comparisons and reviewed evidence hashes.
- Author evidence: control/framework/uganda-assets-diagnosis/{fixed_replay.py,
  fixed-replay-result.json, fixed-replay.log, final-unit.log,
  negative-control.log, replay-result.json, hash-scope-result.json}.
- Final replay JSON SHA256:
  `5eaa9b71af9c5405cdec2f2c89b908ce38d75cea61249330767f869beddc0df5`.

This review contains aggregate counts and source/evidence references only;
private snapshots and raw diagnostic logs must not be published.
