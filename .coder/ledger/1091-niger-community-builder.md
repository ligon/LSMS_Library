# Prior-Art Ledger - GH1091: Niger community builder retirement

Sue, 2026-10-21. Inherits `STANDING.md` sections 3-4.
Search tier: ripgrep + git; no GitNexus callable is available.
Approved boundary: root read the complete frozen diagnosis/proposal at
`control/isa-backlog-20261021/Niger/1091/`, source
`d602f8d63520e521a730f85551b366c4d45d9a35`, and approved this country-only
retirement before implementation. Its private 690-file manifest SHA256 is
`0ee725660357c646add06ad5d625210d5f0d5dad1cf6fb1fa7f952023db4e2a7`.

## Section 1: Task, restated

Retire Niger's concatenate-only country `community_prices` script and its
active country Make target. Keep the independent wave rule and both ECVMA
source builders. The framework is authoritative under GH788 maintainer
comments 6244122374/6259387154; GH840/1086 already settles the native
variety/visit/obs representation. No source/science/grain decision is reopened.

## Section 2: Existing machinery

| Symbol / file | Location at parent | Existing behavior / test | Decision |
|---|---|---|---|
| Country concatenator | `Niger/_/community_prices.py:26-45` | Loops four candidate paths, concatenates existing native frames; no source transform or ID walk | Retire |
| Country Make target | `Niger/_/Makefile:105-115` | Invokes both ECVMA scripts and country concatenator; sole user of COMMUNITY_PRICE_WAVES | Retire only this block |
| Independent wave pattern | `Niger/_/Makefile:117-118` | Runs the requested wave script with niger.py/categorical_mapping.org prerequisites | Reuse byte-exact |
| `_has_country_builder` | `country.py:4893` | Script existence, then real fresh-empty-root make -n; GH1039 tests | Reuse, no selector patch |
| `_aggregate_wave_data` / `load_from_waves` | `country.py:4977/5249` | Walks/normalizes/concatenates waves; make route at 5800 | Reuse |
| `_empty_declared_frame` | `country.py:1050` | Existing no-builder absent-subset result; GH1039/1099 tests | Reuse, disclose actual types |
| `_assert_built_required_columns` | `country.py:4817` | Required Price/Quantity after finalization, including script paths | Reuse unchanged |
| Native source helpers | `Niger/_/niger.py:1330-1555` | Original-Label maps, accepted duplicate handling, exact quantity mask, i==v shim | Preserve byte-exact |
| `get_dataframe` / `to_parquet` | `local_tools.py` | Sanctioned IO and normal hash/coverage gates; STANDING sections 2/4 | Reuse |
| Existing regression harness | `tests/test_gh1039_make_backend_no_country_rule.py` | Actual private script/Make operations, no-country-builder routing | Extend coverage in a new Niger test file |
| Feature audit | `bench/feature_audit/scan.py` | Actual phases 1/2; community_prices has no kwarg grid | Reuse; account for all three findings |

## Section 3: Definitions and country idiosyncrasies

Inherit `STANDING.md`, `CLAUDE.md`, and canonical `lsms_library/data_info.yml`.
The current complete 1559-line `Niger/_/CONTENTS.org` reading carries after
exact blob verification: `ad87b4f92204a53c4ccf49348af623d5b5d904dc`.
ECVMA 2011/2014 and EHCVM 2018/2021 have distinct designs; EHCVM household
identity is (grappe,menage) with one vague per grappe, while 2014 ECVMA
households require EXTENSION. Community v is native grappe and has no
household join. The dictionary key is Original Label, not Code.
Variety is the stable source CODE; Description retains source labels.
Native wave frames have an i==v shim; framework normalization removes it.
Accepted duplicate-entry handling drops 134 exact melted repeats and keeps
30 nonrepeat slots as obs4-6. Preserve all 46,043 readings and GH1088's
76 exact quantity masks; Price and source eligibility are unchanged.

Current Make coverage comments say only ECVMA has an EA-level module;
CONTENTS:1185 says the no-2018 claim "is false for 2018-19" and the country
docstring acknowledges its unwired grappe module. The helper still says
2018 "ships no community price file at all". The schema's old j-to-i API
comment also conflicts with the measured j-bearing public frames and shim.
Record these contradictions in a dated CONTENTS addition; do not edit
unrelated helper/schema comments or wire an EHCVM module.

## Section 4: Invariants and explicit layer decisions

- Production edits are exactly deletion of the country concatenator and
  removal of Make105-115. Keep the wave pattern, all shared variables and
  other targets, both source scripts, niger.py, mappings, schemas,
  materialize:make, masks, IDs and framework byte-exact.
- Preserve complete current default all/supported-singleton frames and
  recursively typed attrs at matched stages; use check_exact=True and
  rtol=atol=0. Never normalize values, axes, dtypes or attrs to compare.
- Parent Make singleton/unsupported outputs are wrong: all return 46,043
  rows. Correct subsets are 32,352 / 13,691 / zero; do not preserve wrong
  rows as acceptance targets.
- Legacy Make raw country output includes the i==v shim and lacks
  id_converted. Authoritative default raw output is normalized and carries
  the flag. Corrected supported Make results must match the latter.
- Parent absent cold/populated empties differ in id_converted; existing
  declared-empty helper additionally projects string visit/obs and object
  Description. Root approved these existing-contract route consequences,
  subject to actual candidate measurements, not blanket cross-state equality.
- Make's wave route writes no new country parquet. Hashless native wave
  eviction/rewrite may occur normally on warm reads; measure bytes/mode/mtime
  rather than suppressing it or requiring a false blanket zero-write claim.
- CLI prewrite API frames and postread Arrow artifacts are separate layers:
  PopulationRecord serializes to dict. Compare each to actual matched parent
  controls; retain complete footer/typed-attrs evidence.
- Food control remains 239,877 with derived counts 165,027 / 239,855 /
  165,014. Its pre-existing u cold/warm dtype residual is owned by GH1196;
  do not repair it here.
- Measure all 549 API hashes and 39 framework fingerprints in separate
  exact-source processes BEFORE source rebuilding. Prediction: 23 Niger
  hashes move, 526 others and 39 framework fingerprints stay exact. This
  ledger records a prediction until measurements exist, not 23 built outputs.
- All sources are held MD5/SHA/size/mode/mtime-gated physical copies in new
  roots, with offline/write guards. Old diagnosis and other country evidence
  are immutable. No acquisition, DVC CLI, bypass, stamp or shared-cache edit.

## Section 5: Reuse decision

Reuse the existing authoritative wave aggregator and no-builder selector.
Delete duplicate orchestration; add no concatenator, selector or transform.
Extend the GH1039 actual Make/script test pattern in a new scoped test file,
plus exact source-backed parent/candidate validation. Keep materialize:make
and required-column enforcement. No fixture, golden or dependency change.

## Section 6: Open scope

No further authority/grain/science decision blocks this approved boundary.
Mali is independently ongoing and Tanzania remains GH1091 residual; this
change makes no completion/composition claim for either country.
Actual empty-state and warm-Make consequences must be reported before
freeze; substantive unexpected behavior goes back to root before expansion.

## Phase 3: Verification

Author validation is complete at runtime/test snapshot
`6be6576607f1862b603d123acfb74ace4026fd3f`; independent review is pending.
The full report and aggregate validation live at
`slurm_logs/isa_backlog_2026-10-21/Niger/1091/`.

- OK (anchored on sections 2/5): only the duplicate country orchestration was
  deleted. Original Make105-115 alone is removed; the independent wave rule,
  its prerequisites, other Make bytes, both native builders and framework
  remain exact. No new transform or selector reinvents existing machinery.
- OK (anchored on sections 3/4): 11 held sources, 11 sidecars and 44 physical
  private copies reached terminal MD5/SHA/size/mode/mtime gates before API
  use. All 46,043 source readings, 30 late obs slots and 76 exact GH1088
  quantity masks remain intact, with no grain/ID/eligibility/unit changes.
- OK (anchored on section 4): the actual prebuild census is 23 changed Niger
  API hashes, 526 unchanged others and 39 unchanged framework fingerprints
  out of 549/39, not merely the earlier prediction. No other corpus build
  was implied or performed.
- OK (anchored on sections 2/4/5): 97 existing focused and 17 new tests pass
  (114, zero skips). The same new file gives 12 meaningful failures and five
  passes against the actual parent, zero fixture errors. An initial omitted
  fixture Waves declaration and its stopped/error run remain excluded and
  preserved; correcting that fixture did not weaken assertions. Twelve
  real built-data assertions also pass with nonempty guards retained.
- OK (anchored on section 4): twelve normal cold, twelve fresh-process warm
  and twelve populated requests measure the complete both-backend subset
  matrix. Seventy-two strict matched-stage/full typed-attrs comparisons,
  72 explicit axis-internal comparisons and three full Arrow footer/schema/
  artifact-byte controls pass at zero numeric tolerance. Healthy Mali40218,
  Feature86261, food239877 and its three derived tables remain exact at
  matched parent stages. All three actual scanner records match individually;
  community_prices has no phase-2 kwargs grid.
- OK (anchored on section 4): default warm receipts have zero changes. Make
  naturally rewrites two/one/zero native wave files for all/singleton/absent
  requests, with exact bytes/size/modes and changed mtimes, no new country
  cache. Supported Make full attrs match authoritative parent default attrs.
  Existing empty-state, native serialization and CLI PopulationRecord-to-dict
  differences remain explicit strict findings, not normalized comparisons.
  The food u cold/warm dtype residual stays with GH1196.
- LIMIT (anchored on section 4): an auxiliary direct-Wave collector asserted
  zero changes before persisting its receipts and failed; the original delta
  was lost and remains unidentified. Later per-call and same-collector
  observations show zero changes/full native equality, but cannot explain
  that original delta. It is separate from the normal validated matrix.
  No further auxiliary repeats were run to obtain zero writes.
- LIMIT (anchored on section 4): first-freeze export packaging replaced the
  private detailed aggregate validation.json with its safe export before the
  first manifest. Its original final size/SHA were not recorded. The safe
  replacement and both freeze attempts remain; a separately named aggregate
  reconstruction uses unchanged per-probe artifacts, without claiming original
  bytes. No API/source/test replay was used. The corrected freeze inventories
  its exact path boundary and isolates safe exports; independent instructions,
  REPORT, READY and residual text disclose this qualification.
- OK (anchored on section 6): Tanzania remains residual and Mali is
  independently handled. No multi-country completion, composition, GitHub
  write or final independent PASS is claimed. Final freeze receipts prove
  source/reference preservation, runtime/test equivalence and quiescence.
