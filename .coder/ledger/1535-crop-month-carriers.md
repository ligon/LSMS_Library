# Prior-Art Ledger — GH1535 crop month carriers

Search tier: ripgrep + git; base development3b28d293f71b962c3d878c2886be9ee794407915.

## §1 Task, restated
Make the ordinary crop_production Country outputs and their Feature assembly
carry month answers in a common nullable textual type, so a pooled frame can
be written to Parquet without losing multi-month responses or year precision.
Investigate both harvest_month (the reported failure) and planting_month (the
same Nigerian year-month versus numeric-month heterogeneity). No date parsing,
month selection, validation or new survey derivation belongs in this repair.

## §2 Existing machinery
- country.py:_load_canonical_dtypes and _enforce_canonical_dtypes read global
  Columns type declarations and apply them after local declared types in
  Country._finalize_result; tested by test_gh798_datetime_mixed.py and the
  canonical dtype tests. REUSE, no new Feature dtype mechanism.
- feature.py:Feature.__call__ stacks finalized Country frames; existing
  _harmonize_country_frame aligns indices and existing attrs carriers preserve
  country metadata. REUSE; no new concat/reducer/serializer.
- countries/GhanaSPS/_/ghanasps.py:month_string retains a sorted set of month
  tokens; tests/test_ghanasps_crop_production.py:test_harvest_month_tokens.
  countries/Nigeria/_/nigeria.py:_month_str emits YYYY-MM where a year exists,
  otherwise MM. Neither is a universal parser; leave both native builders alone.
- data_info.yml:Columns.crop_production declares many optional payload types
  but currently neither month column. Extend this existing schema surface.

## §3 Definitions & conventions in force
- STANDING.md §3: global data_info.yml is the canonical schema; do not hardcode
  a dtype rule in Feature. STANDING.md §4: cached parquets are pre-finalize.
- GhanaSPS/_/CONTENTS.org “harvest_month is per crop later, a plot-level proxy
  in 2009-10”: later answers may span all twelve months; first wave is the
  ending month of the dominant crop's season. Text does not erase this caveat.
- Nigeria _month_str preserves source year when supplied. A global datetime
  or scalar-month parser would destroy that heterogeneity; no such change.
- Local float/int/string declarations describe native carriers. Global schema
  wins at finalization, as country.py:_enforce_canonical_dtypes documents.

## §4 Invariants & assumptions
- Preserve every nonmissing textual answer verbatim, including multi-month
  sets and year-month precision. Numeric scalars become their textual form;
  missing stays missing. Do not normalize, round or choose a month.
- Preserve all keys, rows, row order, other values/dtypes/nulls and typed attrs.
- Preserve native builders, pre-finalize caches and source declarations.
- No added required column; absent/all-null month behavior remains as before.
- No source or weights/inference policy change. STANDING.md §4 IO applies.

## §5 Reuse decision
Extend only global optional string declarations if the two-column reproducer
confirms the scope. Reuse canonical dtype enforcement, ordinary Country/Feature
assembly and pandas' existing nullable-string conversion. No new parser or
feature-only coercion and no country config rewrite is needed.

## §6 Open questions for the human
None for this storage/interface repair. Calendar interpretation and source
month semantics remain survey-specific. A request to normalize these values
would be a separate task and is not inferred here.

### Phase 3 — verification
- Canonical declarations: OK (§§2–5). Only two optional `str` declarations
  reuse the existing finalizer; no parser, Feature implementation, native
  writer, cache writer or estimator is added. Existing numeric public month
  assertions are deliberately updated; native wave/country caches gain five
  explicit numeric-value/type protections. No fixture, source amount, skip or
  tolerance is relaxed. Independent closure at4b635:28 passed/no skips;
  52 old assertion nodes unchanged, two public expectations replaced.
- Parent negative: eight failures/six passes in the final14-case reproducer,
  including both month columns and both country orders. Earlier fixture
  constructor/index-oracle errors are retained separately.
- Source/API: frozen d673 actual Tanzania/Nigeria/GhanaSPS/Malawi frames and
  default Feature253611rows match the parent exactly except intended month
  text. New month fields export and round-trip exactly. Other fields/index
  types/attrs match the parent's identical nonmonth Arrow round-trip control;
  existing string-index null and record-subclass JSON normalization are not
  claimed as bitwise typed-metadata preservation. Warm33cache files retain
  bytes/epochs. Source-v3 authenticates/reuses closed successful parent inputs;
  no parent source campaign was replayed to correct packaging assertions.
- Independent original review:30 candidate passes,18 parent expected-Arrow
  controls across all six country orders; no P1/P2. Standard sanity audit:
  parent63pass/5warn/0fail, candidate61/7/0. Two new native-declaration versus
  canonical-type warnings are explicitly separated as GH1606, with reviewed
  non-blocking status. No all-green/corpus-wide audit claim.
- Full isolated offline CI at4b635:11581 passed/5652 skipped/4xfailed,
  558.35s pytest/561.55s runner, eight allocated CPUs/four loadfile workers,
  false credentials/private roots/network disabled. Initial d673 full run
  (11553pass/5649skip/4xfail/eight stale public-month expectation failures)
  remains an actual failed run; source checks from it are separate evidence.
- Current-development composition9d84 over8505:151 relevant checks pass.
  Core/schema/Feature/Country/Malawi crop producers remain byte-identical to
  tested4b635; incoming1174 only adds plot-input mapping functions and changes
  plot-input registry entries, with crop registry slice exact. This is bounded
  composition/byte evidence, not a new native source build or full-suite run.
- Final metadata census8505 versus9d84:zero differences among450 declared
  country-table,1895wave,35build and186import fingerprints, zero errors or
  provider/build denials; runtime-derived553/2313/39 surface kept separate.
  API audit passes. Final ledger-only freeze requires root tree equality and
  configured final-head GitHub checks before authorized development merge.
- Operational qualification: an initial supporting warm regression omitted
  explicit private global roots and refreshed shared Mali/Tanzania caches.
  User notified;13 recent parquet metadata observations were recorded without
  a prior snapshot, so no complete attribution/continuity claim or blind
  rollback. Subsequent tests/censuses/source actors use explicit private roots.
  All cancelled/setup/source-oracle failures remain in versioned reports.
- Scientific boundaries (§§3–4): calendar meanings remain survey-specific;
  no weight, food inference, sold-unit policy, whole-wave promotion or master
  release is part of this change. Planned shared corpus rewarming stays held.
