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
Pending baseline negative, candidate focused controls, independent review,
metadata invalidation census and bounded actual-source/round-trip evidence.
