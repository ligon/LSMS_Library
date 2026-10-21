# Prior-Art Ledger -- GH #1160: country identity in fertilizer rates

**Search tier used:** ripgrep+git floor; direct definitions, consumers, tests and the full issue dispatch. GitNexus is optional per AGENTS.md's manual guidance and is not available here.
Baseline: frozen integration `4cab5f0db7d375ad543e652f61f43bd28fa94ecd`.
Inherits [STANDING.md](STANDING.md), [1159-pooled-reducers.md](1159-pooled-reducers.md), [1093-declared-unit-factors.md](1093-declared-unit-factors.md), and CLAUDE.md.

## §1 Task, restated

Retain country identity through `fertilizer_rate`'s numerator reduction, positive-area aggregation, inner join and output when both input frames carry country labels.
The user explicitly approved fixing #1160 before 0.16.0 using the same presence-based rule as #1159.
Country-less calls retain their existing behavior; country on only one side and null country labels must fail closed before filtering.
This changes identity, not nutrient shares, unit defaults, parcel policy, area deduplication or scientific interpretation.

## §2 Existing machinery

| Symbol | Baseline anchor | Tested by | Decision |
|--------|-----------------|-----------|----------|
| fertilizer_rate | transformations.py:6773-6945 | test_863_transforms.py:406-466,741-765; test_867_nitrogen_content.py | Extend existing grouping/join identity, preserve arithmetic |
| nitrogen_kg | transformations.py:5455 onward | test_gh1159_pooled_reducers.py; test_867_nitrogen_content.py | Reuse both N and product modes, including declarations and refusal |
| _plot_input_country_index | transformations.py:5424 | test_gh1159_pooled_reducers.py | Reuse existing numerator normalization/nonnull check; do not force area local columns into an index |
| _level_or_column | transformations.py:3834 | declared-factor/reducer tests | Reuse index-before-column identity precedence |
| _resolve_plot_level | transformations.py:3575 | test_plot_id_canonical.py | Reuse plot_id/plot alias choice |
| _parcel_from_crop_plot / _parcel_from_feature_plot | transformations.py existing fertilizer_rate calls | test_863_transforms.py parcel/default controls | Reuse without new parcel bridge policy |
| Hash census | slurm_logs/framework_campaign_2026-10-20/1159/hash_reach.py | prior full 549/2139/39 census | Reuse before/after separate processes, private roots |

## §3 Definitions and conventions in force

- Country is Feature's prepended identity dimension: AGENTS.md Core API; docs/guide/feature.md; #1159 ledger sections 3-4.
- Area is hectares per lsms_library/data_info.yml:444-453; no new denominator policy.
- Canonical plot_id and legacy plot aliases use the existing resolver; on='parcel' remains the current deliberate default.
- Nutrient='product' reuses nitrogen_kg with existing inorganic shares set to one, not a second conversion implementation.
- Inherit STANDING sections 3-4 and #1093's declaration precedence/refusal semantics.

## §4 Invariants and assumptions

- Country presence is axis/column presence, not cardinality or attrs. Both frames must agree on presence, including empty and one-country frames.
- Null labels on either side raise before nutrient/positive-area filtering. Index labels override a redundant country column, even a null contradictory column.
- Area frames with local keys as columns remain accepted. Avoid reset_index's duplicate-country-column failure by discarding only the redundant column on a new frame when the index supplies country.
- Grouping/join/output keys are country first, then existing common local keys; no label inference, broadcasting or joining across unmatched countries.
- Inputs and attrs remain unchanged; no new Country IO or production cache writes are needed.
- Existing area sum, inner join, zero/missing-area drop, unit behavior and country-less diagnostics remain unchanged.
- Measure fingerprints, do not infer cache impact from read-time intent.

## §5 Reuse decisions and acceptance

Reuse nitrogen_kg, country/key accessors and parcel helpers. Extend only rate boundary validation and country participation in existing keys.
For a country-labeled wholly unmatched join, retain PlotGrainMismatchWarning but identify country/land key mismatch rather than asserting plot vocabulary alone; leave country-less warning unchanged.
Extend the rate docstring and Feature guide narrowly.

Before source edits, run new controls against exact 4cab: A/B shared local plot and 1/2 kg Urea, 1/4 ha, expecting N .46/.23 and product 1/.5; separate-call equivalence, reversed frames, index/column combinations, parcel/plot, empty/one-country/null/one-sided/unmatched labels, redundant country column, area local columns and input immutability.
Run existing fertilizer, nitrogen, canonical plot, #1159, #1093 and unit-factor suites with private data and pinned threads; record genuine skips separately.
No real-country assessment is necessary for this identity-only counterexample.

## §6 Open questions for parent

The user's country-preserving rule and fail-closed boundaries resolve the scientific choice; no additional human choice is required.
Parent reviewed, surfaced and authorized this ledger before implementation, including the pooled-only unmatched diagnostic refinement.

### Phase 3 -- verification

- fertilizer_rate -- OK (sections 2-4): existing numerator/area arithmetic and parcel helpers reused; country participates in existing keys. Paired presence and null validation precede filtering; index precedence and accepted area-key columns are preserved without mutation.
- Empty pooled parcel key -- OK (sections 4-5): regression exposed float inference for an empty numerator comprehension; normalize only the country-present empty key, preserving country-less behavior.
- Diagnostics/docstrings/guide -- OK (sections 3-5): describe the presence-based two-input identity rule and distinguish unmatched country/land keys without changing the legacy warning.
- Regression proof -- OK (section 5): unchanged 4cab source gives80fail/7pass; final focused selection gives347pass/2private-cache Nigeria skips, zero failures/errors. The first candidate's two empty-parcel failures are separately recorded, not counted as passing validation.
- Cache reach -- OK (section 4): actual before/after549country/2139wave/39tagged hashes show zero changes/errors; no parquet was written.
- API audit -- OK (section 5): strict audit and diff whitespace checks pass.

Runtime/test/guide commit: 5125d0faa3e8074c289c5dd839cf1690d3e7a570, Sue + GPT-6 attribution, SUCODER published.
Aggregate evidence/repro: slurm_logs/framework_campaign_2026-10-20/1160/REPORT.org, verification.json, reproduce.py, negative_control.py and run.sh.
Full integration and exact-head credentialed/native CI are parent gates; no scientific or parcel policy is added.
