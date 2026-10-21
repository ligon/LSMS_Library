# Prior-Art Ledger -- GH #1159: country identity in plot-input reducers

**Search tier used:** ripgrep+git floor; direct definitions, consumers, tests, live issue and existing evidence.
Baseline: frozen integration `852ac2bfcd5f798533f56e7efa1f77db80c0138f`.
Inherits [STANDING.md](STANDING.md), [1093-declared-unit-factors.md](1093-declared-unit-factors.md) and `CLAUDE.md`.

## Section 1 / §1 Task, restated

Prevent `seed_kg` and `nitrogen_kg` from silently reducing unrelated countries' plots together when local `(t, i, plot_id)` values collide.
The live issue has no comments as read on 2026-10-20.
The maintainer's explicit session ruling, relayed by the parent after the required choice, is **retain country in pooled results**.
Apply that identity rule whenever a country index level or column is present, including one labeled country and empty results; preserve ordinary country-less results.
This extends reduction identity only, with no unit-factor, nitrogen-share, missing-quantity, parcel or price-pooling policy change.

## Section 2 / §2 Existing machinery

| Symbol / evidence | Anchor at baseline | Existing tests | Reuse decision |
|-------------------|--------------------|----------------|----------------|
| `nitrogen_kg` | transformations.py:5424, reduction:5530 | test_867_nitrogen_content.py; test_863_transforms.py | Extend existing per-plot reduction identity; preserve share matching and diagnostics |
| `seed_kg` | transformations.py:5543, reduction:5597 | test_gh1093_declared_unit_consumers.py; test_plot_id_canonical.py | Extend existing per-plot reduction identity and output shape |
| `_declared_kg_factors` | transformations.py:4195 | test_crop_kg_factor.py; test_gh1093_declared_unit_consumers.py | Reuse flat/nested maps and country index/column matching without modification |
| `_kg_factor_series_with_declared` | transformations.py:4268 | test_gh1093_declared_unit_consumers.py | Reuse declaration precedence, explicit-empty override, unit sentinel refusal and fallback |
| `_resolve_plot_level` / `_canonicalise_plot_level` | transformations.py:3575 / :3564 | test_plot_id_canonical.py | Reuse plot/plot_id aliases and canonical output spelling |
| `_with_u_in_index` | transformations.py:3592 | crop/agriculture transform tests | Existing promotion pattern: append a present column to index without resetting colliding rows |
| `_level_or_column` | transformations.py:3834 | factor tests | Existing index-before-column identity precedence |
| `Feature.__call__` | feature.py, country concat at :1095 | test_gh1093_declared_unit_consumers.py:156 | Keep country axis supplied by assembly; extend fake-country integration fixture to colliding local IDs |
| GH #1159 metric A/B repro | live issue body | independently reproduced at 976fc840d and 852ac2bfc | Reuse exact 1/2 kg Seed and .46/.92 kg N counterexample and distinct-ID/column/order controls |
| `fertilizer_rate` | transformations.py:6743, numerator:6843; join keys:6870 | test_863_transforms.py:406-466 | Run ordinary country-less consumer controls; pooled rate's own country-omitting join remains a separate residual |
| Actual cache-hash census | slurm_logs/framework_campaign_2026-10-20/1143/hash_reach.py | prior 1093/1143 evidence | Reuse exact country/wave/tagged census in separate base/patch processes; measure rather than assume zero |

## Section 3 / §3 Definitions and conventions

- Country is a prepended output index level for `Feature`, independent of the number of selected countries: `docs/guide/feature.md:33`; `AGENTS.md` Core API; cross-country-features skill section 5.
- Canonical plot output name is `plot_id` for either accepted input spelling: `CLAUDE.md:350`, transformations.py:3548-3589 and `lsms_library/data_info.yml:133`.
- Declared mass lookup and explicit argument/attrs/empty behavior are defined by `_declared_kg_factors` and the #1093 ledger sections 3-4; this issue changes neither their precedence nor metadata meaning.
- Nitrogen shares, exact normalized matching, organic refusal and `nitrogen_input_match` diagnostics remain those of `nitrogen_kg` and GH #867; no new scientific input is authorized.
- Existing country-less reducer return shape is `(t, i, plot_id)`; metadata such as attrs['country'] alone must not fabricate a new country axis on ordinary Country frames.

## Section 4 / §4 Invariants and assumptions

- Inherit STANDING section 4, sanctioned IO, pandas 3 rules and matching code/config root identity.
- Grouping keys for valid country-labeled plot inputs become `(country, t, i, plot_id)`; country is first even when supplied as a column or in another index position.
- The same shape applies to one labeled country, no matching item rows, unknown-unit refusal, explicitly disabled declarations and empty input carrying those axes.
- Keep country-less outputs, missing quantities, missing local-key behavior, supported plot aliases and existing diagnostics unchanged.
- Promoting country must not turn the existing no-local-group-key error into a country-only sum; country alone is not a plot key.
- Country-column promotion must happen before filtering/conversion; reattaching by `.loc` after filtering would duplicate colliding index rows and is prohibited.
- Do not import Country or build data inside a reducer, change shared factor machinery, or apply a country-wide numeric factor to another country's rows.
- A reducer's new country axis does not cure `fertilizer_rate`'s separate country-omitting area join; pooled rate acceptance is outside the live issue's two-function scope.
- Source and cache fingerprints must be measured, including tagged closures and GhanaLSS import closures; read-time intent is insufficient evidence.

## Section 5 / §5 Reuse decisions and acceptance plan

| Quantity / behavior | Decision | Reason |
|---------------------|----------|--------|
| Seed and nitrogen mass | Reuse existing arithmetic and declared resolver | The bug is reduction identity, not conversion or nutrient content |
| Country column normalization | Extend using existing set_index append pattern | Preserve positional row identity under colliding local indexes |
| Plot reduction keys | Extend existing reducer groupby; small shared helper if useful | Country is an identity dimension, not a new estimator |
| Plot alias normalization | Reuse existing helpers | Preserve a single public plot-axis spelling |
| Output documentation | Extend both docstrings and Feature guide | Existing docstrings only promised three levels and do not define pooled behavior |

Tests must fail at untouched 852 before code changes: metric A/B same local IDs; country index/column; reversed country order; distinct IDs still retain country; one named country; empty input and filtered/unknown-unit empty results; declared per-country factors with shared IDs via attrs and explicit argument, including explicit {} suppression.
Compare each country slice of pooled output against its ordinary separate call and verify input frames/attrs are not mutated.
Cover plot alias, unit index/column, nitrogen matching/refusal tally and existing country-less fertilizer_rate consumers.
Run existing #1093 factor-consumer, plot-axis, nitrogen-content, #863 fertilizer and relevant factor tests within four pinned cores.
Synthetic public-contract evidence needs no real-country microdata or new scientific assumption.
Record exact aggregate source/hash and test evidence under slurm_logs/framework_campaign_2026-10-20/1159/.

## Section 6 / §6 Open questions for the parent

The required pooled-output choice is resolved: retain country.
Proposed one-country/empty rule is axis presence, not cardinality; no further human choice is needed for that interpretation of Feature's contract.
Parent reviewed/surfaced this ledger and authorized implementation.
Present but null country labels must raise an actionable ValueError before filtering/reduction; parent explicitly resolved this routine malformed-input question without changing existing local-key or quantity behavior.
`fertilizer_rate` independently omits country from its own join keys; keep that residual visible without silently broadening this issue.

### Phase 3 -- verification

- `_plot_input_country_index` -- OK (sections 2-4): reuses the existing column-promotion pattern and index-before-column precedence. It adds no axis from attrs, refuses null country labels before filtering, and preserves the old error when no local grouping index exists.
- `seed_kg` / `nitrogen_kg` -- OK (sections 3-5): country participates in reduction identity for index/column inputs, one country and empty paths. Local-ID collision and reversed-order controls now equal separate Country-shaped calls. Plot aliases, local missing-key behavior and input immutability remain pinned.
- Declaration and nutrient machinery -- OK (sections 2-4): no resolver, conversion, nitrogen-share, organic-refusal or tally implementation changed. Explicit/attrs/flat factor maps, explicit-empty refusal and real Feature assembly with colliding IDs pass.
- Documentation -- OK (section 3): both reducer docstrings and the Feature guide state the presence-based output contract. The inherited nitrogen docstring's claim that all-unconvertible plots sum to zero contradicted its existing dropna path; prose now describes existing empty behavior without changing missing-quantity policy.
- Negative proof -- OK (section 5): final regressions run against an exact source-verified untouched 852 runtime give 71 failures and 18 passes; 26 unrelated existing factor cases are deselected. Candidate selection gives 260 passes, two private-cache-gated Nigeria skips and 15 requires_s3 deselections.
- Downstream compatibility -- OK within scoped section 4: ordinary country-less fertilizer-rate tests pass unchanged. Old/new synthetic pooled rate remains 0.276, with separate A=0.46/B=0.23; its independent country-omitting area join remains an inherited residual and is not changed here.
- Cache reach -- OK (section 4): actual before/after census has zero changes among 549 country-table, 2,139 wave and 39 tagged hashes, with no census errors. Recorded source SHA256 values match untouched 852 and committed implementation exactly.

Implementation: `da84c7c790c1b1e040aca5ec3ff4d46649eb1317`, published by the post-commit hook.
Aggregate/reproducible evidence: `slurm_logs/framework_campaign_2026-10-20/1159/README.org`, `comparison.json`, `CONTRACT-REVIEW.md` and scripts.
No real-country microdata or new scientific premise was required.
