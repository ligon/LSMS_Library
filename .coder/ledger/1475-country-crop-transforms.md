# Prior-Art Ledger — GH1475 / GH1517: country identity in crop transforms

Search tier: ripgrep + git + direct source/test inspection; no GitNexus query tool is exposed. Base: `c964c6c4f25945dc14636fd094893b16b12180b7`. Inherits `STANDING.md`, `CLAUDE.md` and the canonical schema.

## §1 Task, restated

Retain supplied row-level country in `harvest_kg`, `yield_kg`, `crop_diversity` and `gross_crop_revenue`, including one labeled country and empty outputs. Prevent local-key collisions from combining unrelated countries. Yield must use each labeled country's existing land route, including Uganda's validated native constructor inverse after household walking, without allowing stale scalar attrs to authorize another country's decode. Ordinary country-less behavior, native operands and scientific definitions remain unchanged.

The eight delegated country-owner assessments preceded this implementation. Uganda, Ethiopia and Malawi owners read their full country records; their published queue/report hashes are in the coordinator registry. Root reuses those findings and has read the relevant crop/land entries directly. This is a shared transformation task, not a new country-source diagnosis; no new source coverage/count claim follows from those earlier assessments.

## §2 Existing machinery

| Symbol/evidence | Existing role and tests | Decision |
|---|---|---|
| `_plot_input_country_index`, `seed_kg`, `nitrogen_kg` | GH1159 row-country promotion/non-null/presence contract; `test_gh1159_pooled_reducers.py` | Extend/reuse normalization, preserve its local-key guard |
| `fertilizer_rate` | GH1160 requires country on both operands or neither; joins country/local land keys; index outranks redundant country column; per-row land dispatch | Reuse these paired-operand rules in yield; do not change fertilizer behavior |
| `_level_or_column`, `_one_country` | Index-before-column identity; labels outrank stale scalar attrs | Reuse |
| `_kg_factor_series`, `harvest_kg_factors` | Existing inferred factor machinery and reported/shipped/survey/metric precedence; `test_crop_kg_factor.py`, shipped/declared tests | Preserve arithmetic; isolate crop inference by row country before estimating |
| `_survey_median_factors`, `_declared_kg_factors`, shipped joins | Already country-aware; reported factor support and declarations | Reuse unchanged |
| `_parcel_from_crop_plot`, `_parcel_from_feature_plot` | Validated Uganda constructor inverse and former fallback; `test_gh1205_uganda_parcel_prefix.py` | Reuse; authorize Uganda inverse from its own row labels, not pooled attrs |
| `_YIELD_LITERAL_LAND_COUNTRIES`, `_YIELD_LAND_ROUTE_HELD` | Existing whole-key routes and explicit Benin/Guinea-Bissau/Togo hold; `test_gh1460_yield_land_route.py` | Reuse exact sets |
| `_area_on_harvest_rounds`, `pp_ph_area_t` | Explicit PP→PH round mapping and collision refusals; `test_gh1460_yield_area_t.py` | Preserve; map/check labeled area within its country |
| `_waves_joining_nothing` | GH1545 partial-loss warning, currently reconstructs country through local `(t,i)` | Extend to use retained country directly; preserve actual lost-wave warnings |
| `_without_redundant_farm_level` | Already includes country in crop occurrence identity | Reuse unchanged |
| `Feature`, `_harmonize_country_frame` | Prepends country and resolves crop/plot aliases; cross-country skill / audit harness | Exercise actual synthetic assembly, do not rebuild country data in first phase |

## §3 Definitions and conventions

- Country is an identity dimension whenever an index level or column supplies it, independently of cardinality. Attrs alone add no output axis: GH1159 ledger §§3–4 and Feature guide.
- Existing GH1160 paired-frame rule is country on both operands or neither, with actionable refusal for asymmetry/null labels. An index label takes precedence over a redundant column and scalar attrs.
- Uganda's documented limit is deliberate: `Uganda/_/CONTENTS.org:4543` says “An explicit country index level or column disables the new fallback”. GH1517 requests safely extending this limit, not dismissing it as stale. Keep the protection against stale scalar metadata and malformed suffixes.
- Ethiopia land keys retain holder/parcel/field; Malawi later keys retain garden/plot. Complete keys are not coarser holder/garden identifiers: cross-country skill and `_YIELD_LITERAL_LAND_COUNTRIES`.
- Malawi `plot_features.Area` is hectares while `AreaUnit` retains native acres (`Malawi/_/CONTENTS.org:1872` onward). No area conversion changes here.
- Crop count diversity is land/season occurrence entropy, value diversity uses reported positive sales, gross revenue is nominal reported sales, and unknown plots/conditions retain existing contracts: current function docstrings and GH1460/1469 tests.
- Ethiopia's older statement that parcel fertilizer rate is universally empty (`CONTENTS.org:1559`) predates later labeled whole-key dispatch. Preserve both stages, do not use the old statement as authority to undo the current route. Fertilizer remains outside this implementation scope.

## §4 Invariants and assumptions

- Inherit sanctioned IO, pandas 3, private caches and exact code/config identity from STANDING §4. No raw/harmonized source or shared-cache writes in this initial implementation phase.
- Country-less outputs/arithmetic remain exact. Labeled results prepend country, including one-country and empty cases; country alone cannot satisfy an existing local-key requirement.
- Conversion, filtering, unknown-plot disclosure, farm-level occurrence suppression, grouping and joining must all retain the same row identity. Reattaching country after a collapsing reduction is unsafe.
- Crop inference must equal separate-country inference under identical local keys; never change food inference or adjacent plot-input policy as a shortcut.
- Per-country declared/shipped weights, survey totals, support floors, screening and kilogram precedence remain unchanged. Exact kilograms are not inferred factors.
- Whole-key routes and the three held countries stay exactly as ruled. No missing/nonpositive area filling, area/unit redefinition, guessed plot parsing or unknown-country inference.
- For country-labeled areas, PP/PH source/target conflicts are checked within country, so another country's round name cannot create false refusal or authorize mixing.
- Preserve existing disclosure scope and real warnings. Old tests that deliberately pin the now-fixed pooled failure require explicit new oracles, while unrelated stale-attr, malformed-key and country-less controls remain intact.
- Independent review, full hash census, exact-head offline CI and bounded real source/API acceptance remain separate gates. No master release or shared re-warm.

## §5 Reuse decisions and verification

Reuse the GH1159 country-index normalization pattern and GH1160 paired land-key/row dispatch contract. Extend the normalizer through one shared private core only if needed to preserve the plot-input-specific guard without imposing it on household crop reducers. New crop-only country partitioning of the existing inferred-factor call is justified because the existing generic unit helper returns one pooled unit mapping and is also used by plot inputs outside this task.

First establish untouched-base failures with synthetic colliding keys, genuine independent-country reference outputs, reversed/index/column/singleton/empty controls, actual Feature assembly and a real price-inference discriminator. Then implement minimal shared changes, preserving source inputs and attrs. Cover per-row declared/shipped factors, native kg, unknown plots/farm rows, Uganda panel-walk inversion, whole-key and held land routes, positive-area masks, source/round mismatches, warnings and unsupported identities. Update only explicit former-defect oracles, explaining why; never weaken old valid protections. Measure actual hash scope with the campaign's existing metadata-only machinery and mutation positive control; then independent review/offline CI/source checks on frozen commits.

## §6 Open questions

The supplied-country identity rule is already ruled (GH1159) and the paired-land-frame identity policy already exists (GH1160); no new scientific choice is needed. If a source-specific ambiguity or external-key harm appears, stop that product progression and report concrete evidence rather than invent a compatibility fallback. Malawi weight policy48, food policy49/50, date51, fertilizer1485 and food-inference1509 remain outside this repair.

### Phase 3 verification

Pending; this ledger precedes product/test implementation. No test/build/source-success claim yet.
