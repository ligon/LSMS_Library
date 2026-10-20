# Prior-Art Ledger -- GH #1124/#1125 Feature audit participation

Inherits `.coder/ledger/STANDING.md` sections2-5, `CLAUDE.md`, and `lsms_library/data_info.yml`; no production Feature assembly change is authorized.
Search tier: ripgrep + git; GitHub issue1124 and1125 bodies/comments read completely (no comments on either as of2026-10-20).
Base: development976fc840d.
Skills: cross-country-features and bench/feature_audit/README.md; the existing per-task crop ledger supplies historical registration context.

## §1 Task, restated

The baseline Feature audit must distinguish an entire positive-row country's exclusion from row collapse among countries that remain in the assembled frame. Include registered crop_production, plot_labor and plot_inputs in the canonical sweep. Reuse the hardened warm-assembly helper for crop_production so an existing warm country whose read fails cannot be silently skipped and the final country participation set is checked exactly. Keep legitimate kwarg degradation and empty-country exclusions out of the new baseline exclusion signal.

## §2 Existing machinery

| Symbol | Anchor at base | Existing behavior / tests | Reuse decision |
|---|---|---|---|
| `CANONICAL_FEATURES` | `bench/feature_audit/scan.py:85` | Curated baseline canonical list currently omits the3 registered farm tables | Extend list only |
| `_scan_one_country` / `phase1a_country_sanity` | `scan.py:302/:363` | Only nonempty successfully built DataFrames contribute row counts; errors retain ClassA findings | Reuse per-country row map; no country source reads added |
| `_assembly_invariants` | `scan.py:425` | Checks names/canonical levels and row reduction only within represented countries at490 | Extend with separate per-country baseline exclusion warnings |
| `Finding` | `scan.py:167` | Stable per-feature/country/kwargs/check fingerprint, severity and numeric metrics | Reuse record format |
| `phase1b_assembly` | `scan.py:517` | Calls invariants with baseline empty kwargs and country row map | Preserve baseline scope |
| `_scan_one_kwarg` | `scan.py:551` | Calls invariants with `check_rows=False`; ContractB and unit modes legitimately exclude countries/change rows | Keep new participation check disabled here |
| `_warm_assembly` | `tests/test_feature_canonical_index.py:862` | Existing warm cache existence check, default hash-checked reads, named error assertion before fewer-than3 skip; returns counts/shape, output, grain delta | Reuse; align Country monetary read to Feature's default currency=index |
| crop warm test | `test_feature_canonical_index.py:565` | Currently independently uses trust_cache and broad swallow; only verifies survivors | Replace manual read loop with helper and exact positive-built-country set |
| plot warm tests | `test_feature_canonical_index.py:903` | Already use helper and exact participation + per-country counts | Reuse doctrine without changing production assembly |

## §3 Definitions and conventions

- STANDING sections3-4 and canonical `data_info.yml` remain authoritative.
- `cross-country-features` section3 and benchmark README distinguish legitimate additive collapse from lost survey facts; a baseline row delta remains a warning requiring triage, not an automatic corruption judgment.
- `cross-country-features` section5: Feature is country-axis counterpart of Country and defaults monetary `currency` to index; diagnostic per-country reads should use the same monetary representation.
- ContractB for labels/units legitimately excludes unavailable-country variants; phase2's disabled row checking is intentional, not a gap to repair here.
- Empty Country frames contribute no rows and are intentionally skipped by production Feature (feature.py:1076); do not call them excluded positive builds.

## §4 Invariants and assumptions

- New whole-country signal is baseline-only, considers strictly positive built row counts, names every absent country, and records its lost row count with ClassB severity.
- Keep existing within-kept row_conservation calculation separate: A2/B7 with result A2 should report B7 excluded, not pretend A lost7 through within-country collapse.
- If the result is completely empty, positive built countries are still absent; record them alongside assembly_nonempty rather than return before the new check.
- A missing country axis in a nonempty malformed result already has structural findings; do not guess attribution of surviving rows to countries.
- Kwarg variants retain their current ContractB handling; no new ClassB country exclusion under labels/unit/market/period variants.
- Crop warm check reads only countries whose country parquet exists, fails by name if any such read raises, compares the exact positive-built participation set, and rejects row loss within retained crop countries.
- No production `Feature`, Country, schema, country config or cache mechanism changes; expected fingerprint/cache impact is zero, measured rather than assumed.

## §5 Reuse decision

| Need | Decision | Reason |
|---|---|---|
| Detect entire country exclusion | Extend `_assembly_invariants` with a distinct `country_exclusion` finding | Existing row check is intentionally only within survivors |
| Preserve additive-collapse triage | Keep existing row_conservation check unchanged in role | Warning does not imply corruption |
| Handle intentional kwarg exclusions | Reuse `check_rows=False` and require no-kwargs baseline for new signal | Avoid false ClassB findings from valid ContractB degradation |
| Harden crop warm test | Reuse `_warm_assembly('crop_production')` | Already tested named failure and cache-presence doctrine |
| Validate scanner | New synthetic harness tests calling its existing APIs | No survey builds required to prove A2/B7 and kwarg behavior |

## §6 Questions for dispatcher authorization

No human semantic decision is needed: both issues explicitly request harness/test correction, and production assembly remains unchanged. Proposed check name is country_exclusion, one ClassB warning per positive built country absent from a baseline DataFrame's country axis (also cover an entirely empty result). Additional malformed-index policy is outside scope.

The issue wording says currency=index on the helper read, but current `_warm_assembly` calls Country with no explicit currency; propose adding currency='index' inside that shared helper so monetary Country and Feature reads align. This changes tests only, not the API.

### Phase3 -- verification

Pending surfaced-ledger authorization.
