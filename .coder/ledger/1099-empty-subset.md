# Prior-Art Ledger -- GH #1099 empty absent-wave subsets

Inherits `.coder/ledger/STANDING.md` sections 2-5, `CLAUDE.md`, and `lsms_library/data_info.yml`; this ledger records only task-specific decisions.
Search tier: ripgrep + git + complete GitHub #1099 body/comments (none as of 2026-10-20).
Base: development 976fc840d.

## §1 Task, restated

For a country-declared DataFrame table, a strict subset of valid country waves that do not declare that table should return the declared empty frame consistently on the default backend, whether the country parquet is cold, warm, coverage-short, stale or bypassed with LSMS_NO_CACHE. Preserve other waves' existing country-cache content, genuine country-only builders, failed source exceptions, and actionable unknown-table errors. The make backend's no-country-builder route already supplies the precedent; do not revive retired DVC stages or invent absence from a source read failure.

## §2 Existing machinery

| Symbol | Anchor at base | Existing role / tests | Decision |
|---|---|---|---|
| `_empty_declared_frame` | `country.py:1050` | Declared index and required/optional columns; `test_gh1039_make_backend_no_country_rule.py:310` | Reuse, followed by normal finalization |
| `Wave.data_scheme` | `country.py:1166` | Wave script stems plus YAML keys; absent wave folder means no declarations with diagnostic | Reuse to distinguish absent configuration from failed source |
| `Country._has_country_builder` | `country.py:4883` | Conservative script/Make-rule probe against a private absent target; grouped rules supported | Reuse, never override an existing country builder |
| `Country._restrict_to_waves` | `country.py:4355` | Read-time subset filter; full-country request preserves NULL-t rows | Preserve |
| `Country._read_l2_country_cache` | `country.py:4436` | Hash, coverage and degraded gates; returns union coverage on misses | Preserve; do not restamp or overwrite a warm nonempty parquet with an absence answer |
| `Country._read_assumed_fresh_cache` | `country.py:4635` | Explicit trusting shortcut with requested-wave restriction | Preserve contract |
| `load_from_waves` closure | `country.py:5212` | Builds declared waves; default `country_builder=True`; no-builder make branch returns shaped empty at5360 | Extend absence routing only |
| `load_dataframe_with_dvc` closure | `country.py:5439` | On miss unions request+cached coverage, builds and writes country parquet | Avoid descending/writing when request alone proves absence |
| `Country._finalize_result` | `country.py:3813` | Existing schema, dtype, panel-id and geography harmonization | Reuse; test empty dtypes/index match warm serve |
| Existing synthetic Fakeland fixture | `tests/test_gh1039_make_backend_no_country_rule.py:117` | Private countries/data roots, two waves, second absent table; existing default case plants parquet and succeeds accidentally | Extend meaningful tests here or new standalone fixture using the same pattern |

## §3 Definitions and conventions in force

- Source of truth: STANDING sections3-4, canonical `data_info.yml`, country `data_scheme.yml`; no schema duplication in runtime logic.
- #1099 body: request for a wave that does not declare the table must answer the declared empty frame cold, warm, and under LSMS_NO_CACHE.
- #1039 ruling and existing test at `test_gh1039_make_backend_no_country_rule.py:302`: a real country-level builder retains its route; absence at wave level alone cannot prove country-level absence.
- `Wave.data_scheme` is the existing declaration test used by `build_wave` at `country.py:5237`; absence never inferred from an exception or empty source result.
- `load_dataframe_with_dvc` comments at5470: coverage union is monotone; a subset absence answer must not destroy existing warm coverage.

## §4 Invariants and assumptions

- STANDING section4 IO, cache, dtype and config-root invariants apply.
- Empty answer is only for a nonempty strict subset of known country waves, a country-declared non-JSON table, no declaration in any requested wave, and no genuine country builder.
- Full-country requests that produce no rows still fail; a requested wave that declares the table but has an unavailable/invalid source still fails through the existing path.
- A coverage-short or bypassed warm cache may contain valid data for other waves; absence answers must preserve its bytes/manifest rather than write an empty replacement.
- Unknown requested waves, absent country-table methods and JSON panel mappings retain existing dispatch behavior.
- Genuine country-builder fallback behavior is preserved; correcting its independent wrong-wave behavior is outside this narrowly scoped fix.
- Hash impact must be measured: edits to the tagged `_aggregate_wave_data` are expected to invalidate corpus fingerprints; no cache-cost optimization justifies changed semantics.

## §5 Reuse decision

| Need | Decision | Reason |
|---|---|---|
| Build empty result | Reuse `_empty_declared_frame` plus `_finalize_result` | Already tested and schema-driven |
| Detect no source declaration | Reuse `Wave.data_scheme` and valid-wave subset check | No source reads or guesses |
| Detect country fallback authority | Reuse `_has_country_builder` | Existing conservative Make/script protocol handles grouped rules |
| Preserve cached other-wave content | Return proven-empty answer before rebuild/write descent | Does not shrink cache manifest or launder stale fallback |
| Missing-source behavior | Preserve all current exceptions | Absence condition excludes declared-wave builds |

## §6 Questions for dispatcher authorization

Proposed narrow boundary preserves existing genuine country-builder behavior, including the independently recorded wrong-wave fallback surface; this issue fixes only no-country-builder absent-wave subsets. Confirm this boundary while surfacing the ledger. No further human decision is required for the issue's stated behavior.

Finalization must be measured on empty frames: if the existing helper/finalizer cannot reproduce declared types/index, extend only the existing helper's empty dtype handling with a targeted test rather than a parallel constructor. Report that necessity before broadening schema semantics.

### Phase 3 -- verification

- `absent_subset_frame` -- OK (sections2,4,5): reuses declaration and country-builder machinery; called only after existing cache-hit opportunities or at a wave-build descent. It never turns a source exception into absence.
- Existing `_empty_declared_frame` / `_finalize_result` -- OK (sections2,5): reused without changing dtype behavior; real Senegal result is0x3 with the existing Float64 schema and `(i,t,animal)` index.
- Cache preservation -- OK (section4): cold/all/partial/stale/no-cache matrix checks bytes; real2018-only and all-wave manifests survive a2021-22 empty request.
- Warm read path -- OK (sections2,4): independent review caught a provisional eager guard; two new tests reject additional wave declaration inspection on ordinary and assumed-fresh covered strict-subset hits. The guard is now lazy and both pass.
- Error boundaries -- OK (section4): source failure stays FileNotFoundError; invalid wave stays KeyError; unknown method stays AttributeError; full build with no source stays RuntimeError; real country-builder control unchanged.
- Regressions -- OK (section4):15 new cases; original aggregation replay4 failures /11 passes; final code15 passes. Routing/cache/DVC group136 passed /10 cold-root skips. Fingerprint/schema/API guard group 352 passed after the final lazy placement.
- Cache impact -- OK (section4):4/4 build fingerprints,11/11 country hashes,58/58 wave hashes move. Corpus re-warm is expected; no shared L2 was cleared or rebuilt.
