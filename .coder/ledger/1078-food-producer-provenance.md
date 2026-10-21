# Prior-Art Ledger -- GH #1078 food producer provenance

**Search tier used:** ripgrep + Git + direct source/AST inspection. This ledger inherits `STANDING.md`; line anchors below are the initial f8 baseline.

## §1 Task, restated

Preserve the actual producer route of each `Wave.grab_data` result so `Wave.food_acquired` cannot decide how to transform an already-read frame from the later existence of its mutable wave cache. Keep the default public DataFrame interface. Restore naturally corrupted current-hash Uganda country caches through normal content-hash invalidation. This is the approved framework-only producer/versioning boundary, not a source, canonical-food, unit, currency, household-ID or nutrition change.

The frozen private diagnosis reproduced the exact historical six Quantity and three Expenditure NaN-to-zero cells on f8 and reviewed #1062 with real normal evictions and actual writers. The original historical full-suite writer/interleaving remains unidentified. Diagnosis evidence under `control/isa-backlog-20261021/Uganda/1078` is immutable and must not be rewritten.

## §2 Existing machinery

| Symbol | Initial location | Existing behavior/tests | Decision |
|---|---|---|---|
| `Wave.grab_data` | `country.py:1773` | YAML cache/read/build and script/Make routes; cache/hash, script-path, schema tests | extend with keyword-only private per-result origin; default unchanged |
| `_read_l2_wave_cache`, `_fresh_script_parquet` | `country.py` | Read gates excluded from fingerprints; `test_build_transform_hash.py` pins that boundary | reuse unchanged |
| `Wave.food_acquired` | `country.py:2303` | Existing non-parquet numeric coercion and sum/first reducer; later file-existence gate races | extend to consume actual private origin, preserve reducer |
| `_evict_hashless_wave_caches` | `country.py:4709`, call `:5268` | GH #479 forces script rebuild on stale/absent country cache | reuse unchanged; do not suppress or lock around it |
| `to_parquet` / atomic write | `local_tools.py:3225`, `:3012` | Sanctioned redirected writer, unchanged Arrow inputs and atomic bytes | reuse unchanged |
| `build_transform` | `_build_registry.py:65` | Scoped no-wrapper tag; pinned entry points and closure/version tests | add approved food-scoped tag to the content-producing Wave method |
| `Wave._input_hash`, `Country._table_cache_hash` | `country.py:1190`, `:4184` | Existing hash/coverage gates; `test_cache_hash_invalidation.py` | reuse natural invalidation, never stamp unverified content |
| Uganda canonical reshape / source fields | `Uganda/_/uganda.py:440`, eight food drivers | Source-faithful frame; original source oracle independently proves nine missing operand sets | reuse, no edits |
| Default derived food / Feature assembly | `Country._FOOD_DERIVED`, `transformations.py`, `feature.py` | Standing API, source/value/unit semantics and Feature audit harness | reuse, scoped full-frame/attrs comparisons |

No existing per-result producer indicator is returned by `grab_data`. A new private origin is needed because path/config observations cannot establish which route actually supplied this frame. No public attrs sentinel, mutable global state or instance last-origin field is allowed.

## §3 Definitions and conventions in force

- Schema and derived-table definitions are `STANDING.md §3`, `CLAUDE.md` "Derived Tables", and `lsms_library/data_info.yml`; do not redefine them.
- Cached content is pre-finalize; the same public finalization applies after warm reads (`STANDING.md §4`). Keep every value, index/key multiplicity/order, dtype, axis and complete attr outside the defective nine cells exact.
- Source reads/writes use the existing sanctioned helpers (`STANDING.md §4`). Held sources require terminal physical-copy MD5/SHA/size/mode/mtime gates before builds. No acquisition, DVC CLI, shared-cache change or in-tree payload.
- Script-wave hashless policy and country stale policy differ (`Uganda/_/CONTENTS.org:1952`, GH #809). Hashless eviction remains necessary. A normal current country hash alone does not certify frame correctness.
- Uganda's 2009 hybrid GPS-bin/EA `v`, later parish/district-parish identities and household Region fallback remain country-specific. `CONTENTS.org:253` says Region is complete, but `:270` explicitly qualifies that with three missing 2019-20 Regions. `:279` says refresh "each wave", while `:689` quotes the same 2013-14 sample used in 2015-16. Live panel/GPS/null-key and WAITING #829 caveats remain unresolved.
- Initial current country CONTENTS is byte-identical to the previously fully read f8 document (blob `6bad560bef33eb6b2b9decaf6f96c901f67bf67b`, SHA256 `774d5bc13c35ca5a6d9543766648f88dbc1efe508d73aa388d5ac6e81146256c`). Only an additive producer-race note is approved.

## §4 Invariants and explicit route distinctions

- Default `Wave.grab_data(request)` still returns a DataFrame, with unchanged values and attrs. Only `Wave.food_acquired` opts into a keyword-only private origin return.
- Origin comes from the actual return route: parquet read, successfully YAML-materialized frame, in-memory frame, or missing/empty route. No later/earlier `exists()` or `!make` declaration substitutes for producer provenance.
- A successful script read, including the post-Make candidate fallback, stays a parquet producer if another process subsequently removes that path. Successful YAML materialization likewise does not become an in-memory producer after deletion.
- YAML write failure and cache-disabled YAML output remain in-memory producers. An unrelated old file must not make them parquet-produced. This intentionally differs from the old existence heuristic when a stale/unrelated path remains; report and test that distinction explicitly. Cache-disable is exercised only as a focused synthetic route contract, never as source-validation setup or healing bypass.
- Preserve the exact legitimate non-parquet reducer: selected Expenditure/Quantity/Produced sums, Price first, numeric coercion, original group keys/default aggregation behavior. No global min_count policy change.
- `Wave.food_acquired` is currently untagged and absent from all measured relevant closures. Add its approved food-scoped tag. Do not alter read-gate exclusions or global schema to disguise broad cost.
- `grab_data` is already tagged for every table. Its private plumbing can move broad hashes. Measure actual 549 country API hashes, 39 per-table framework fingerprints and relevant wave hashes against immutable f8, in separate exact-source processes before source rebuilds. No whole-corpus rebuild is authorized.
- Natural healing uses a physical receipt-verified copy of a genuinely corrupt current-hash f8 root. The ordinary candidate read must grade it stale and rebuild, restoring all nine NaNs and every other full payload/attr without clear/stamp/bypass/replacement.
- Repeat the real two-process schedule with actual eviction and unchanged real frame/write inputs; every writer/output must be correct. Original diagnosis roots and all source/reference receipts remain immutable.
- This f8-based implementation does not establish coexistence with #1062 or other campaign fixes. Composition requires a separate exact-source proof.

## §5 Reuse decisions

| Quantity/behavior | Decision | Reason |
|---|---|---|
| Per-result origin | new private metadata return | No existing result carries actual producer information; do not leak attrs or use mutable state |
| Loading, canonical schema, derived arithmetic, IO and eviction | reuse | Existing tested machinery already defines the data and cache contracts |
| Wave food compatibility transform | extend | Producer-dependent routing changes; legitimate non-parquet reducer remains exact |
| Cache invalidation/healing | extend existing tag registry | Current unversioned producer otherwise leaves corrupt current-hash caches served indefinitely |
| Cross-country/source/warm checks | reuse audit harness and held-reference comparisons | Actual outputs and complete findings are the oracle, not green test counts |

## §6 Open questions / approvals

Root explicitly approved this bounded producer-return design, food-scoped tag, broad hash measurement and scoped source validation before this ledger. No additional human decision blocks implementation. Broad invalidation must remain visible for human PR review; neither whole-corpus rebuilding nor later composition is authorized here. Independent red-team review is required before a final PASS.

### Phase 3 -- verification

Pending implementation and anchored verification. Keep original historical attribution as an explicit residual.
