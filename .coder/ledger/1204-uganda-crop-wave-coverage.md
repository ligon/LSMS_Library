# Prior-Art Ledger - GH1204 Uganda requested-wave crop coverage

**Search tier used:** ripgrep + git + Python stdlib static inspection. No GitNexus or cross-session memory tool is exposed. Source grounding is anchored to actual parent `491daf5654b9ee45332bec97e1296ae821ff46d5` (reviewed PR1207 over PR1202). This initial ledger precedes diagnosis, production edits, and project/data imports.

## §1 Task, restated
Repair ordinary `Country('Uganda').crop_production(waves=...)` routing for supported and genuinely barren requests, under Default and Make, including truthful raw country-cache contents and coverage. Preserve the reviewed native observation grain and twelve factor bindings. A public-only filter or invented coverage is insufficient. Root must approve the source-bound proposal before implementation or held-source runs.

## §2 Existing machinery

| symbol / path | role | existing tests | decision |
|---|---|---|---|
| `country.py:4893 Country._has_country_builder` | Real script / GNU Make rule discovery; selects existing framework wave assembly when no country builder exists | `test_gh1039_make_backend_no_country_rule.py::test_real_country_builders`, `test_multi_target_country_rule_keeps_the_make_route` | reuse; retirement is not yet assumed sufficient |
| `country.py:5249 load_from_waves` | Wave declaration/build, ID walk, normalization, concat, country fallback | `test_gh1039_make_backend_no_country_rule.py`, `test_gh866_stale_country_parquet_not_laundered.py` | reuse; trace every caller in phase0 |
| `country.py:5004 absent_subset_frame`; `country.py:1050 _empty_declared_frame` | GH1099 declared typed empty result without replacing other waves' country cache, when no country builder exists | `test_absent_subset_is_typed_and_preserves_other_cached_waves`, `test_covered_subset_cache_hit_does_not_construct_waves` in GH1039 tests | reuse |
| `country.py:4317 _cache_covers_waves`; `local_tools.py:3103 read_parquet_cache_waves`; `local_tools.py:3225 to_parquet` | GH891 coverage manifest and monotone requested/cached union | `test_gh891_wave_subset_cache.py` coverage/union/request tests | reuse; measure actual raw result and footer |
| `country.py:4738 _evict_stale_country_cache` | GH866 stale fallback target protection | `test_gh866_stale_country_parquet_not_laundered.py` | reuse |
| `country.py:5245 _replay_wave_stamp`; `country.py:7827 _replay_wave_stamp`; `country.py:7792 _replay_grain_audit` | Existing wave-to-country grain/date/drop transport, including out-of-process scripts | `test_gh868_reduce_to_agreed_ledger.py`, grain tests | reuse; prove all relevant routes |
| `country.py:5352`, `country.py:3874`; `local_tools.py:3636 id_walk` | Framework ID conversion and finalizer use closure-resolved maps plus `id_converted` | `test_id_walk.py::test_id_walk_idempotent_on_two_step_chain`, `test_id_walk_sets_id_converted_attr` | reuse; compare country-local walk and reviewed map |
| `countries/Uganda/_/crop_production.py`; `countries/Uganda/_/Makefile:103-107` | Legacy all-wave country concat, local `uganda.id_walk`, wave-grain audit fold, raw var write | existing held-source GH1198/GH849 evidence; new actual-parent control required | investigate authority and transport before choosing edit |
| `countries/Uganda/_/uganda.py:679 id_walk` | Country-local one-pass wave-scoped renamer differs in implementation from generic closure-resolved walk | reviewed `updated_ids.json`, existing ID tests | verify actual map equivalence; no speculative rewrite |
| `countries/Uganda/2018-19/_/crop_production.py` and six sibling loaders | Retained maximal production observations with literal source-fingerprint references and per-wave audit write | `test_gh1198_uganda_production_observations.py`, `test_gh849_uganda_reported_factors.py` | preserve unchanged |

## §3 Definitions and conventions in force
Inherit `STANDING.md` §§3-4, `CLAUDE.md` cache/read-only source/ID/grain rules and `lsms_library/data_info.yml` canonical schema. Full Uganda `_/CONTENTS.org` read, including TODO/WAITING/LOGBOOK, before other task material.

- GH1198 dated section: `production_obs` follows `season`; source IDs, actual condition slots, and explicit release-row identities are distinct source observations, even with equal payloads. Preserve the legacy conditional null-key boundary and all eligible public null-v records.
- GH849 dated section: the twelve reported-factor bindings are reviewed, with the existing numeric/nonpositive handling and downstream screen unchanged. Older `KgFactor` text says "Wired for 2018-19 only, deliberately"; the 2026-10-22 addendum says "The five previously unwired rounds now carry their own reported kg-per-unit fields". Preserve both as dated evidence; do not silently reinterpret the current baseline.
- 2013-14 AGSEC5A unit/condition variable labels are swapped; value vocabularies govern. 2018-19 AGSEC5A asks a unit but the extract has two condition columns and no harvest-unit column. Mixed legacy/WB names occur within one file.
- GH829 remains WAITING: the measured 2015-16 sale-value x100 correction is producer-undocumented and conditional on the held release; no rescaling change here. A/B are module/visit labels, not universal calendar-season order.
- GH1204 is explicitly recorded under GH1198/GH849: Make supported/barren subsets return all waves. Default final-result emptiness alone does not prove correct raw-cache coverage.
- The old "100% of crop rows" parcel-match claim is contradicted by the dated held-source account; GH1205 area/key repair is separate. Population, sampling, recall and derivation attrs are typed carriers; finalized all-wave filtering is not a full-attrs subset oracle.

## §4 Invariants and assumptions
Inherit `STANDING.md` §4. No cache bypass/stamping/promotion, source acquisition, new reducer, factor estimator, eligibility change, area repair, or fixture refresh. 2005-06 has area allocation without crop-level harvest quantities and must stay genuinely barren. Compare actual491 counts by stage rather than forcing older issue counts. Every project/numerical/data actor needs a fresh offline namespace and pre-import socket/write guards, exact controller AND pytest-worker source/config/data identity, private roots and one-thread/build/Make pins. Provider gates precede held-source copy/decode. Preserve all stops and guard events privately. The old Malawi1209 evidence is immutable.

## §5 Reuse decision

| behavior | decision | reason |
|---|---|---|
| Requested-wave assembly | reuse existing `load_from_waves` if transport proof succeeds | Framework already handles supported and absent requests; country retirement is only a hypothesis until phase0 trace and controls |
| Raw coverage and union | reuse GH891/GH1099 | No new footer format or public post-hoc filter |
| Canonical household IDs | reuse reviewed map and existing walk | Compare local and framework semantics on source metadata and targeted tests; no new ID rule |
| Native observation/factor/quantity semantics | preserve existing loaders/helper/transforms | GH1198/GH849 already own these definitions |
| Regression coverage | extend narrowly | Actual491 parent controls must discriminate both wrong Make output and wrong Default raw end state |

## §6 Open questions for root
- Pending phase0 source trace: can local country-builder retirement preserve all ID/audit/attrs transport while making the raw empty-state truthful? Root approval of the concrete proposal is required before production edits or held-source runs.
- No human scientific decision is requested: factors, area namespaces, partial-harvest slots and source caveats remain separate.

### Phase 3 verification
Pending implementation, guarded bounded verification and separate independent review. This ledger is grounding, not acceptance.

Sue
