# Prior-art ledger — GH #1095 Nigeria community price units

Approved implementation boundary, committed BEFORE code. Search tier: ripgrep + git, no generated-index mutation.

## 1. Task

Restore recorded C8 units that the current canonicalizer cannot place without changing package-size normalization, grain, eligibility, older rounds or other unit consumers. Actual loss is 2,482 W4 + 2,746 W5, independently remeasured from held sources.

## 2. Reuse inventory

| Existing machinery | Decision and evidence |
|---|---|
| `Nigeria/_/nigeria.py::_strip_code_prefix` | Reuse source code-prefix normalization; exact label-body aliases only. |
| `_canon_unit` | Preserve shared harvest contract; crop callers and `_canon_input_unit` depend on it. |
| `_canon_input_unit` | Preserve module-specific extension precedent; Metric Cup overlap is real, not evidence that global expansion is safe. |
| `_canon_units`, `UnplacedUnitLabelWarning` | Reuse reporting, defer until after metric rescue, accurately distinguish warning/drop from retention; existing defaults must remain exact. |
| `_c8_size_metric_grams`, `_c8_w3_size_text`, `community_prices_for_wave` | Reuse unchanged #834 normalization, deletion and Derivation rules, positive-price filter, reducer and i==v shim. |
| `categorical_mapping.org::u` | Ten Preferred Labels already exist; no new dictionary rows or code inference. |
| `Country._finalize_result`, normal Country/Feature routes | Reuse existing post-read coercion, canonical alignment, native axes and attrs; no framework changes. |
| Existing #1047 unit and Nigeria cluster/C8 tests | Reuse existing crop/input boundaries and metric-price/PH-only regression checks. |
| Feature audit harness phases 1/2 | Actual baseline six-record finding multiset captured and classified; no CP phase-2 grid exists. |
| `Country._table_cache_hash`, tagged fingerprints | Actual 549/39 inventory; real hash-input perturbation proves 24 Nigeria/525 other/39 framework scope. |

## 3. Definitions in force

Inherit `.coder/ledger/STANDING.md`, `CLAUDE.md`, current canonical `data_info.yml` and Nigeria CONTENTS #834/#1047/#607. C8 is PH-only, at cluster/item/unit grain with the existing shim. Reported package sizes may normalize prices only under the current #834 rule. No native label implies a physical kg/l factor. Missing is not a recorded, unrecognized unit label.

The historical #591 ledger's raw-food-unit instruction conflicts with the later #607 notes and current tested per-round food decode; report that chronology rather than applying the old prescription to this task.

## 4. Invariants

W1–W3 and all existing W4/W5 rows/keys/Price/Derivation/NA masks exact; no new collision/fanout; no changes to first policy, roles, grain, quantities, factors, item maps, cluster IDs, sample or household data. Preserve 14,603 W3 and 133 W5 intentional metric-size exclusions. Respect source codebooks and the 13 held-source integrity receipts, including DVC text hash normalization without byte rewriting. Never acquire, stamp stale caches or touch shared/frozen trees.

## 5. Reuse/extend choice

Extend only the community-specific label path; keep shared `_canon_unit` unchanged. Defer and accurately word reporting via existing `_canon_units`. The ten-label fix has a complete source/actual-helper conservation proof and 31 private positive/adversarial controls; ten production recovery expectations fail on unchanged f8. Actual implemented cold/warm/Feature/scanner validation is required before freeze.

## 6. Approved partial scope and residual

Root approved the ten community-only aliases and reporting AFTER metric rescue with default-preserving `_canon_units` disposition. Future genuinely unplaced labels retain the existing NA/drop policy, now reported accurately. No observed positive recorded residual remains in held W3-W5 after aliases. This is PARTIAL work on #1095: its literal "instead of dropping" intent remains a Tier C retention/fallback design residual. No fallback, new unit meaning or global unit expansion is authorized.

Full cold/warm native/helper/Country/Feature comparisons, six-finding audit accounting, unchanged healthy Niger, actual all-549 country hashes and all-39 tagged fingerprints, baseline failures and fault boundaries remain verification obligations.

Sue
