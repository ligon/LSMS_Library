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

Extend only the community-specific label path; keep shared `_canon_unit` unchanged. Defer and accurately word reporting via existing `_canon_units`. The ten-label fix has a complete source/actual-helper conservation proof and 31 private positive/adversarial controls; ten production recovery expectations fail on unchanged f8. Implemented verification is complete; see section 7 and the safe task report.

## 6. Approved partial scope and residual

Root approved the ten community-only aliases and reporting AFTER metric rescue with default-preserving `_canon_units` disposition. Future genuinely unplaced labels retain the existing NA/drop policy, now reported accurately. No observed positive recorded residual remains in held W3-W5 after aliases. This is PARTIAL work on #1095: its literal "instead of dropping" intent remains a Tier C retention/fallback design residual. No fallback, new unit meaning or global unit expansion is authorized.

The retention/fallback decision remains a future Tier C residual, explicitly preserving the original issue's literal retention intent.


## 7. Verification against the ledger

Runtime/evidence commit: `e55de22856064ab3b01f1bc5fcfac681f2f3ebc8`.

- **OK (sections 2-4):** exact community-only label extension reuses `_strip_code_prefix` and existing Preferred Labels. AST checks preserve all shared harvest/input/food functions and both size algorithms. Existing reporter defaults are pinned exactly; future community unplaced labels are reported AFTER rescue with accurate existing-drop disposition.
- **OK (sections 3-4):** actual normal empty-L2 cold builds preserve all 131,338 old rows, dtypes, null masks and attrs across native/helper, wave, raw-country and Country layers; recover exactly 5,228 unique source cells without collision/fanout; and preserve exact old metric-drop source masks (14,603 W3, 0 W4, 133 W5). No factor, size, eligibility, reducer or grain change.
- **OK (sections 2-4):** fresh-process warm Country/Feature frames and all private cache bytes/mtimes are exact. Healthy Niger remains 46,043 rows and three cache files byte-identical. Feature grows 177,381 to 182,609 with every old row and full attrs exact. All six scanner findings are accounted and multiset-exact after only root-path normalization; no errors/failures/skips.
- **OK (sections 2, 4):** 60 focused tests pass with no skips; ten actual production-builder recovery controls fail on f8. Accurate deferred reporting, all ten labels, malformed/missing/bare-code units, eligibility and size/factor boundaries are covered. API audit passes.
- **OK (sections 2, 4):** actual complete 549-cell census has 24 Nigeria hashes move, 525 others unchanged, and all 39 tagged framework fingerprints unchanged. Other Nigeria table outputs are not claimed verified.
- **OK (section 4):** final claims use a second physical private verified root after an interrupted initial selector briefly overlapped the first cold run. Preparation gated cold, warm and Feature phases; no other consumer overlapped each final data root. All 24 complete final frames/attrs match the first successful results. Source receipts verify 13 held blobs / 28,426,011 bytes per root, including unchanged-byte CSV text-key verification. Initial artifacts/logs remain private.

See `slurm_logs/isa_backlog_2026-10-21/Nigeria/1095/REPORT.org`, `validation.json`, `REPRODUCE.sh` and `DRAFT-RESIDUAL.md`. Final tracked-content/mode/head/status and process-quiescence proofs are private; independent review follows freeze. No GitHub writes, source acquisition, shared-cache mutation, fixture/golden edit or framework change.

Sue
