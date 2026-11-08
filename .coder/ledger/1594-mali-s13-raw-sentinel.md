# Prior-Art Ledger — GH1594 Mali 2014-15 purchased-food sentinel

**Search tier:** ripgrep + git + static Python source (no source-data access).
**Base:** 609b7720f8dbc72b85cb846fdd1f63e84d51f977. Inherits STANDING.md, CLAUDE.md and lsms_library/data_info.yml.

## §1 Task, restated
Apply the user's 2026-11-08 ruling that exact 99,999,999 is a missing-value sentinel to Mali 2014-15 raw s13q03c in BOTH EACIALI passages before the existing food_acquired aggregation. The authority is the user's interpretation, recorded on GH1594; the old producer-document diagnosis did not establish a sentinel code. Preserve native contribution rows and keys, all quantities, real NA, other acquisition sources, genuine paired 16,325 XOF and existing aggregation/Price/recall/weights policy.

## §2 Existing machinery
| Symbol | Path | Role / tests | Decision |
|---|---|---|---|
| food_acquired | countries/Mali/2014-15/_/mapping.py:84 | Existing source-stream reshape; no direct existing wave mapper test found; tests/test_gh1513_mali_padded_food_labels.py covers other-wave labels | extend only purchased raw expenditure mask |
| myvars/file | same wave data_info.yml:128 | Both EACIALI_p1/p2 s13q03c -> ExpenditurePurchased before mapping | reuse; no source/schema change |
| _normalize_dataframe_index | country.py:9124 | Actual additive canonical aggregation; sum(min_count=1), rederived Price; tests/test_assets_additive.py and food duplicate controls | reuse actual pure normalizer in focused tests |
| _mask_value_code / _is_quantity_sentinel | countries/Mali/_/mali.py:506 | Source-specific tree-value 999999 and separate tree-quantity rules | precedent only; NOT reuse code/field/value scope |
| _unit_canonicalizer | wave mapping.py | Existing own-org unit canonicalization | unchanged |

## §3 Definitions and country grounding
Full current Mali CONTENTS personally authenticated: 3551 lines / 247025 bytes, SHA256 e10f15f214218de84c76850ee9c7f4bab17e3be00315a1d960d85ed6df6d26a7; previously read full3481-line prefix and all70 additions, including LOGBOOK/WAITING/TODO. EACI14 identifiers are separate from EACI17 and later EHCVM; 2014 rural agricultural holdings differ from urban households.

Contradiction retained: mapping says visit is a "sample split, not a repeated measure", while the closed1594 source/document diagnosis shows the same3804 units with repeated seven-day asks. GH851 recall/exposure interpretation remains separate. CONTENTS says own-production value is absent from the instrument, while own first-passage manual p73 instructs estimated own/gift value; released files lack s13q04c/05c. No imputation follows from that conflict.

Own2014 documentary derivatives: reports/mali-1594-diagnosis-20261107/{REPORT.md,documents-summary.json,instrument-hashes.json}; manuals128/118 pages and forms57/30 pages already closed. No PDF or original data reopened. Exact raw amount 99999999 differs from a large aggregated cell. Existing aggregation sums available components and leaves all-NA additive groups missing (country.py:9341), preserving its Price formula.

## §4 Invariants
- Original per-stream eligibility is computed BEFORE masking; retain a positive-sentinel-only raw contribution even if Quantity is missing/zero. Do not manufacture a zero to preserve public count.
- Only ExpenditurePurchased originating from own2014 s13q03c is masked by exact equality. No tolerance, locale/nines rule, other fields or waves.
- Input frame, Quantity, unit mapping, t/v/i/j/source keys, raw component order and passage behavior remain unchanged.
- Existing normal all-NA serving policy may change a public row count; report its causal effect rather than force parity. No post-aggregation large-cell deletion.
- Existing sum/Price/recall851, weights and observation unit unchanged. Any missing-component Price concern is a separate finding.
- No source/raw/cache/API/Make/warm/getter or closed capture loads under this code/test preparation grant. Focused frozen operator requires ROOT approval before tests.

## §5 Reuse decision
Reuse the existing wave source-stream mapper, unit canonicalizer and canonical additive kernel. Extend only the purchased stream with exact raw expenditure masking AFTER its original eligibility mask. Tree-specific helpers cannot be called because their field/value policy differs. Add discriminating invented paired-passage tests and exact old-parent counterfactual; no old cohort replay.

## §6 Decisions / remaining gates
Sentinel interpretation is resolved by user ruling (not producer proof). Source correspondence remains a separately authorized finite stage on existing closed1594/1452 captures only; no new raw acquisition. Synthetic tests and focused source-free operator will be frozen for root before execution. Future public count/sums and genuine paired-amount conservation must be measured with explicit scope. GH1594 closure/PR remains root-owned.

### Phase 3 — verification
Pending focused operator approval and measured controls. No source or public-serving acceptance claimed.
