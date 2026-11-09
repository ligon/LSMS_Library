# Prior-Art Ledger — GH #1495 Farm-level definition documentation

**Status:** grounded before the documentation edit; verification pending.
**Source:** `6863a6bf6a7ad1e4af1c9743fa4992f393a1e4ce`.
**Search tier:** git + ripgrep + source/AST inspection (floor); no index refresh.
Inherits [STANDING.md](STANDING.md), especially §§3–4, and `CLAUDE.md`.

## §1 Task, restated

Reconcile the explanatory `plot_id` comments in `lsms_library/data_info.yml`
and the null-key control's docstring in
`tests/test_gh1469_farm_level_occurrence.py` with the existing Farm-level
rows. This is the authorized documentation-only residual, not an allocation,
occurrence-rule, eligibility, weight, schema or default decision. No survey
source, Country/Feature build, acquisition or shared-cache operation is needed.

The supplied issue snapshot predates partial fixes: the pinned schema already
mentions tree harvests, and the test already says Mali's trees *were* null
until GH #1476. Preserve that fact rather than claiming a present-tense stale
docstring was found. Remaining prose still says the record "has no plot" and
"the sale is there", and omits the existing EHCVM harvest-total case.

## §2 Existing machinery

| Source | Existing behavior / receipt | Verification to reuse |
|---|---|---|
| `lsms_library/transformations.py:8114` | `_without_redundant_farm_level` tests keys and the sentinel, not sale/harvest content; keeps a Farm-level row only where no non-Farm-level row repeats its household-wave-crop (country when present) | Existing pure GH #1469 controls; no helper edit |
| `tests/test_gh1469_farm_level_occurrence.py:105` | Synthetic null plot key is not the structural sentinel | Existing assertion and fixture stay exact |
| `tests/test_gh1476_mali_tree_farm_level.py:33` | Static control checks both EAC-I producers write `FARM_LEVEL`, not null | Run this existing source-only control |
| `Mali/2014-15/_/crop_production.py`, `Mali/2017-18/_/crop_production.py` (under `lsms_library/countries/`) | EAC-I tree records use `'plot_id': FARM_LEVEL`; numeric tree columns come from `mali.tree_crop_rows` | Read source, do not execute producers |
| `lsms_library/countries/Mali/_/mali.py:623,1171` | Tree rows carry harvest and sale answers; `derive_hh_crop_harvest_total` fills or creates a Farm-level row for a multi-row crop, even without a sale | Existing implementation plus dated CONTENTS receipts; no new oracle or data replay |

## §3 Definitions & conventions in force

Canonical source: `lsms_library/data_info.yml:7–35` (per STANDING §3).
The structural household-grain sentinel and epistemic missing-plot sentinel
remain distinct. The occurrence and value-sum rules are unchanged.

Full Mali `_/CONTENTS.org` read, including the TODO/WAITING convention and dated
decisions. Concrete grounding (existing documentary receipts, not new counts):

- Lines 2652–2686, GH #1476: the tree roster is household/species grain.
  Lines 2670–2673 say scattered trees can name a parcel but "the parcel is an
  attribute of the line"; this qualifies the schema's "has no plot" wording.
- Lines 2680–2684: "Farm-level now means two things here: a tree row carries a
  harvest; a 2021-22 sale row (above) carries none." The historical observation
  that none of the 627 tree rows repeats a plotted crop is not re-measured here.
- Lines 2808–2820 and 2911–2915, GH #1437: household harvest totals may occupy
  Farm-level rows, including rows created without a sale. This is the missing
  documented example. Lines 3031–3038 keep scattered trees Farm-level by ruling.

No new country idiosyncrasy is discovered, so no CONTENTS addition is warranted.
No other country's actual module/example is used as evidence; existing sibling
citations in the schema remain unchanged and are not independently revalidated.

## §4 Invariants & assumptions

Inherits STANDING §4 and the dispatch's stricter private-runtime/no-source fence.
Only explanatory YAML comments and the named Python docstring may change.
Parsed schema must be entirely equal (no descriptive-value exception planned).
All executable Python AST, non-docstring tokens, test fixtures/assertions,
markers and tolerances must remain exact. No new tests, skips or baseline edits.

`Wave._input_hash` (`country.py:1362` onward) hashes wave/country build inputs;
`Country._table_cache_hash` (`country.py:4516` onward) folds those hashes plus
country inputs/framework closures. The edited global YAML comments are not a
wave/country YAML input, and the test docstring is outside those inputs. Record
the source-based hash implication separately from an unperformed hash census.

## §5 Reuse decision

| Item | Decision | Reason |
|---|---|---|
| Row semantics and occurrence policy | reuse | Existing code and Mali receipts already establish them; do not change helper behavior |
| Explanation | extend | State household-grain allocation and possible harvest/sale contents accurately |
| Verification | reuse | Existing pure GH #1469 and static GH #1476 controls; exact structural proof is evidence only |

## §6 Open questions for the human

None blocks this bounded documentation patch. Scientific/default/eligibility,
allocation, source/API/cache acceptance and full CI remain outside authorization.
Independent review and source/GitHub integration belong to root. The supplied
issue's older description is explicitly reconciled above; no broad reassessment.

### Phase 3 — verification

Pending. Evidence lives in the dispatch's private evidence directory; final
ledger will record exact results and limits in a separate commit.
