# Prior-Art Ledger - GH #1109 Malawi plot-input missingness

**Search tier used:** ripgrep + git floor. Base `f8d236694`.
Inherits `.coder/ledger/STANDING.md`; source/test evidence is re-grepped below.

## §1 Task, restated

Preserve missing reported Quantity when Malawi plot_inputs consolidates input
lines. Module D records organic fertilizer use but no amount; its missing
amount must survive assembly. Retain observed zero and existing numeric sums.
No unit interpretation, fertilizer share, identifier, or other reducer changes.

## §2 Existing machinery

| symbol | path:line | existing behavior and verification | decision |
|---|---|---|---|
| `_organic_block` | `lsms_library/countries/Malawi/_/malawi.py:3007` | Emits a use row with nullable Quantity/u NA; current-helper probe reproduces it | reuse |
| `assemble_plot_inputs` | same file `:3148`, aggregation `:3223` | Plain Quantity sum fabricates 0 for all-missing groups; actual helper regression needed | extend |
| `_seed_purchase_block` | same file `:3142` | Existing `lambda s: s.sum(min_count=1)` preserves missing purchase quantity | reuse pattern |
| four wave `plot_inputs.py` callers | `Malawi/{2010-11,2013-14,2016-17,2019-20}/_/` | All delegate organic and other input assembly to this helper | reuse |
| `nitrogen_kg` | `lsms_library/transformations.py` | Organic input has no assigned share; `tests/test_867_nitrogen_content.py` covers explicit refusal | reuse unchanged |
| isolated module import | `tests/test_malawi_cassava_recode.py` | Loads country helper by file path, not a global `malawi` module | reuse test pattern |

## §3 Definitions and conventions in force

- `Malawi/_/CONTENTS.org:3018-3030`: Module D records no organic quantity;
  its served zero is explicitly a fabrication. Organic inputs receive no
  nominal nutrient share under the #867 ruling.
- Canonical columns, index and sanctioned IO are inherited from
  `STANDING.md` §§3-4 and `lsms_library/data_info.yml`.
- `Malawi/_/CONTENTS.org:3039-3063` leaves applied fertilizer codebook
  interpretation unresolved; #1111 is outside this change.

## §4 Invariants and assumptions

- Consolidation keeps native `(t,i,plot_id,input,crop,u)` grain and
  `dropna=False`; #1118/#1119's missing index-label issues remain separate.
- Every reducer other than Quantity, every flag, all observed numeric sums,
  index values and ordering remain unchanged.
- `malawi.py` is a whole-country build input. Editing it invalidates every
  Malawi source-table cache once; a private cold plot_inputs build verifies
  this change without promoting or deleting shared caches.
- Use existing L1 source blobs only; missing sources block the corpus check
  rather than authorizing acquisition. Private L2 is physically separate.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| all-missing Quantity group | extend existing reducer | min_count=1 exists in neighboring machinery; no new helper |
| observed zero and numeric sum | reuse | keep additive sum semantics and nullable float dtype |
| organic use row | reuse | existing source block already models reported presence correctly |
| nitrogen output | reuse unchanged | organic refusal policy excludes missing/zero alike |

## §6 Open questions for the human

None for this bounded missingness correction. #1111/#1106/#1118 and other
Malawi science/unit defects remain outside scope. Root authorized implementation
after this ledger is surfaced.

## Phase 3 - verification

Pending actual-helper regression, targeted tests, private cold before/after,
Feature plot_inputs comparison and independent review.
