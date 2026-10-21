# Prior-Art Ledger - GH #1109 Malawi plot-input missingness

**Search tier used:** ripgrep + git floor. Base `f8d236694`.
Inherits `.coder/ledger/STANDING.md`; source/test evidence is re-grepped below.

## §1 Task, restated

Preserve missing organic Quantity and the raw Module D organic-use answer through Malawi plot_inputs assembly and Country finalization.
Carry ag_d36 == 1 as nullable Applied=True, following Ethiopia.
Overlay only organic groups with sum(min_count=1), aligned on the complete existing key; retain observed numeric/zero organic inputs and the existing nonorganic sums.
No unit interpretation, fertilizer share, identifier, core filter, or other existing reducer changes.

## §2 Existing machinery

| symbol | path:line | existing behavior and verification | decision |
|---|---|---|---|
| `_organic_block` | `lsms_library/countries/Malawi/_/malawi.py:3007` | Emits a use row with nullable Quantity/u NA; current-helper probe reproduces it | reuse |
| `assemble_plot_inputs` | same file `:3148`, aggregation `:3223` | Plain Quantity sum fabricates 0 for all-missing groups; actual helper regression needed | extend |
| `_seed_purchase_block` | same file `:3142` | Existing `lambda s: s.sum(min_count=1)` preserves missing purchase quantity | reuse pattern |
| four wave `plot_inputs.py` callers | `Malawi/{2010-11,2013-14,2016-17,2019-20}/_/` | All delegate organic and other input assembly to this helper | reuse |
| `Applied` use carrier | `Ethiopia/_/ethiopia.py:2117-2124`; `Ethiopia/_/data_scheme.yml:234` | Preserves raw organic-use flag with missing Quantity through Country finalization | reuse candidate |
| `Country._finalize_result` | `lsms_library/country.py:3961-3981` | Drops rows with every payload missing; exposes row-loss boundary of a reducer-only patch | preserve unchanged |
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
- Every existing reducer, flag, nonorganic value, index value and ordering remains unchanged.
- Applied is an additional optional nullable boolean: True only for the raw affirmative organic-use answer and NA elsewhere.
- Recompute organic Quantity alone with sum(min_count=1), aligned on the full existing key; do not infer application for 437 nonorganic records exposed by the rejected global reducer change.
- `malawi.py` is a whole-country build input. Editing it invalidates every
  Malawi source-table cache once; a private cold plot_inputs build verifies
  this change without promoting or deleting shared caches.
- Use existing L1 source blobs only; missing sources block the corpus check
  rather than authorizing acquisition. Private L2 is physically separate.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| organic Quantity groups | extend existing assembly | keyed min_count=1 overlay preserves missing/observed quantities; nonorganic sum unchanged |
| raw organic use | extend with existing Applied carrier | preserve affirmative ag_d36 answer through the existing all-null row filter |
| observed zero and numeric sum | reuse | keep additive sum semantics and nullable float dtype |
| organic use row | reuse | existing source block already models reported presence correctly |
| nitrogen output | reuse unchanged | organic refusal policy excludes missing/zero alike |

## §6 Open questions for the human

Root approved the narrow extension after the first cold comparison: raw organic Applied carrier plus organic-only keyed missingness correction.
No open question blocks that correction.
The 437 nonorganic records (390 Seed, 24 NPK, 11 Urea, 6 DAP, 4 Herbicide, 1 Fungicide, 1 Insecticide) keep their existing behavior and require a separately drafted source-presence/missingness investigation.
#1111/#1106/#1118 remain outside scope.

## Phase 3 - verification

Actual-helper regression: candidate 7 pass; untouched base 3 missingness failures and 4 observed-value passes.
Neighbor nitrogen pure tests plus new regressions: 31 pass.
Plot declaration/harmonization/permutation checks: 21 pass.
Private offline cold source builds: baseline 180,184 rows / 11,075 organic zeros; reducer-only candidate 168,672 rows / zero organic rows.
This fails section 4 row preservation and cannot be accepted.
Nitrogen sum is unchanged, but 14 zero-only plots and the organic refusal warning disappear.
The accepted organic-only overlay plus raw Applied carrier passes final private cold and warm verification: all 180,184 rows and original index/order/dtypes retained; 11,075 organic zeros become NA with Applied true; other measures and nitrogen output are exact.
Final targeted tests: 36 passed; existing plot assembly checks: 21 passed; base negative control: six failures and five compatibility passes.
Pooled Malawi/Uganda retains 264,400 rows; scanner has the same 15 warning checks, no failures/errors, with Applied added only to the expected constant-column detail.
Measured hash impact is 22 Malawi changes and zero of 24 Uganda changes.
OK against sections 2-4; no reinvention or definition contradiction remains in the accepted patch.
Independent review is pending.
