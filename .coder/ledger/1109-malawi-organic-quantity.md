# Prior-Art Ledger - GH #1109 Malawi reported organic inputs

**Search tier:** ripgrep + git floor; amendment base `c08abb3f1`.
Inherits `.coder/ledger/STANDING.md`; source review and design approved 2026-10-21.

## §1 Task, restated

Restore reported organic quantity (`ag_d37a`) and unit (`ag_d37b`) for affirmative `ag_d36` rows.
Keep Applied=True, organic-only keyed sum(min_count=1), optional-field compatibility and every nonorganic behavior, including 437 unadjudicated records.

## §2 Existing machinery

| symbol | path | existing behavior and verification | decision |
|---|---|---|---|
| `_organic_block` | `lsms_library/countries/Malawi/_/malawi.py:3007` | Discards amount/unit, preserves affirmative use; `tests/test_1109_malawi_input_quantity.py` exercises helper/finalizer | extend |
| `_fertilizer_block` | same file `:2967` | Nullable numeric read via pd.to_numeric(errors='coerce').astype('Float64'); regression numeric/missing/zero controls | reuse numeric idiom only |
| `_int_codes` | same file `:2946` | Existing nullable integer normalization for Module D codes | reuse |
| `assemble_plot_inputs` | same file `:3150` | Organic-only full-key min_count overlay; all other reducers unchanged | reuse unchanged |
| four wave callers | `Malawi/{2010-11,2013-14,2016-17,2019-20}/_/plot_inputs.py` | All delegate organic block; 2016 Cross_Sectional `_D_USECOLS` omits both fields | extend whitelist only |
| physical labels | `Malawi/_/categorical_mapping.org:446,566,693,704` | Existing Kilogramme, Bucket, Ox-Cart; source Wheelbarrow and Other (Specify) remain recorded labels | reuse spellings; own organic codebook |
| `U_UNKNOWN` | `lsms_library/transformations.py`; `lsms_library/data_info.yml:16-39` | Canonical missing-unit sentinel under #1041; not a conversion unit | reuse |
| `Country._finalize_result` | `lsms_library/country.py:3961-3981` | Applied preserves missing-amount affirmative rows through all-null payload filter | preserve unchanged |
| `nitrogen_kg` | `lsms_library/transformations.py`; `tests/test_867_nitrogen_content.py` | Organic nominal share explicitly refused | reuse unchanged |

## §3 Definitions and conventions in force

- **CONTRADICTION, 2026-10-21:** `Malawi/_/CONTENTS.org:3027-3028` says "Module D records no organic quantity".
  The initial ledger inherited that false claim.
  All seven held Module D headers label ag_d37a quantity applied and ag_d37b unit.
  Six sources actually loaded contain 11,075 affirmative rows, 11,068 positive amounts, seven missing amounts and 11,067 observed units.
  The fields were unmaterialized, not unasked.
- The organic unit dictionary agrees across all seven sources: 2 Kilogram, 10 Bucket, 11 Wheelbarrow, 12 Ox cart, 13 Other (Specify).
  Reuse canonical spellings; do not borrow inorganic/food codebooks.
  Five recorded code-1 rows and one code-3 row remain normalized raw codes; eight truly missing units become U_UNKNOWN.
- Evidence: campaign control `review-1109/source-semantics.json`, `source-amounts.json`, `source-organic-unit-counts.json`; safe aggregate evidence accompanies final verification.
- Schema/IO follows STANDING sections 3-4 and `lsms_library/data_info.yml`.
  Organic N refusal remains #867; disputed inorganic applied-unit interpretation remains #1111.

## §4 Invariants and assumptions

- Retain raw ag_d36==1 gate and nullable Applied=True; missing/negative flags produce no row.
- Numeric amounts are neither imputed nor rescaled; observed zero stays zero and missing stays NA.
- Decode only the organic field's five evidenced codes; unlisted recorded identity stays raw, free-text Other stays a separate inventory without interpretation.
- Preserve whole row count modulo organic unit rekeying; preserve exact nonorganic index, fields, reducers and order, including the 437 records.
- Keep native (t,i,plot_id,input,crop,u), dropna=False and organic-only keyed overlay.
  Optional absent amount/unit columns remain supported.
- Expect exact nitrogen output; explain any measured difference before acceptance.
- Editing malawi.py invalidates every Malawi source-table hash once: one private cold plot_inputs build plus verified Uganda comparator.
  Existing L1 only, physically private writable L2, exact checkout/config/image Python, offline checks, single-thread builds.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| organic amount | extend source projection | read available ag_d37a with existing nullable numeric idiom |
| organic unit | new five-entry dictionary | evidenced organic codebook differs from inorganic/food |
| missing unit | reuse U_UNKNOWN | #1041 missing-identity contract; unlisted raw codes are observed identity |
| organic use/missingness | reuse c08 carrier/overlay | preserve affirmative rows and missing-vs-zero semantics |
| nitrogen | reuse unchanged | no organic share, container weight or scientific rescaling authorized |

## §6 Open questions for the human

Root approved this concrete source-backed extension before implementation.
No decision blocks restoring these reported fields.
Other free-text units, #1111/#1106, 437 nonorganic residual and remainder of #1118 stay separate.
Update #1118 organic amount/unit denominators in report only; do not claim the whole issue fixed.

## Phase 3 - verification

Pending c08 negative controls, source-backed helper/finalizer tests, actual per-row cold source accounting, exact nonorganic/nitrogen comparison and pooled Feature checks.
Earlier c08 checks established mechanical preservation under a false source premise; this amendment supersedes that interpretation.
