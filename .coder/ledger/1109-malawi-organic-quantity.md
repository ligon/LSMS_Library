# Prior-Art Ledger - GH #1109 Malawi reported organic inputs

**Search tier:** ripgrep + git floor; amendment base `c08abb3f1`.
Inherits `.coder/ledger/STANDING.md`; source review and design approved 2026-10-21.

## §1 Task, restated

Restore reported organic quantity (`ag_d37a`) and unit (`ag_d37b`) for affirmative `ag_d36` rows.
Keep Applied=True, organic-only keyed sum(min_count=1), optional-field compatibility and every nonorganic behavior, including 437 unadjudicated records.

## §2 Existing machinery

| symbol | path | existing behavior and verification | decision |
|---|---|---|---|
| `_organic_block` | `lsms_library/countries/Malawi/_/malawi.py:3019` | Reads reported amount/own unit dictionary and preserves affirmative use; `tests/test_1109_malawi_input_quantity.py` exercises helper/finalizer | extend |
| `_fertilizer_block` | same file `:2977` | Nullable numeric read via pd.to_numeric(errors='coerce').astype('Float64'); regression numeric/missing/zero controls | reuse numeric idiom only |
| `_int_codes` | same file `:2957` | Existing nullable integer normalization for Module D codes | reuse |
| `assemble_plot_inputs` | same file `:3178` | Organic-only full-key min_count overlay; all other reducers unchanged | reuse unchanged |
| four wave callers | `Malawi/{2010-11,2013-14,2016-17,2019-20}/_/plot_inputs.py` | All delegate organic block; 2016 Cross_Sectional `_D_USECOLS` now includes both fields | extend whitelist only |
| physical labels | `Malawi/_/categorical_mapping.org:446,566,693,704` | Existing Kilogramme, Bucket, Ox-Cart; source Wheelbarrow and Other (Specify) remain recorded labels | reuse spellings; own organic codebook |
| `U_UNKNOWN` | `lsms_library/transformations.py`; `lsms_library/data_info.yml:16-39` | Canonical missing-unit sentinel under #1041; not a conversion unit | reuse |
| `Country._finalize_result` | `lsms_library/country.py:3961-3981` | Applied preserves missing-amount affirmative rows through all-null payload filter | preserve unchanged |
| `nitrogen_kg` | `lsms_library/transformations.py`; `tests/test_867_nitrogen_content.py` | Organic nominal share explicitly refused | reuse unchanged |

## §3 Definitions and conventions in force

- **CONTRADICTION, 2026-10-21:** `Malawi/_/CONTENTS.org:3027-3028` at c08 said "Module D records no organic quantity".
  The initial ledger inherited that false claim.
  All seven held Module D headers label ag_d37a quantity applied and ag_d37b unit.
  Six sources actually loaded contain 11,075 affirmative rows, 11,068 positive amounts, seven missing amounts and 11,067 observed units.
  The fields were unmaterialized, not unasked.
- The organic unit dictionary agrees across all seven sources: 2 Kilogram, 10 Bucket, 11 Wheelbarrow, 12 Ox cart, 13 Other (Specify).
  Reuse canonical spellings; do not borrow inorganic/food codebooks.
  Five recorded code-1 rows and one code-3 row retain their raw identities as source-qualified `Organic unit code 1` / `Organic unit code 3`; eight truly missing units become U_UNKNOWN.
  The first literal-numeric fallback failed the public boundary: Country applies Malawi food u aliases to raw 1/3 and silently changes them to Kilogramme/90 kg Bag.
  Root approved the source-qualified fallback on 2026-10-21 after that measured collision; no framework or food mapping changes are authorized.
- Evidence: campaign control `review-1109/source-semantics.json`, `source-amounts.json`, `source-organic-unit-counts.json`; safe aggregate evidence accompanies final verification.
- Schema/IO follows STANDING sections 3-4 and `lsms_library/data_info.yml`.
  Organic N refusal remains #867; disputed inorganic applied-unit interpretation remains #1111.

## §4 Invariants and assumptions

- Retain raw ag_d36==1 gate and nullable Applied=True; missing/negative flags produce no row.
- Numeric amounts are neither imputed nor rescaled; observed zero stays zero and missing stays NA.
- Decode only the organic field's five evidenced codes; unlisted recorded identity stays source-qualified and unlabelled, free-text Other stays a separate inventory without interpretation.
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

OK against sections 2-4 at implementation commit `d4fc6434c2eaa1b107a5c065606406505a053187`.
The source-backed correction supersedes the inherited no-quantity interpretation, and the source-qualified fallback supersedes the rejected bare-numeric public collision.

- Before production edits, c08 runtime: 15 failures / 12 passes across the then-27-case source regression.
  The actual Country fallback regression additionally fails bare 1/3 alias reinterpretation before passing the final source-qualified representation.
- Final targeted run: 72 passed, zero skips; Ethiopia warm case excluded from this private Malawi seed.
  Reuses existing helper/finalizer, nitrogen and canonical plot tests; final new regression has 28 cases.
- Independent metadata/row accounting: all seven source dictionaries checked; six loaded sources, 11,075 organic records; every quantity/unit/Applied answer exact at wave and finalized public keys.
  Final amounts: 11,068 positive, seven missing, zero observed source zeros; tests retain synthetic observed zero.
  Final units: 5,518 Kilogramme, 2,155 Ox-Cart, 1,451 Bucket, 1,204 Other (Specify), 733 Wheelbarrow, eight Unknown, five Organic unit code 1 and one Organic unit code 3.
  The six source-qualified unlabelled codes have no inferred kg factor.
- Private cold output retains 180,184 rows; all 169,109 nonorganic records/index/order/values/dtypes exact, including the 437 residual records.
  Other organic fields/index parts are exact modulo reported Quantity/unit restoration.
  Nitrogen frame and input-match/refusal tally are exact: 33,466 plots, 840,235.7795800001 kg N, all 11,075 organic inputs still refused a nominal share.
- Warm and pooled Malawi/Uganda checks retain 264,400 rows; Uganda exact.
  Scanner identities/statuses exact: 15 warnings, zero failures/errors; only two null-unit warning details move from 28,724 to 17,649.
- All 22 measured Malawi source-table hashes change, zero of 24 Uganda hashes change: one Malawi rewarm.
  Existing L1 physically copied, fresh private L2, no acquisition/shared cache mutation or promotion.

Safe aggregate evidence and runnable commands: `slurm_logs/isa_campaign_2026-10-21/1109/REPORT.md`, `REPRODUCTION.md`, source-amended-*.json.
The 437 nonorganic diagnosis and remaining 17,649 native null units remain separately drafted; original #1109 organic scope is implemented pending independent review.
No reinvention or remaining definition contradiction was found against this corrected ledger.
