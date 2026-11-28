# Prior-art ledger — #1721 Nigeria W3 sale unit label

**Base:** 47df62d49e614c9a56e319def7324145d138c3d9. **Search:** git/ripgrep,
current issue and ruling history, approved projection. Inherits [STANDING](STANDING.md).

## §1 Task

Make W3 A3.ii sell-unit codes 190–192 say `Jerry Can` rather than `l` by
using the same literal raw-code map as the W3 harvest. Leave the overlapping
shared decoder and all rules for placement, grain and factors unchanged.

## §2 Existing machinery

| Anchor | Contract |
|---|---|
| `Nigeria/_/nigeria.py:841,949` | `_canon_unit` handles litres before cans, is shared with harvest, inputs and community prices. `_canon_units` keeps the existing missing-label and warning behavior. |
| `nigeria.py:1193,4089`, #1518 | Existing raw-code W3 harvest override maps exactly 190–192 to Jerry Can. Harvest-side kilograms and the food/harvest litre ruling are not sale conversion authority. |
| `nigeria.py:1390–1485`, country CONTENTS #843 | W3 records its own sale unit (`sa3iiq5b`) at household-crop grain. W4/W5 use the unit of the household total harvest. Positive sold quantity or value determines sale records. |
| `derive_hh_crop_sale_attachment`, `_inputs_hh_crop_sale` | Group on raw unit/size/state keys and place once per household/crop. Mixed stated units keep value but withhold summed quantity/unit. Derivation inputs return original survey coordinates and columns. Rules pinned in `test_gh843_nigeria_household_crop_sales.py`. |
| `transformations._sold_kg_factors`, #824 ledger | Prefer the recorded sale unit and a reported sale factor. Only missing `Unit_sold` permits harvest-label fallback. There is no W3 reported sale factor to add. |
| Accepted #1721 round 1 §2, exact held projection | Four independently cloned/verified metadata artifacts, only W3 sale numeric four-column aggregate. Code 193 exists on both W3 cards but is unused in W3 sale. The 15 can and five plain-litre raw records are not a joined verification of the historical 20 served rows. |

## §3 Definitions and rulings

Reuse the STANDING definitions and schema. The #843 single-harvest/Farm-level
rule and #1551 preservation ruling stay in force. The #1518 harvest table
and recorded density convention do not authorize sold kilograms or a new
`KgFactor_sold`. The #1721 branch-pending sentence is historical: #1752
(116345613) already shipped the harvest-side patch.

## §4 Invariants

The W3 raw-code map is selected at its own sale-reader call. All numeric
values, indexes, grouping raw keys, mixed-unit NA, provenance and original
derivation input fields stay exact. W4/W5 labels, plain W3 litres and
centilitres, and dormant code 193 keep existing behavior. Do not change
the generic decoder, harvest mapping, other features, or conversion ladder.
No crop or household identifiers were admitted in the raw evidence.

## §5 Reuse

Pass the existing W3 harvest map as an optional override to the real sale
decoder and keep its output normally canonicalized. Verify with synthetic
frames through actual sale reader, sale placement and derivation input
code. Use metadata-only baseline/candidate hash comparison for the Nigeria
cache footprint. Append dated metadata evidence to country CONTENTS without
rewriting the historical report.

## §6 Open scope

The served-row delta and complete valuation rerun are coordinator work
under separate data admission. Correcting the sold label withdraws a false
metric price input, not an inferred kilogram. Any sale factor or future
193 policy needs independent authority.

## Verification against the ledger

- OK (§§2–4): static AST comparison finds just the two sale-reader functions
  changed. The entire remaining module, including the generic canonicalizer,
  the existing harvest map, raw placement and derivation helpers is identical.
  The country Org file retains all old text with a dated evidence insertion.
- OK (§4): source-free base has the two expected new label failures, with
  45 passes, 2 skips and 46 data-gated deselections. Final selected cohort
  has 65 passes, 4 skips and 46 data-gated deselections. It covers W3–W5
  sale-reader controls, W1/W2 tests, synthetic full-wave equality outside
  `Unit_sold`, mixed raw can units withheld, exact original arguments to
  three provenance readers, and sale factors becoming unavailable for
  the previously mislabeled cans without harvest fallback.
- No new survey read or auth took place during implementation. The preceding
  independently verified four-copy projection is raw metadata/count evidence,
  not a joined served-artifact or valuation rerun.
- OK (§§2–5): metadata-only census on the exact code delta from the base
  changed all 24 Nigeria L2-country and 240 Nigeria L2-wave hashes and none
  of the other 532 country or 2,083 wave hashes. Both the 42 build-transform
  and 38 framework-import entries were identical, with zero errors.
  No rewarm or full valuation test ran here.
