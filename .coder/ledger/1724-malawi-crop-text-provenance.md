# Malawi #1724: distinguish crop-text evidence from a coded label

## 1. Task
Make the shipped cowpea derivation report its actual text operand instead of
guessing it from the final crop label. Do not wire 2024-25 or change the
existing crop text, code, slot, season, sale, or eligibility policy.

## 2. Existing machinery
- Malawi `_/malawi.py`: `derive_cowpea_from_crop_text` already returns the
  exact boolean mask beside the displayed label. `_harvest_block` uses it
  for counts, and `_seed_block` currently discards it.
- `_consolidate_harvest` and `assemble_plot_inputs` already own their
  respective native grain and legacy reduction. `_farm_level_sales` owns
  the unanimous household/native-code label path. `union_derivation_key`
  already composes with the sale ladder.
- Malawi `_/derivations.yml`: two keys and the existing inputs callable.
  The country `_/CONTENTS.org` #1228 entry records slot identity, unanimous
  sale inheritance, and the payload gate.
- Existing tests: `test_gh1228_malawi_cowpea_label.py`,
  `test_gh1457_malawi_farm_level_sales.py`,
  `test_gh1190_selected_farm_carrier.py`, `test_malawi_sale_basis.py`.
  Discovery used git, rg and direct callers.

## 3. Definitions
Use the existing #1228 crop text scope and only its actual boolean operand.
See Malawi's `_/CONTENTS.org`, `STANDING.md`, `CLAUDE.md` and the
`production_obs`, `input_obs` and Derivation schema in
`lsms_library/data_info.yml`.

## 4. Invariants
No new key, crop relabel, aggregation grain, sale attachment, metadata
operand, sample weight or private flag on output. Preserve no-payload seed
deletion and the existing sale-basis union. An unstamped or mixed donor is
not proof of household text provenance. See `STANDING.md` for sanctioned IO.

## 5. Reuse
Reuse the existing relabel mask at both source blocks, the existing groups
for its unanimous reduction, the existing Farm-level label match and the
existing derivation union. Extend only private transport and stamping.

## 6. Open questions
Code 50 and IHS6 table integration, 2019-20 code 49, two-text slots,
local names, and any later sale identity remain outside this change.
