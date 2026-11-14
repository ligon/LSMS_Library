# Served-wave corrections (2026-11-12): Mali rice block, Senegal salt and near-zero quantities, Niger computed quantity, Uganda 2009-10 ea

This PR implements five maintainer rulings of 2026-11-12 on values the library already serves. Each one is registered in the country's `_/derivations.yml` and recorded in its `CONTENTS.org`. Principle (@ligon): "Without a very good reason we don't want to serve up someone else's undocumented imputations."

| change | ruling | registry key | effect |
|---|---|---|---|
| Mali 2018-19 rice-ration block (#1707) | Remove, registered | `Mali::food_acquired::rice-ration-block-withheld` (`rows: 0`) | 3,357 records withheld: 3,356 served rows go and 1 summed row is corrected |
| Senegal 2021-22 salt at a fixed amount per person | Remove, registered | `Senegal::food_acquired::salt-per-capita-withheld` (`rows: 0`) | 100 records withheld (100 rows) |
| Niger 2018-19 `s07bq07a` (#1708) | Flag now, diagnose | `Niger::food_acquired::computed-last-purchase-quantity` | 15,014 purchased rows keyed; nothing else changes |
| Uganda 2009-10 `ea` (#1536) | Fill, registered | `Uganda::sample::ea-same-household` | 514 of 569 NA `ea` filled; weight with no `ea` goes from 29.32% to 3.53% |
| Senegal 2018-19 near-zero quantities (#1519) | Exact-value NA rule | `Senegal::food_acquired::near-zero-quantity-withheld` (`rows: 0`) | 3 rows served with Quantity NA; no `food_prices` value near 1e22 remains |

## What changed

### Mali 2018-19 rice-ration block (#1707)
- **Which records.** `s07b_me_mli2018.dta` holds 3,357 records with no `vague`. Every other record in the file has one. These are no longer served.
- **What they look like.** Each record is `Riz local Gambiaka` in kg, one per household, answered `Plus de 30 jours`. Nothing on it is produced, given or bought.
- **The quantity.** `s07bq03a` is a whole multiple of 1.4 kg on every record, and equals 1.4 × the welfare `hhsize` on 3,348 of them.
- **The selector is the missing `vague`.** On rows that do have a `vague`, 1.4 × `hhsize` matches 284 genuine answers, so `hhsize` cannot select the block.
- **The build checks the pattern.** It verifies every no-`vague` record against the pattern and raises `RiceRationBlockError` if one does not fit.
- **Effect on 2018-19.**
  - Rice-kg purchases fall from 5,632 rows / 55,895.1 kg to 2,276 rows / 23,581.7 kg.
  - Expenditure is unchanged.
  - One household's block row had been summed into its own rice purchase. That row now serves only the household's answer, 5.6 kg less.

### Senegal 2021-22 salt
- **Which records.** 100 salt-in-kg records with `s07bq03a` = **0.0199997** kg × `hhsize` (to 1e-12) are no longer served. No other product or unit meets that constant.
- **The constant is not 0.02.** No row equals 0.02 × `hhsize`. "0.02" was this constant rounded to 4 significant digits.
- **Where `hhsize` comes from.** It is read from `ehcvm_welfare_sen2021.dta`, now declared under `household_size:` so the file's sidecar is part of the hash.
- **Rows removed.** 95 purchased and 5 in-kind rows go: 17.04 kg, and 17.78 FCFA on the 2 rows that carried an Expenditure.

### Niger 2018-19 computed `s07bq07a` (#1708)
- **Which records.** 15,264 of 107,263 positive `s07bq07a` values (14.2%) are off the quarter grid, and 14,908 of those carry 13–18 decimals. The 2021-22 wave of the same instrument has 15 such values in 112,923.
- **What is keyed.** Those records become 15,014 purchased rows. 12,227 of them carry an Expenditure, which is 26.0% of the wave's Expenditure. All 15,014 rows now carry the key. Nothing else changes.
- **These are not the rows where the CAPI disables the field.** 15,263 of the 15,264 have `s07bq06` in (1, 2, 3), where the field is enabled. The 21,063 records that are disabled but filled carry no `s07bq08` and feed no served value. They are recorded in CONTENTS and not keyed.
- **Diagnosis.**
  - The unit value is one value per (product, unit, size, region) on 90% of the flagged records.
  - On 12,250 records the quantity was backed out of the reported value at that unit value.
  - On 3,013 records the value and the quantity were both rebuilt: `s07bq08` is non-integer even though the CAPI field is an integer.
  - That reference value is not the file's own median, a community price, or an NSU conversion.
  - The details are in `Niger/_/CONTENTS.org`.

### Uganda 2009-10 `ea` (#1536)
- **What is filled.** A household whose own `comm` gives no `ea` takes the same household's label from another wave. The `comm` waves (2005-06, 2010-11, 2011-12, matched by HHID through `comm_ea`) come first. The 2013-14 `ea`, matched by `HHID_old`, is used only for the 22 households that no `comm` wave records.
- **Labels must agree.** If they do not, the build raises `OriginEaError`. On all 514 households, every label held in any of the seven other waves agrees, so the order changes nothing today.
- **IDs.** Raw ids go through `format_id`; the walked `i` is never used. 2009-10 has no `updated_ids` entry, so its served `i` is the raw HHID.
- **What stays the same.** No `ea` that is already served changes, and `v` is untouched.
- **How the other waves are read.** Two new `dfs:` sub-dfs read the other covers. Their hooks keep only this wave's households, so the outer merge adds no rows.

### Senegal 2018-19 near-zero quantities (#1519)
- **Which records.** Exactly three records match: product 132, with `s07bq03a` = 40 × `s07bq07a`, both between 4e-19 and 2e-20. The same rule catches nothing in the other 12 EHCVM `s07b` files checked.
- **What changes.** Quantity becomes NA. Expenditure becomes NA too, because the registered #876 rule multiplies the served quantity (2,000 and 4,000 FCFA on two rows). Quantity_kg was already NA. The rows themselves stay.
- **Effect on 2018-19 `food_prices`.** The kgvalue maximum falls from 1.79e22 to 479,452 FCFA/kg; the unitvalue maximum falls from 4.79e21 to 70,000.
- **Precedent.** #1594 (Mali 2014-15, an exact value of 99,999,999) is a mapper mask with no registry entry. This rule takes the registered `rows: 0` form instead, as Burkina Faso's withheld GPS does.
- **CONTENTS.** `Senegal/_/CONTENTS.org:836-858` is corrected, including the stale statement that "Site Q does not screen food_prices". Site Q has screened it since #1544.

## Contradictions with the brief
- **Mali.** The block is 3,357 records selected on the missing `vague`, not "1.4 × hhsize AND no vague", which would select 3,348.
- **Mali.** One block row was summed into a real row before this change.
- **Senegal.** The constant is 0.0199997 kg × hhsize, not 0.02.
- **Niger.** The 14.2% are rows where the field is enabled; the disabled-but-filled rows are a separate set.
- **#1519.** The ruling names the quantity only. Expenditure also becomes NA through the #876 rule. Please confirm that this is intended.

## Verification
- **Cold builds**, base `e5fdd2fa8` against candidate, each country in its own private root (burst jobs 41249476 and 41249611). Every other row and wave is frame-equal:
  - Mali, Senegal and Niger: `food_acquired`, `food_expenditures`, `food_prices` and `food_quantities`.
  - Uganda: `sample`, `cluster_features` and `household_characteristics`.
  - Niger: every column except `Derivation`.
- **`derivation_inputs`** returns rows for each new key, and every stamped row joins them: Mali 3,357, Senegal 100 and 3, Niger 15,264, Uganda 1,303 rows for 514 households.
- **Cache-hash footprint**: Mali 23/23 tables and 23/92 waves, Niger 24/24 and 24/96, Senegal 22/22 and 44/44, Uganda 27/27 and 216/216. Nothing outside these four countries moved, and 0 of 41 build fingerprints moved. **Cost:** one re-warm each for Mali, Senegal, Niger and Uganda.
- **Targeted tests** cover all Mali, Niger, Senegal, Uganda and EHCVM tests, plus `test_derivations.py`, `test_build_transform_hash.py` and the drift and schema tests:
  - base: 4802 passed, 69 skipped
  - candidate head `a495b16d1`: **4863 passed, 69 skipped, 0 failed**. This count includes `test_gh1493_near_zero_screen.py` (27 passed at base, where it pinned the 1e22 prices) and the new file.
- **New test file:** `tests/test_gh1707_served_corrections.py`. These existing tests are re-pinned, each to a figure measured on the cold build:
  - `test_gh1459` and `test_gh689` (Uganda `ea` counts);
  - `var/sample` in `uganda_baseline.json`;
  - `test_ehcvm_7day` (Senegal totals and gift rows; the Niger key);
  - `test_ehcvm_unit_weights` (its row oracles skip the records a `rows: 0` entry withholds);
  - `test_gh1493_near_zero_screen` (Senegal no longer serves the residues);
  - `test_gh1500`'s source digests.
- **Final-head check.** All three cold runs agree: a smoke run on the patch, one on `8f0b7d91a`, and one on `a495b16d1`. The hash probe is identical at `8f0b7d91a`, `a495b16d1` and `52386ee64`. Every later commit touches only tests or registry text.
- **Sweep of 85 more country-naming test files**, run under `-k <country>`: base 554 passed, `52386ee64` 552 passed. The 2 failures were `test_sampling`'s Uganda historical view, which did not yet list the new key; it is fixed and re-run at the head (`test_sampling.py` and `test_gh1707` whole: 319 passed at `3ae15b56c`). Six kg-factor and food files, run whole, pass 324/324 at both.
- **Uganda's registered function** reproduces the served `ea` on all 514 rows from the real `derivation_inputs`.
- **Not edited: CLAUDE.md.** Its Site Q lines still name Senegal's near-zero quantities and 1e22 prices, which this PR stops serving.

## Issue drafts

**#1707: close.**
> Done in this PR (ruling @ligon 2026-11-12, "Remove, registered").
> - The 3,357 records of `s07b_me_mli2018.dta` that have no `vague` are no longer served. Each is `Riz local Gambiaka`, in kg, `Plus de 30 jours`, with nothing produced, given or bought, and `s07bq03a` a whole multiple of 1.4 kg (1.4 × hhsize on 3,348 of them). They are registered as `Mali::food_acquired::rice-ration-block-withheld` (`rows: 0`), and `derivation_inputs` returns them.
> - **Rows removed.** 3,356 served rows go. A fourth record had been summed by core into the household's own rice-kg purchase; that row now serves the household's answer alone. Rice-kg purchased kilograms in 2018-19 fall from 55,895.1 to 23,581.7. Expenditure is unchanged.
> - **Everything else is frame-equal**, cold, against development.
> - The details are recorded in `Mali/_/CONTENTS.org`.

**#1708: narrow to the drop decision.**
> The flag is done (ruling "Flag now, diagnose").
> - **Which rows.** 15,264 records whose `s07bq07a` is off the quarter grid (14.2%) produce 15,014 purchased rows. They now carry `Niger::food_acquired::computed-last-purchase-quantity` and are still served (12,227 with an Expenditure, 26.0% of the wave's Expenditure).
> - **Diagnosis**, from the data (no document explains it):
>   - The unit value `s07bq08/s07bq07a` is one value per (product, unit, size, region) on 90% of these records.
>   - On 12,250 records the quantity was backed out of the reported value at that unit value.
>   - On 3,013 records both the value and the quantity were rebuilt.
>   - The reference value is not the file's own median, a community price, or an NSU conversion.
>   - These are not the CAPI-disabled rows: 15,263 of the 15,264 are on enabled rows.
>   - The details are in `Niger/_/CONTENTS.org`.
> - **What this issue now tracks:** the maintainer's decision on whether to drop these rows, NA their Expenditure, or keep serving them flagged.

**#1536: close.**
> Done (ruling "Fill, registered").
> - **What is filled.** 514 of 2009-10's 569 NA `ea` now take the same household's label from another wave, stamped `Uganda::sample::ea-same-household`. The `comm` waves are used first, by HHID; 2013-14 is used by `HHID_old` only for the 22 households no `comm` wave records.
> - **Labels must agree.** If they do not, the build raises. All 514 agree across all seven other waves.
> - **Result.** The 2009-10 weight with no `ea` falls from 29.32% to 3.53%. The 55 rows still NA are split-offs that no other wave records, plus 4 households held only in `AGSEC3B`/`GSEC12`. No `ea` that was already served changes, and `v` is untouched.
> - **Cost.** One Uganda re-warm. The details are in `Uganda/_/CONTENTS.org`.

**#1519: close.**
> Done (ruling "Exact-value NA rule").
> - **Rule.** Records of `s07b_me_sen2018.dta` whose `s07bq03a` and `s07bq07a` are both in (0, 1e-3) are served with Quantity NA, and so Expenditure and Quantity_kg NA. The rows keep their #876 key. It is registered as `Senegal::food_acquired::near-zero-quantity-withheld` (`rows: 0`).
> - **What it catches.** Exactly the 3 product-132 records, and nothing in any other EHCVM `s07b`.
> - **Result.** 2018-19 `food_prices` (kgvalue) peaks at 479,452 FCFA/kg instead of 1.79e22, and no price exceeds 1e10.
> - **Docs.** `Senegal/_/CONTENTS.org:836-858` is corrected, including the stale Site Q sentence.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
