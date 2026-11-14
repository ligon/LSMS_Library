
**Guinea-Bissau: 2018-19 label bundle and purchase fix; 2021-22 sample, roster (leavers flagged), cluster_features, food_acquired (as released, rounding registered), shocks and assets**

## Summary

**2018-19 (one Guinea-Bissau re-warm):**
- **#1107:** `harmonize_input` (Urea / Phosphate / NPK; DAP stays raw) and a `u_kg` table. `nitrogen_kg` goes from 0 to 188 households, 1,012.5 kg.
- **#1119:** `harmonize_seed_crop`.
- **#1539:** Djagatu → Jaxatu and Beringela → Aubergine. `crop_production` goes from 12,173 to 12,188 rows, and its `GrainCollapseWarning` is gone.
- **#1226:** product 94 is bridged to 95.
- **#1118:** a missing input unit is now `Unknown` (0 rows).
- **Gift/purchase fix (ruling "Fix all three now"):** `Purchased` / `Quantity_purchased` now read the purchase, `s16bq08` / `s16bq09a`. `Purchased` True goes from 423 to 1,231. The old 423 are now `Received_as_gift`, beside `Quantity_received_as_gift` and the two unit columns. 45 rows are both a gift and a purchase.

**2021-22 (#1697), EHCVM-2 WB 6274, on the Togo/Benin/CIV template:**
- `E2_` ids; no panel link.
- `weight` = `panel_weight` = `hhweight`. This is the cross-sectional household weight, **not a longitudinal one** (precedents: Mali and Benin 2021-22).
- Strata are region × milieu, as in #1575. The file's own `strate` agrees one-to-one.
- **Roster:** all 41,501 people are served. The 341 who have left carry the new optional `Departed` = True (ruling "Serve leavers"). **`Departed` is a proposal**, because no canonical flag existed. Benin 2021-22 still drops its leavers (#1565).
- **Food:** `food_acquired` is **served as released** (ruling "Serve, registered key").
  - Every positive `s07bq03a` / `s07bq07a` is an even integer.
  - The best fit is 2 × round-half-up(x/2). It holds across the distribution, not row by row, it cannot be inverted, and I found it documented nowhere.
  - Every row carries `Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even`. Its inputs return the released answers.
  - `Expenditure` is derived from the rounded quantities and inherits the rounding.
  - The 3,945 consumed rows rounded to 0 are a registered deletion.
- **Shocks** follow #1556.
- **Assets:** the lawnmower takes the global `Appliances`.

## Verification

- **Cold, private roots, base `f138a3d61`.** On the 2018-19 slice every table is frame-equal to base, except:
  - the Phase-A relabels;
  - the `plot_inputs` purchase and gift columns;
  - the kilogram pool: 39,916 `food_quantities` rows and 22,573 `food_prices` rows change value.
- **Hash census:** Guinea-Bissau only, from my run at `69f2d2fb3` and the red team's at `90bdd6742`. 22 of 555 tables and 44 of 2,321 waves move; `build_transforms_fingerprint` 0 of 42; `framework_imports_fingerprint` 0 of 38. The head adds only `CONTENTS.org` edits.
- **Tests:** the 36 targeted files give 3,325 passed and 0 failed. `tests/ -m "not requires_s3 and not slow"` gives 15,520 passed and 0 failed.
- **Red team delta review:** APPROVE.

## Must-state

**Food, 2021-22**
- 2021-22 food is served as released under a rounding key:
  - 3,945 zeroed rows are deleted (registered). They carry 2,739 positive purchase values, 3,575,875 FCFA, in 2,265 households. Togo's served-NA treatment is the closer precedent.
  - 46 served quantities are odd.
- **Why purchased `Expenditure` is NA on 14,447 rows.** Only the third cause is specific to this release:
  - `s07bq07a` not asked (the instrument skips it when the last purchase was over 30 days ago or never): 3,673 rows;
  - unit or size mismatch between the 7-day and last-purchase lots: 10,325 rows;
  - `s07bq07a` rounded to 0: 449 rows. This occurs in no sibling and not in 2018-19.
- **The kilogram pool moves 2018-19** on unchanged rows: +4.0% kg (226,907 → 236,031), and the `food_prices` sum falls 10.4% (64.91M → 58.14M). The red team broke this down:
  - 2,638 2018-19 rows move from the `unit` rung to an item rung estimated from 2021-22 alone;
  - 7,618 item-rung rows now take a median across two waves;
  - four smoked-catfish cells (`Sillure fumé`, per-piece factor 0.125 → 1.0 kg) make up 61% of the price fall.
- One new `StatedVsInferredKgWarning`: 2021-22 `Saco (25 Kg)`, 2 rows, served at the stated 25 kg.
- No own-country NSU weights (#1598): the `ehcvm.py` edit would re-warm Mali and Niger.
- `Inteiro` is missing from the size vocabulary and is handled by a test exception; a residual is drafted.

**Roster and derived tables**
- `household_characteristics` and the derived food tables are now served automatically for 2021-22.
- **9 leavers in 2 households enter `household_characteristics`**, adding 9 persons (Sex × age counts 40,322 → 40,331). Of the 12 leavers with a `MonthsSpent` of 12, the other 3 carry no sex and no age, so they fall in no bracket.
- 227 of the 341 leavers are in households flagged non-panel.
- `MonthsSpent` is now Int64 in two-wave reads.

**Other tables**
- **Shocks:** 4,417 of 10,842 are served. Code 25 gets a constructed rank on 34 rows, and `Aucune stratégie` is served as HowCoped0 on 961 rows.
- **Assets:** 14 owned lines are not served.
- **Inputs:** the generic NPK share is applied; `Tonelada` is undeclared.
- **Strata:** the registry relies on a spelling fold.
- **GPS:** the 2021-22 cover point is not 2018-19's fix and is served as recorded (#791).
- **Shared file:** `kinship.yml` gains `Sobrinho(A)`, which moves no hash.

**At integration**
- `PARTIALLY_WIRED_SAMPLE`.
- The coverage regrade.
- A recall CSV row for 2021-22 (basis: the variable label).

## For review: `harmonize_food` choices that change what a cross-wave `j` means

- `Chá` → `Thé` (916 rows), while `Chá tipo 2` (788) stays raw. 2018-19's `Thé` had 1,244 rows, so 2021-22's may be a narrower good.
- `Chabéu` → `Huile de palme rouge` (1,573), while the literal `Óleo de palma vermelho (Citi)` (3,002) stays raw.
- 2021-22's `Outros legumes frescos, n.e.s (Djagatu, nabo, beterraba)` absorbs Djagatu, so `Jaxatu` exists in 2018-19 food only.

## Tests re-pinned or added

- The Guinea-Bissau #1575 and gh812 strata scopes.
- The 2018-19 `cluster_features` scope.
- The `ehcvm_condition_originals.json` hash (a comment-only change).
- `test_ehcvm_7day`: the Guinea-Bissau 2021-22 cells, `Inteiro`, and `Tabuleiro (8)`.
- `test_food_kg_size_pool`: the `Inteiro` exception.
- New files: `test_gnb_2021_wave.py` and `test_gnb_2018_input_labels.py`.

Addresses #1697, #1107, #1539, #1119, #1226, #1118.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
