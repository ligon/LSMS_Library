
**Guinea-Bissau: 2018-19 label bundle and purchase fix; 2021-22 sample, roster (leavers flagged), cluster_features, food_acquired (as released, rounding registered), shocks and assets**

## Summary

**2018-19 (one Guinea-Bissau re-warm):**
- **#1107:** `harmonize_input` (Urea / Phosphate / NPK; DAP stays raw) and a `u_kg` table. `nitrogen_kg` goes from 0 to 188 households, 1,012.5 kg.
- **#1119:** `harmonize_seed_crop`.
- **#1539:** Djagatu → Jaxatu and Beringela → Aubergine. `crop_production` goes from 12,173 to 12,188 rows, and its `GrainCollapseWarning` is gone.
- **#1226:** product 94 is bridged to 95.
- **#1118:** a missing input unit is now `Unknown` (0 rows).
- **Gift/purchase fix (ruling "Fix all three now"):** `Purchased` / `Quantity_purchased` now read the purchase, `s16bq08` / `s16bq09a`. `Purchased` True goes from 423 to 1,231. The old 423 are now `Received_as_gift`, beside `Quantity_received_as_gift` and two unit columns. 45 rows are both a gift and a purchase.

**2021-22 (#1697), EHCVM-2 WB 6274, on the Togo/Benin/CIV template:**
- `E2_` ids; no panel link.
- `weight` = `panel_weight` = `hhweight`. This is the cross-sectional household weight, **not a longitudinal one** (precedents: Mali and Benin 2021-22).
- Strata are region × milieu, as in #1575; the file's own `strate` agrees one-to-one.
- **Roster:** all 41,501 people are served.
  - The 341 who have left carry the new optional `Departed` = True (ruling "Serve leavers"). **`Departed` is a proposal**, because no canonical flag existed. Benin 2021-22 still drops its leavers (#1565).
  - Their MonthsSpent is served NA (ruling "Exclude from HH size"), so `household_characteristics` counts the survey's household. The 12 recorded answers are withheld under `Guinea-Bissau::household_roster::departed-monthsspent-withheld`, and the key's inputs return them.
- **Food:** `food_acquired` is **served as released** (ruling "Serve, registered key").
  - Every positive `s07bq03a` / `s07bq07a` is an even integer.
  - The best fit is 2 × round-half-up(x/2). It holds across the distribution, not row by row, it cannot be inverted, and I found it documented nowhere.
  - Every row with a quantity carries `Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even`, whose inputs return the released answers.
  - `Expenditure` is derived from the rounded quantities and inherits the rounding.
  - The 3,945 consumed rows rounded to 0 are each served one row with Quantity and Expenditure NA, under `…::s07bq03a-rounded-to-zero-withheld` (ruling "Serve NA, as Togo"). No reported item disappears. 3,260 are purchased rows. The 685 whose household says it never bought the item (`s07bq06` = `Nunca`) are served at `s='other'`, without the 7-day key (ruling "GNB 'Nunca' rows").
- Shocks follow #1556; assets map the lawnmower to the global `Appliances`.

## Verification

- **Cold, private roots, base `f138a3d61`.** On the 2018-19 slice every table is frame-equal to base, except:
  - the Phase-A relabels;
  - the `plot_inputs` purchase and gift columns;
  - the kilogram pool: 39,916 `food_quantities` rows and 22,573 `food_prices` rows change value.
- **Against the reviewed head `a51a616e2`:** +3,260 purchased and +685 `other` NA rows, the 12 withheld MonthsSpent values and the roster's `Derivation` column. Everything else is frame-equal, including all derived food tables. `household_characteristics` equals the survey household (Sex × age 40,322).
- **Hash census:** Guinea-Bissau only. 22 of 555 tables and 44 of 2,321 waves move; `build_transforms_fingerprint` 0 of 42; `framework_imports_fingerprint` 0 of 38.
- **Tests:** the 36 targeted files give 3,327 passed and 0 failed. `tests/ -m "not requires_s3 and not slow"` gives 15,521 passed and 0 failed.

## Must-state

**Food, 2021-22**
- Served as released under a rounding key; 46 served quantities are odd.
- **The 3,945 zeroed rows are served with Quantity and Expenditure NA under a registered withheld key:**
  - 3,260 as purchased, by Togo's test;
  - 685 at `s='other'`: their households say they never bought the item, and whether the food was own production or a gift cannot be told, both being rounded to 0.
- **Why purchased `Expenditure` is NA on 17,707 of 98,354 rows (four causes):**
  - the 3,260 withheld purchased rows;
  - `s07bq07a` not asked (the instrument skips it when the last purchase was over 30 days ago or never): 3,673;
  - unit or size mismatch between the 7-day and last-purchase lots: 10,325;
  - `s07bq07a` rounded to 0: 449. This occurs in no sibling and not in 2018-19.
- **The kilogram pool moves 2018-19 (accepted, disclosed)** on unchanged rows: +4.0% kg (226,907 → 236,031), and the `food_prices` sum falls 10.4% (64.91M → 58.14M). The red team broke this down:
  - 2,638 2018-19 rows move from the `unit` rung to an item rung estimated from 2021-22 alone;
  - 7,618 item-rung rows now take a median across two waves;
  - four smoked-catfish cells (`Sillure fumé`, per-piece factor 0.125 → 1.0 kg) make up 61% of the price fall.

  A residual to scope the rounded-key rows out of the kilogram evidence is drafted.
- `kg_factor_sources` counts the 3,945 withheld rows; no kilogram moves.
- One new `StatedVsInferredKgWarning`: 2021-22 `Saco (25 Kg)`, 2 rows, served at the stated 25 kg.
- No own-country NSU weights (#1598): the `ehcvm.py` edit would re-warm Mali and Niger.
- `Inteiro` is missing from the size vocabulary and is handled by a test exception; a residual is drafted.

**Roster and derived tables**
- `household_characteristics` and the derived food tables are now served automatically for 2021-22.
- Leavers' MonthsSpent is withheld, so `household_characteristics` 2021-22 matches the survey household (Sex × age counts 40,322).
- The roster gains a `Derivation` column, with 12 keyed rows in 2021-22.
- **The 2018-19 roster now carries an all-NA `Derivation` column.**
- **A `Feature('household_roster')` call that includes Guinea-Bissau gains a `Derivation` column, NA for every other country.**
- **Shared file:** the canonical `lsms_library/data_info.yml` declares `Derivation` optional for `household_roster`, which moves no hash outside Guinea-Bissau.
- 227 of the 341 leavers are in households flagged non-panel.
- `MonthsSpent` is Int64 in two-wave reads.

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
- `test_ehcvm_7day`: the Guinea-Bissau 2021-22 cells, the withheld key, `Inteiro`, and `Tabuleiro (8)`.
- `test_food_kg_size_pool`: the `Inteiro` exception.
- New files:
  - `test_gnb_2021_wave.py`, including the withheld-food test (3,260 purchased, 685 `other`, no 7-day key on `other`) and the withheld-months test;
  - `test_gnb_2018_input_labels.py`.

Addresses #1697, #1107, #1539, #1119, #1226, #1118.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
