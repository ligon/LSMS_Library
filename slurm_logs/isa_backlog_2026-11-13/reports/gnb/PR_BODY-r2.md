
**Guinea-Bissau: 2018-19 label bundle and purchase fix; 2021-22 sample, roster (leavers flagged), cluster_features, food_acquired (as released, rounding registered), shocks and assets**

## Summary

**2018-19 (one Guinea-Bissau re-warm):**
- **#1107:** `harmonize_input` (Urea / Phosphate / NPK; DAP stays raw) and a `u_kg` table. `nitrogen_kg` goes from 0 to 188 households, 1,012.5 kg.
- **#1119:** `harmonize_seed_crop`.
- **#1539:** Djagatu → Jaxatu and Beringela → Aubergine. `crop_production` goes from 12,173 to 12,188 rows and its `GrainCollapseWarning` is gone.
- **#1226:** product 94 is bridged to 95.
- **#1118:** a missing input unit is `Unknown` (0 rows).
- **Gift/purchase fix (ruling "Fix all three now"):** `Purchased` / `Quantity_purchased` read the purchase, `s16bq08` / `s16bq09a`. `Purchased` True goes from 423 to 1,231. The old 423 rows are now `Received_as_gift`, beside `Quantity_received_as_gift` and the two unit columns.

**2021-22 (#1697), EHCVM-2 WB 6274, on the Togo/Benin/CIV template:**
- `E2_` ids; no panel link.
- `weight` = `panel_weight` = `hhweight`, the cross-sectional household weight, **not a longitudinal one** (precedents: Mali, Benin 2021-22).
- Strata are region × milieu (#1575; the file's own `strate` agrees one-to-one).
- **Roster:** all 41,501 people are served. The 341 who have left carry the new optional `Departed` = True (ruling "Serve leavers"). **`Departed` is a proposal**: no canonical flag existed. Benin 2021-22 still drops its leavers (#1565).
- **Food:** `food_acquired` is **served as released** (ruling "Serve, registered key").
  - Every positive `s07bq03a` / `s07bq07a` is an even integer.
  - The fit, 2 × round-half-up(x/2), is distributional, cannot be inverted, and I found it documented nowhere.
  - Every row carries `Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even`, whose inputs return the released answers.
  - `Expenditure` derives from the rounded quantities and inherits the rounding.
  - The 3,945 consumed rows rounded to 0 are a registered deletion.
- Shocks follow #1556; assets use the global `Appliances` for the lawnmower.

## Verification

- **Cold, private roots, base `f138a3d61`.** On the 2018-19 slice every table is frame-equal to base, except:
  - the Phase-A relabels;
  - the `plot_inputs` purchase and gift columns;
  - the kilogram pool: 39,916 `food_quantities` rows and 22,573 `food_prices` rows change value.
- **Hash census:** Guinea-Bissau only (22/555 tables, 44/2,321 waves; btf 0/42, fwimp 0/38).
- **Tests:** the 36 targeted files give 3,325 passed; `tests/ -m "not requires_s3 and not slow"` gives 15,520 passed; 0 failed in both.

## Must-state

- 2021-22 food is served as released under a rounding key. `Expenditure` inherits the rounding; 3,945 zeroed rows are deleted; 46 served quantities are odd.
- **The kilogram pool moves 2018-19:** +4.0% kg, and the `food_prices` sum falls 10.4%, on unchanged rows. One new `StatedVsInferredKgWarning` (`saco (25 kg)`).
- No own-country NSU weights (#1598): the `ehcvm.py` edit would re-warm Mali and Niger.
- `Inteiro` is missing from the size vocabulary and is handled by a test exception (residual drafted).
- `household_characteristics` and the derived food tables are now served automatically for 2021-22. 12 leavers with an answered `s01q12` move `household_characteristics`.
- `MonthsSpent` is Int64 in two-wave reads.
- 4,417 of 10,842 shocks are served. Code 25 gets a constructed rank on 34 rows; `Aucune stratégie` is HowCoped0 on 961 rows.
- 14 owned asset lines are not served.
- The generic NPK share is applied; `Tonelada` is undeclared.
- The strata registry relies on a spelling fold.
- The 2021-22 cover GPS is not 2018-19's fix and is served as recorded (#791).
- The shared `kinship.yml` gains `Sobrinho(A)`, which moves no hash.
- At integration: `PARTIALLY_WIRED_SAMPLE`, the coverage regrade, and a recall CSV row for 2021-22.

## Tests re-pinned or added

- The Guinea-Bissau #1575 and gh812 strata scopes.
- The 2018-19 `cluster_features` scope.
- The `ehcvm_condition_originals.json` hash (comment-only).
- `test_ehcvm_7day`: Guinea-Bissau 2021-22 cells, `Inteiro`, and `Tabuleiro (8)`.
- `test_food_kg_size_pool`: the `Inteiro` exception.
- New: `test_gnb_2021_wave.py` and `test_gnb_2018_input_labels.py`.

Addresses #1697, #1107, #1539, #1119, #1226, #1118.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
