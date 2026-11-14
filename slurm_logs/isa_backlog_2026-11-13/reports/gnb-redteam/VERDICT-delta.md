**Delta verdict: APPROVE, with PR-body must-states and two wording fixes.**
- Candidate `90bdd6742d59390a09189a81050d56ba32466367`, base `f138a3d61c203f892148592675a7ef72e160a322`, previous candidate `c35c95eaf4dfd4c70474e7ba0c63110aa47d4bcc`.
- The worker's claims reproduce cold, in separate private roots for base, candidate and previous candidate.
- Nothing needs a code or config change. The two weaker spots are a miscount in the `household_characteristics` must-state and two `harmonize_food` choices the user should see.

## The eight points

**1. Food wiring holds up.**
- **Rows:** 129,384 (purchased 95,094, produced 20,263, in kind 14,027), index unique.
- **Keys:**
  - The rounding key is on all 129,384 rows and on no 2018-19 row.
  - The 7-day key is on all 95,094 purchased rows and on nothing else.
  - The deletion key appears in `attrs` at 0 rows.
- **Inputs join:**
  - The rounding key's inputs have exactly the served (i, j) pairs, 128,227 against 128,227.
  - The deletion's 3,945 rows meet no served row.
  - The 7-day key's inputs cover all purchased (i, j).
- **Served values:** 46 odd quantities (23 produced, 23 in kind), 0 fractional, 0 zero.
- **The Expenditure gap is mostly not GNB-specific.** There is no 30-day *rule* in the construction; the 30-day part is the instrument's skip. My raw recompute matches the served NA count exactly: 80,647 valued and 14,447 NA purchased rows, made up of three causes.
  - *q7a not asked, 3,673 rows.* The last purchase was "Há mais de 30 dias" (3,318) or "Nunca" (354). The siblings show the same skip: Benin 11,282, Côte d'Ivoire 11,446, Senegal 5,116.
  - *Unit or size mismatch, 10,325 rows.* Siblings show 5k–29k.
  - *q7a rounded to 0, 449 rows.* This is the only GNB-specific cause: 0 in every sibling and in 2018-19.
- **Remap:** the purchase-unit remap rescues 5,559 rows.
- **Where the gap sits:** of the 41.3M, 24.8M of `s07bq08` is on unvalued rows. The other 16.5M is the 7-day quantity against the last-purchase lot on valued rows (ratio 0.786).
- **Ratio against peers:** Expenditure over `s07bq08` is 0.595, against 0.63 for GNB 2018-19 and 0.64–0.98 for the 2021-22 siblings (all waves pinned: 0.55–0.98).
- **The 107/122/126 remap is safe.** Each label is unique within its value-label set, the source labels do not occur in `s07bq03b`, and each target sits at the same code. It is a code bridge.
- **`harmonize_food`:** of the 59 rows, none is plainly mislabelled. Only sugar folds (codes 134 and 135), and 69 codes pass through raw. Two choices change what a cross-wave `j` means (F2).

**2. Spillover onto 2018-19, which goes to the user.** I reconstructed it exactly from `food_kg_factors`: kg 226,907 → 236,031 (+9,124) and the `food_prices` sum 64.91M → 58.14M (−6.77M), on identical rows and quantities.
- **2,638 rows move from `unit` to `item_unit` or `item_unit_tight`.** All of them have `kg_item_n_waves` = 1, and 2018-19 alone serves them `unit`, so their factor now comes from the rounded wave alone.
- **7,618 item-rung rows change factor.** They now take the median of two waves whose estimates differ 2.0× at the median.
- **Price.** Four smoked-catfish cells (`Sillure fumé`) make up 61% of the price fall; the per-piece factor goes 0.125 → 1.0 kg. The top ten cells make up 86%.
- **Kilograms.** Of the +9,124 kg, +6,348 come from pooled `unit` factors (Monte sizes 0.083 → 0.120, Litro). `Oignon frais|Unidade` contributes +1,302.

**3. Leavers.**
- `Departed` is True on 341 rows and False on 41,160; the 41,160 common rows are value-equal to the previous candidate. `MonthsSpent` is `Int64`. The 2018-19 roster values and `household_characteristics` are equal to base.
- **The leavers' effect on `household_characteristics` is overstated (F1).** 12 leavers carry `MonthsSpent` 12, but only 9 people in 2 households are added: totals 40,322 → 40,331. The third household's 3 are not counted; I did not establish why.
- **`Departed` False is right for the 9,727 unasked rows.**
  - 6,805 of the 6,844 in panel households are listed after the last asked member, so they were appended.
  - 2,883 are in non-panel households.
  - In 39 panel-household rows nobody was asked.
  - All 9,727 are in `ehcvm_individu`. NA would mislabel known members as unknown.

**4. Gift/purchase rewire: exact.**
- Base `Purchased` / `Quantity_purchased` equal the candidate's `Received_as_gift` / `Quantity_received_as_gift` on all 5,534 rows, with 0 differences.
- `Purchased` True on 1,231 rows equals raw `s16bq08` Sim. `Quantity_purchased` (1,231 values, 28,418.03) equals raw `s16bq09a`.
- 45 rows are both a gift and a purchase.
- The column names and types match Benin, Togo, Burkina, Côte d'Ivoire and Mali.

**5. The #1598 stop: confirmed.** I re-ran the worker's patch probe: 97 of 555 tables (Burkina Faso 25, Mali 24, Niger 25, Senegal 23) and 194 waves move; `build_transforms_fingerprint` and `framework_imports_fingerprint` 0. The constants are folded through those countries' 2021-22 imports of `food_acquired_with_unit_weights`. A real GNB wiring would import it too.

**6. `StatedVsInferredKgWarning`:** 2 rows, both 2021-22 `Saco (25 Kg)`, served 25 kg on the `metric` rung. It does not fire on a 2018-only request.

**7. Hash census, base → `90bdd6742`:** 22 of 555 tables and 44 of 2,321 waves, all Guinea-Bissau; `build_transforms_fingerprint` 0 of 42, `framework_imports_fingerprint` 0 of 38. My own run.

**8. Tests:** 23 files, 2,135 passed, 90 skipped (none Guinea-Bissau), 4 xfailed, 0 failed. The 2021-22 `sample`, `cluster_features` and `shocks` are frame-equal to `c35c95eaf`; `assets` differs only on the 3 lawnmower rows (now `Appliances`).

## Findings
- **F1 (wording, PR body and REPORT):** say "9 leavers in 2 households enter `household_characteristics` (+9 persons)". The "50,240.5 → 50,250.7" figure adds `log HSize` to the counts.
- **F2 (note, confirm or record in CONTENTS):**
  - `Chá` → `Thé` (916 rows) while `Chá tipo 2` (788) stays raw. 2018-19's `Thé` had 1,244 rows, so 2021's may be narrower.
  - `Chabéu` → `Huile de palme rouge` (1,573) while the literal `Óleo de palma vermelho (Citi)` (3,002) stays raw.
  - 2021's "Outros legumes frescos (Djagatu…)" absorbs Djagatu, so `Jaxatu` exists in 2018-19 food only.
- **F3 (note, CONTENTS idiosyncrasy):** 227 of the 341 leavers are in households flagged non-panel. Only 258 non-panel members were asked `s01q00aa` at all.

## The worker's five questions
1. **Leavers in `household_characteristics`:** the framing holds, except that 9 enter, not 12. EHCVM's 12 means "lived 6 months or more", which a recent leaver can truthfully answer Sim; the release's own household size excludes them.
2. **Spillover:** the framing holds; add the decomposition in point 2. `no_kg` cannot remove evidence, so a `transformations.py` edit is indeed needed to keep 2021-22 out of the pool. Whether #1598's kilogram rows would leave the estimators is unverified.
3. **Deleting the 3,945 zeroed rows:**
   - Togo's ruled treatment of uninvertible rounded rows (served NA, registered) is the closer precedent.
   - The deletion drops 2,739 positive purchase values, 3,575,875 FCFA (3.5% of the release's total), in 2,265 households.
4. **`Departed`:** False for the unasked is correct (point 3). Benin's asymmetry stands.
5. **#1598:** the footprint is confirmed (point 5); own-country weights on rounded quantities would still give rounded kilograms.

## Limits and disclosures
- Not run: `scan.py`, warm and fresh-process reads, the strict levers.
- I made a third no-op `python3 -c 1` on n0035 during this delta. It imported nothing.
- Jobs: 41277345 (build and compare), 41277346 (census, NSU probe, tests), 41277717 (spillover follow-up), 41277731 (plot_inputs compare).

Outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/gnb-redteam/out/delta/`.
