## Serve reported sold kilograms as `Quantity_sold_kg` (Niger, Mali), GH #1560 / #1571

**Rulings (@ligon, 2026-11-12).**
- **"Treat 2021 as totals."** The sold-kilogram answers are served as the TOTAL kilograms of the sale, in a literal optional column `Quantity_sold_kg` on `crop_production` (#1571 option (a)). That follows the released label and the stored Kg controls (Niger 45/48, Mali 495/495 Kg sales have kg equal to the sold quantity), not the own 2021 forms' "poids en kg d'UNE UML".
- **"Sold-kg details":**
  - 3E is served as recorded, per FIRST cycle; any multiplication waits on #1529.
  - (a) 3E sold kg on records with no sale are masked under a registered rule.
  - (b) The rainy 9999 mask is kept.
  - (c) 2021-22 sold kg carry the "estimate" key.
  - (d) Site Q screens the column, report-only.

### What
| country / wave | source | window | NA: code | NA: 0 beside a sale | NA: no sale (a) | served > 0 | kg |
|---|---|---|---|---|---|---|---|
| Niger 2011-12 rainy | `as02eq12c` | the line's sale | 9999 ×10 (ruled, b) | 0 | 0 | 1,286 | 333,568 |
| Niger 2011-12 3E | `as03eq11c` | 3E.11, the **first cycle**, not × `as03eq06` | 99999 ×39 | 36 | 7 (8,740 kg) | 356 | 364,768 |
| Niger 2014-15 | `AS02EQ12C` | crop-level sale | 9999 ×11 | 26 | 0 | 1,051 | 467,963 |
| Niger 2018-19 | none | | | | | 0 | 0 |
| Niger 2021-22 | `s16dq05c` | crop-level sale | none | 0 | 0 | 1,818 | 380,476 |
| Mali 2021-22 | `s16dq05c` | crop-level sale | none | 4 | 0 | 1,136 | 4,873,574 |

- **Placement.** The column rides with the sale through the existing placement: on its line, or on the crop's one served line, or on the one `Farm-level` row (GH #1457, #1360 one-LINE rule). The wave scripts assert sold-kg conservation onto served rows. `KgFactor_sold` is untouched.
- **No default consumer.** `median_price_valuation` reads the column only via an explicit `kg_qty=`.
- **Registry**, all with `inputs` that return the raw rows:
  - `hh-crop-sale-single-row` (both countries) and `dry-season-sale-window` list the column.
  - New `Niger::crop_production::dry-season-sold-kg-no-sale` (a): a mask on 7 served rows. The rows are harvests and carry the key.
  - New `Niger::crop_production::sold-kg-estimate` and `Mali::crop_production::sold-kg-estimate` (c).
    - These are **siblings** of `survey-kg-estimate`, not that key. Its rule names `Quantity_kg` / `s16cq16c`, its `inputs` are s16c harvest lines, and its tests pin keyed == `Quantity_kg`. The sold kg are s16d crop-level sale records on lines and Farm-level rows.
- **Site Q (d).** `Quantity_sold_kg` is in `quantity_audit._SCREENED_COLUMNS` and `_NEAR_ZERO_COLUMNS` for `crop_production`, as `Quantity_kg` is: both tails, report-only.
- **Declarations.** The column is optional in both `data_scheme.yml` files and the canonical `data_info.yml`. Both `CONTENTS.org` files are updated, and every ruling is recorded as RULED.

### Codes (each module's own evidence)
- **3E 99999.** The documented 3E width; 33 of 39 sit beside `as03eq09c` = 99999.
- **2014-15 9999.** The wave's quantity code and the column max; 7 of 11 are all-code lines.
- **Rainy 9999 (ruled).** It is narrower than the module's 999999.
  - 10 rows at exactly 9999, against 13 in 5,000–9,998.
  - 2 all-code lines.
  - 500–5,000 kg per Tiya/Botte if read as totals.
  - 7 of 8 exceed the line's harvest kg.
  - The Kg sale among them breaks kg = quantity, which holds on 151/151 others.
- **Zeros.** A 0 beside a positive `Quantity_sold` is NA, the existing `Quantity_kg` rule.

### (a) The seven masked 3E records (measured on the cold build)
- 6 have sold flag **Non** with sold quantity and value both missing.
- 1 has flag **Oui** with quantity and value both **0**. It is masked under the same rule, because the rule is the missing sale, not the flag.
- 6 of the 7 kg answers equal the line's harvest kg. All are one-cycle lines with no sold unit.
- Exactly these 7 rows carry the key, and `derivation_inputs` returns them.

### (d) What Site Q names
- **Niger:** nothing, on either tail.
- **Mali 2021-22:** four rows, none changed.
  - High tail:
    - the 1,150,000 kg Arachide sale on a row whose `u` is `Sac (100 Kg)`, 511× its (t,u,j) cell's reference of 2,250 kg;
    - Riz on rows whose `u` is `Sac (100 Kg)`, 437,500 kg (175×) and 400,000 kg (160×), against a reference of 2,500.
  - Low tail: a 5 kg Coton sale on a row whose `u` is `Kg`, 160× below 800.
- The cells key on the row's harvest `u`, as for `Quantity_kg`, where `Quantity_sold` keys on `Unit_sold`. That is a choice about which rows are named, not about any value. The lower tail is included, as for `Quantity_kg`, and it is where the Coton row comes from.

### Verification (Savio, private data roots, cold)
- **Base vs candidate.** Base is development `e5fdd2fa8`; candidate is `3199f2f6b`. The head `43632ff5b` adds CONTENTS.org and one `derivations.yml` assumption sentence that no test has loaded; neither is a build input. Niger 49,042 rows and Mali 50,752 rows, both sides.
  - `assert_frame_equal` passes after dropping `Quantity_sold_kg` and `Derivation`.
  - `Derivation` changes only by ADDED keys; no key is removed:
    - Niger: 1,825 rows (`sold-kg-estimate` on 1,818, 553 of them Farm-level; `dry-season-sold-kg-no-sale` on 7).
    - Mali: 1,136 rows (`sold-kg-estimate`, 70 of them Farm-level).
    - **(c) rows: 2,954**, as expected.
  - Against the previous cut (`c7cb7d625`), `Quantity_sold_kg` moves on exactly the 7 masked rows (8,740 kg).
- **Tests:** 2,765 passed, 1 skipped, 0 failed at `3199f2f6b` (job 41250719, `-n 4 --dist=loadfile`, BLAS pinned). The skip is `test_quantity_screen.py:662`, which needs a warm Tanzania cache this private root does not hold. The list covers every Niger and Mali test file, the crop / sold / quantity-screen / #1457 / #1462 / #1437 / #824 files, `test_build_transform_hash.py`, `test_schema_consistency.py` and `test_derivations.py`.
- **Hash footprint (38-country census).**
  - Branch vs base: all 25 Niger and all 24 Mali `_table_cache_hash` values (49 of 555), and all 100 + 96 `Wave._input_hash` values (196 of 2,321). No other country moves.
  - Unmoved: 0 of 42 `build_transforms_fingerprint` and 0 of 38 `framework_imports_fingerprint` values.
  - The Site Q commit alone (`daaa8477f` → `3199f2f6b`) moves 0 of 555, 0 of 2,321, 0 of 42 and 0 of 38: `quantity_audit` reaches no build fingerprint.
  - **Cost: one Niger and one Mali re-warm.**
- **Build warnings.** Niger: identical to base (5). Mali: base's 19, plus the two new Site Q `QuantityImplausibleWarning`s for `Quantity_sold_kg` (3 high, 1 low).

### Still open (not this branch)
- A cycle product for 3E: waits on #1529.
- Donor vs recipient state: #1698.
- A default consumer (`median_price_valuation`) for the column.
- The price-implied screen and rung for crop units: a separate design, next batch (#1559).

### Issue drafts
**#1560, close:**
> Fixed by this PR, per the 2026-11-12 rulings ("Treat 2021 as totals"; "Sold-kg details").
> - **What is served.** The sold kilograms are the literal total `crop_production.Quantity_sold_kg`:
>   - Niger 2011-12 `as02eq12c`, plus `as03eq11c`, which is per first cycle;
>   - Niger 2014-15 `AS02EQ12C`;
>   - Niger and Mali 2021-22 `s16dq05c`.
> - **What is masked.** Each module's own codes, a 0 beside a positive sold quantity, and, by registered rule, sold kg on the seven 3E records with no sale.
> - **Served positive rows:** Niger 1,642 / 1,051 / 0 / 1,818 by wave; Mali 2021-22 1,136. Reconciled to the raw counts in this issue:
>   - 1,296 → 1,286 after the 9999 mask;
>   - 403 → 402 after the duplicate drop → 363 after the 99999 mask → 356 after the no-sale mask;
>   - 1,062 → 1,051 after the 9999 mask;
>   - 1,818 and 1,136 unchanged.
> - **Keys and screening.** The 2021-22 values carry `sold-kg-estimate` (2,954 rows). Site Q screens the column, report-only; it names 4 Mali rows, including the 1,150,000 kg sale, and no Niger row.
>
> `KgFactor_sold` is unchanged, and nothing consumes the column by default. Still unread here: the 2014-15 crop-level harvest total `AS02EQ110E`. File it separately if wanted.

**#1571, narrow:**
> Settled and implemented in this PR:
> - field meaning: totals;
> - carrier: `Quantity_sold_kg`;
> - codes;
> - the 3E window: first cycle, as recorded;
> - eligibility: no-sale records masked under `dry-season-sold-kg-no-sale`.
>
> Measured, not wired: with `Quantity_sold_kg` as the denominator, Niger's #1457 Farm-level sales with a usable price per kg would go from 251 to 1,040 of 1,051. This was measured at `c7cb7d625`; the no-sale mask touches no sale row.
>
> Moved elsewhere:
> - the state question: #1698;
> - any cycle product: #1529;
> - the price-implied rung: #1559, next batch.
>
> **Narrowed to the consumer step only:** whether and how `median_price_valuation` should use `Quantity_sold_kg` by default for these sales. The ruling left that as a separate step.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
