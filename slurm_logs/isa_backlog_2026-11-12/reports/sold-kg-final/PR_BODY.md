## Serve reported sold kilograms as `Quantity_sold_kg` (Niger, Mali), GH #1560 / #1571

**Rulings (@ligon, 2026-11-12).**
- **"Treat 2021 as totals."** The sold-kilogram answers are served as the TOTAL kilograms of the sale, in a literal optional column `Quantity_sold_kg` on `crop_production` (#1571 option (a)). That follows the released label and the stored Kg controls (Niger 45/48, Mali 495/495 Kg sales have kg equal to the sold quantity), not the own 2021 forms' "poids en kg d'UNE UML".
- **"Sold-kg details":**
  - 3E is served as recorded, per FIRST cycle; any multiplication waits on #1529.
  - (a) 3E sold kg on records with no sale are masked under a registered rule.
  - (b) The rainy 9999 mask is kept.
  - (c) 2021-22 sold kg carry the "estimate" key (see "Key: an implementation reading" below).
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
- **Site Q (d).** `Quantity_sold_kg` is in `quantity_audit._SCREENED_COLUMNS` and `_NEAR_ZERO_COLUMNS` for `crop_production`, as `Quantity_kg` is: both tails, report-only.
- **Declarations.** The column is optional in both `data_scheme.yml` files and the canonical `data_info.yml`. Both `CONTENTS.org` files are updated, and every ruling is recorded as RULED.

### Key: an implementation reading, open to objection (ruling (c))
Ruling (c) says "the same 'estimate' key". The branch does **not** reuse `survey-kg-estimate`; it mints a per-country **sibling** key, `sold-kg-estimate`. This is the coordinator's implementation reading of the ruling, and the maintainer may object to it.
- **Why.** Two landed tests pin `survey-kg-estimate` to exactly the `Quantity_kg` rows: `tests/test_niger_crop_survey_kg.py::test_the_estimate_key_is_exactly_where_2021_22_has_a_total` and `tests/test_mali_crop_survey_kg.py::test_the_key_is_exactly_where_a_value_is` both assert `keyed == Quantity_kg.notna()`, and both join that key's `inputs` to the s16c harvest rows. The sold kilograms are s16d crop-level sale records, and 553 + 70 of them sit on `Farm-level` rows. Reusing the key would break that landed contract.
- **What the sibling covers.** The sibling's rule is the same "Estimation" conversion. It sits on exactly the 2,954 rows with a `Quantity_sold_kg` (Niger 1,818, 553 of them Farm-level; Mali 1,136, 70 Farm-level), and its `derivation_inputs` join 1:1 on (t, i, crop).
- **If the literal reading is wanted:** the change is to the registry and the tests.
  - Retarget the two survey-kg tests to containment.
  - Merge the rule text and the `inputs` callables.
  - Drop the sibling records.
  - `Derivation` would then change on these 2,954 rows. The key name changes, and a row that now carries both keys would carry one. No other column would move.

### Codes (each module's own evidence)
- **3E 99999.** The documented 3E width; 33 of 39 sit beside `as03eq09c` = 99999.
- **2014-15 9999.** The wave's quantity code and the column max; 7 of 11 are all-code lines.
- **Rainy 9999 (ruled).** It is narrower than the module's 999999.
  - 10 rows at exactly 9999, against 13 in 5,000–9,998.
  - 2 all-code lines.
  - 500–5,000 kg per Tiya/Botte if read as totals.
  - 7 of 8 exceed the line's harvest kg.
  - The author also wrote that the one Kg sale among them breaks kg = quantity, which holds on 151/151 others. **The red team did not reproduce this:** in its count that row has no positive sold quantity. The sentence is still in Niger `CONTENTS.org`.
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
    - the 1,150,000 kg Arachide sale on a row whose harvest unit is `Sac (100 Kg)`, 511× its (t, harvest u, j) cell's reference of 2,250 kg;
    - two Riz sales of 437,500 kg (175×) and 400,000 kg (160×), against a reference of 2,500.
  - Low tail: a 5 kg Coton sale on a row whose harvest unit is `Kg`, 160× below 800.
- The cells key on the row's harvest `u`, as for `Quantity_kg`, where `Quantity_sold` keys on `Unit_sold`. That is a choice about which rows are named, not about any value. The red team recomputed the ladder on `Unit_sold` and on `(t, j)`: the same four Mali rows either way, and only `(t, j)` adds one Niger low-tail row.
- **The message now says what the value is (red team F2).** It used to print the harvest unit beside a kilogram total, `u=Sac (100 Kg) 1,150,000`, which reads as 1,150,000 sacks. For this column it now prints:

  > `Mali/crop_production 2021-22: 3 of 7,498 row(s) report a 'Quantity_sold_kg' more than 100x the 90th percentile of comparable rows with a positive value (each value is the TOTAL kilograms sold on its row, not a count of any unit; comparable rows share the wave, the row's HARVEST unit (harvest_u, not the sale's Unit_sold) and the crop where at least 30 such rows exist; zeros never set the scale): i=<redacted> plot=<redacted> Arachide harvest_u=Sac (100 Kg) 1,150,000 kg sold (x511.1 its cell's p90 of 2,250 kg sold, cell keyed on t,harvest_u,j); …`

  - **The unit is labelled, not dropped**, because the reference really is that harvest-unit cell's p90; without the unit the 2,250 kg would have no visible source.
  - **Wording only.** Only `_format_offender` and `_format_quantity_report` changed. `audit_quantities`, `_reports_for` and the structured report (`u`, `rung`, `value`) are unchanged, so the rows named cannot move. Every other column's text is byte-identical.
  - `Quantity_kg` has the same reading problem and is left unchanged here.

### Disclosed, not changed: answers that read as one sack's weight or a sack count (red team F4)
Under the totals reading, some served answers look like something other than a total. The counts below are the independent red team's (2026-11-12, F4, on served rows of `43632ff5b`), and the two classes are not summed:

| pattern (sized-sack sales with a positive `Quantity_sold_kg`) | Niger | Mali 2021-22 |
|---|---|---|
| **kg of ONE sack:** 5 or more sacks sold, kg within 25% of one sack's stated mass | 25 (2011-12 12, 2014-15 8, 2021-22 5) | 15 (Sac 100: 9, Sac 25: 1, Sac 50: 5) |
| **the sack count typed as kg:** more than one sack, kg = `Quantity_sold` | 4 (2011-12 3, 2021-22 1) | 30 (Sac 100: 24, Sac 50: 6) |

- **The price per kg on these rows.** `Value_sold / Quantity_sold_kg` is off by about `Quantity_sold` on a one-sack row; that is the red team's statement. By the same arithmetic it is off by about the sack's stated mass (50 or 100) on a count row; that refinement is mine.
- **Site Q does not name them**, because each value is an ordinary size for its cell.
- **Not a defect.** The ruling asks for the values as recorded.
- **Where it is recorded:** both `CONTENTS.org` files (per wave and unit) and the canonical `data_info.yml` note for `Quantity_sold_kg`.

### Verification (Savio, private data roots, cold)
- **Base vs candidate (author).** Base is development `e5fdd2fa8`; the candidate was measured at `3199f2f6b`. Niger has 49,042 rows and Mali 50,752, on both sides.
  - `assert_frame_equal` passes after dropping `Quantity_sold_kg` and `Derivation`.
  - `Derivation` changes only by ADDED keys; no key is removed:
    - Niger: 1,825 rows (`sold-kg-estimate` on 1,818; `dry-season-sold-kg-no-sale` on 7).
    - Mali: 1,136 rows (`sold-kg-estimate`).
- **Independent red team at `43632ff5b`: APPROVE.**
  - Every served value matches its source.
  - All 46 other Niger and Mali tables are frame-equal to base, as are Uganda `crop_production`, `harvest_kg` and `gross_crop_revenue`.
  - The Make backend equals the default.
  - `Feature('crop_production')` assembles to 233,958 rows.
- **F2/F4 follow-up at head `5fca3904e1919bd11fdbe9c4ab7eaa426f72a7be`** (job 41252145, `savio4_htc`, private roots, cold):
  - **Cold check.** Mali and Niger `crop_production` were built from an empty private root. Mali has 50,752 rows with 1,136 sold-kg rows; Niger has 49,042 with 4,511.
    - Site Q's ledger names the same 4 Mali rows as before: 2021-22, high 1,150,000 / 437,500 / 400,000 and low 5. It names no Niger row.
    - The ledger equals a direct `audit_quantities` call on the served frame.
    - The structured reports still carry `u` = the harvest unit and rung `t,u,j`.
    - The two emitted warnings carry the new wording quoted above.
  - **Tests.** 2,768 passed, 1 skipped, 0 failed (`-n 4 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`).
    - The skip is `test_quantity_screen.py:699`, which needs a warm Tanzania cache.
    - The run covers every Niger and Mali test file, `test_quantity_screen.py`, `test_gh1482_1485_quantity_diagnostics.py`, `test_gh1569_sold_diagnostics.py`, `test_build_transform_hash.py`, `test_schema_consistency.py`, `test_derivations.py`, and the crop / sold / #1457 / #1462 / #1437 / #824 files (79 files).
    - It includes three new tests: a synthetic wording test with a `Quantity` control, Mali's four-row pin and Niger's zero pin.
    - Separately, `test_gh1493_near_zero_screen.py -m "not requires_s3"` gave 27 passed. Its `requires_s3` tests cold-build Malawi and Senegal and were not run.
  - **Hash census, 38 countries.**
    - `43632ff5b` → head moves 0 of 555 `_table_cache_hash`, 0 of 2,321 `Wave._input_hash`, 0 of 42 `build_transforms_fingerprint` and 0 of 38 `framework_imports_fingerprint` values.
    - Positive control, base `e5fdd2fa8` → head: 49 of 555 tables (Niger 25, Mali 24) and 196 of 2,321 waves (Niger 100, Mali 96), no other country, 0 / 0 fingerprints.
- **Hash footprint (38-country census).**
  - Branch vs base: all Niger and all Mali `_table_cache_hash` and `Wave._input_hash` values move, and no other country's. 0 `build_transforms_fingerprint` and 0 `framework_imports_fingerprint` values move.
  - The Site Q commits (`daaa8477f` → `3199f2f6b`, and the F2 wording) and the F4 disclosure move nothing.
  - **Cost: one Niger and one Mali re-warm.**

### Still open (not this branch)
- A cycle product for 3E: waits on #1529.
- Donor vs recipient state: #1698.
- A default consumer (`median_price_valuation`) for the column.
- The price-implied screen and rung for crop units: a separate design, next batch (#1559).
- Pre-existing, not introduced: 23 of the 242 rainy 2011-12 (household, crop) pairs with two or more sale lines repeat an identical (quantity, kg, value) triple on every line (red team). Worth an issue.

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
> - **Keys and screening.** The 2021-22 values carry a per-country sibling key, `sold-kg-estimate` (2,954 rows), not `survey-kg-estimate` itself. Site Q screens the column, report-only. It names 4 Mali rows, including the 1,150,000 kg sale, and no Niger row, and its message labels them as kilograms sold in the harvest unit's cell.
> - **Disclosed, not changed.** Some answers read as one sack's weight (Niger 25, Mali 15) or as a sack count (Mali 30, Niger 4). On those rows a price per kg is far off, and Site Q does not name them.
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
> Measured, not wired: with `Quantity_sold_kg` as the denominator, Niger's #1457 Farm-level sales with a usable price per kg would go from 251 to 1,040 of 1,051. This was measured at `c7cb7d625`; the no-sale mask touches no sale row. Read it with the F4 disclosure in mind: a usable denominator is not always a right one.
>
> Moved elsewhere:
> - the state question: #1698;
> - any cycle product: #1529;
> - the price-implied rung: #1559, next batch.
>
> **Narrowed to the consumer step only:** whether and how `median_price_valuation` should use `Quantity_sold_kg` by default for these sales. The ruling left that as a separate step.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
