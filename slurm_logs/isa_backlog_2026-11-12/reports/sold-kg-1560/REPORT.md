# Sold kilograms (GH #1560 / #1571) and a price-implied crop kg screen (GH #1559, measured only)

Author: Claude Opus 5.5 (fix-and-measure agent, dispatched by Sue, Slurm job 41201840). No sub-agents were spawned.

- **Branch:** `feature/1560-1571-sold-kg-20261112`, from development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`. It is on the mirror only, not on GitHub.
- **Heads:**
  - `b7c1a0d70`: the implementation.
  - `c7cb7d625`: a comment fix. This is the measured head.
  - `826bc9137` and `04ca85822`: CONTENTS.org only, which is not a build input.
  - The follow-up rulings: see "Follow-up rulings applied". The final head is `43632ff5b1f21a14845d4cd01766d01b9649459d`.
- **Jobs.** All ran on savio4_htc with private data roots under `.../isa-sm-20261112/private/sold-kg-1560/`:
  - 41248298: base raw census and cold build.
  - 41248926: candidate build, tests and census at `b7c1a0d70`.
  - 41249490: final-head build, compare, analysis, tests and census at `c7cb7d625`.
  - 41250719: the follow-up rulings at `3199f2f6b`: build, compare2, followup, tests and census, plus a census at `daaa8477f`.
- **Scripts:** `scripts/`. Aggregate outputs only: `out/raw-base/raw_codes.json`, `out/compare-final/compare.json`, `out/part2-final/part2.json`, `out/census-{base,cand,final}.json`, `out/pytest-*.txt`.

## Part 1: implemented

### Country idiosyncrasies this rests on (from Niger and Mali `_/CONTENTS.org`)
- **Niger s16d (2021-22) repeats each crop-level line once per s16c harvest LINE.** Exact repeats are dropped (3,329). A sale goes only on a crop's single served LINE (#1360), otherwise on one Farm-level row (#1457). The sold kg ride on that de-duplicated record: 2,585 raw answers become 1,818.
- **Niger 2011-12 3E is the dry season, asked about the FIRST production cycle.** 3E.11a/c are per cycle, and 3E.12's value window is unstated. Its rows are 2011-12's Farm-level rows. Its code width is 99999, not the rainy module's 999999.
- **Mali Farm-level means several things.** It holds EAC-I tree rows, derived 2021-22 harvests (#1437) and 2021-22 sale-only rows (#1457). The sold kg sit on sale rows only.
- **Quantity codes.** 999999 (2011 rainy), 9999 (2014-15) and 99999 (3E) are masked by exact literal in the wave script (#865). The EAC-I 99/9999 `Manquant` mask is not carried into EHCVM files.
- **The own 2021 forms say kg per ONE UML; the release label says total.** The ruling settles the reading; no export program was found.

### What is served (`crop_production.Quantity_sold_kg`, optional float, a TOTAL in `Quantity_sold`'s window)
| wave | source | code masked | 0 beside a sale -> NA | served > 0 | kg | max |
|---|---|---|---|---|---|---|
| Niger 2011-12 rainy | `as02eq12c` | 9999 x10 | 0 | 1,286 | 333,567.8 | 11,000 |
| Niger 2011-12 3E | `as03eq11c` (per cycle, not x `as03eq06`) | 99999 x39 | 36 | 363 (356 after the (a) mask) | 373,508 (364,768 after (a)) | |
| Niger 2011-12 total | | | | 1,649 (1,642 after (a)) | 707,075.8 (698,335.8) | |
| Niger 2014-15 | `AS02EQ12C` | 9999 x11 | 26 (+1 zero beside q = 0, not on a served row) | 1,051 | 467,963 | 9,150 |
| Niger 2018-19 | none (asks no sold kg) | | | 0 | 0 | |
| Niger 2021-22 | `s16dq05c` | none | 0 | 1,818 | 380,476.38 | 8,000 |
| Mali 2021-22 | `s16dq05c` | none | 4 | 1,136 | 4,873,574 | 1,150,000 |
| Mali 2014-15, 2017-18, 2018-19 | none | | | 0 | 0 | |

- **Every served value is positive.** No zero and no code is served.
- **Reconciled to the #1560 body** (raw counts 1,296 / 403 / 1,062 / 1,818 / 1,136): 1,296 -> 1,286 after the 9999 mask; 403 -> 402 after the duplicate drop -> 363 after the 99999 mask; 1,062 -> 1,051 after the 9999 mask; 1,818 and 1,136 unchanged.
- **`KgFactor_sold` is untouched.** No consumer reads the new column by default, and `median_price_valuation` uses it only as an explicit `kg_qty`.
- **The registry:** `hh-crop-sale-single-row` (Niger, Mali) and `dry-season-sale-window` list the column and raw variables, and their `inputs` return them. No new key was minted at `c7cb7d625`, so `Derivation` was unchanged there. The follow-up rulings add two keys; see below.

### Codes and zeros: decided from each module's own evidence (`out/raw-base/raw_codes.json`)
- **3E 99999 (39 rows).** This is 3E's documented width. 33 of the 39 sit beside `as03eq09c` = 99999, and it is the column's modal value. The implied kg per unit would be 318-99,999.
- **2014-15 9999 (11 rows).** This is the wave's quantity code, and it is the column maximum.
  - 7 of the 11 are all-code lines: q 9999, value 9999999, unit and flag `Manquant`.
  - 8 sit beside `AS02EQ110E` = 9999.
  - The other 4 would be 83-5,000 kg per 50/100 kg sack.
- **Rainy 9999 (10 rows): a judgement, stated as such.** It is narrower than the module's 999999. The evidence:
  - 10 rows at exactly 9999, against 13 anywhere in 5,000-9,998 and 1 above.
  - 2 all-code lines.
  - 500-5,000 kg per Tiya/Botte if read as totals.
  - 7 of the 8 with a harvest kg exceed the same line's `as02eq07c`. Only 24 of 1,286 other sales exceed it.
  - The one sold in Kg would break kg = quantity, which holds on 151 of 151 other Kg sales.
  - It is reversible by removing one literal (`SOLD_KG_SENTINELS`).
- **Zeros.** A 0 beside a positive `Quantity_sold` is NA (3E 36, 2014-15 26, Mali 4). This is the existing `Quantity_kg` rule ("no answer, not a harvest of nothing"). In 3E and Mali every one sits beside a positive quantity and a positive value; in 2014-15, 23 of the 27 zero records have a positive value. A 0 beside no positive sold quantity would stay; none is on a served row.
- **Sold kg often equal the harvest kg** of the same record: 495 of 1,286 rainy sales, 142 of 356 3E sales, and 285 of 1,038 2014-15 sales (against `AS02EQ110E`). This bears on a price-per-kg use and on ruling item 2. Whether sold QUANTITY also equals harvest quantity there was not measured.
- **No other code-like values.** 9 (2011: 12 rows), 98 and 99 occur rarely, are plausible kilograms, and are served.

### Verification
- **Cold, private roots, default backend, base `e5fdd2fa8` vs candidate `c7cb7d625`.**
  - Niger 49,042 = 49,042 rows; Mali 50,752 = 50,752 rows.
  - `assert_frame_equal(base, cand.drop(columns='Quantity_sold_kg'))` passes for both. The index is equal in order, and the other columns are the base's in base order.
  - At `c7cb7d625`, only `attrs['derivations']` differs: the two records' `columns` lists. The new dtype is `Float64`.
  - Build warnings are identical to base: Niger 5, Mali 19.
- **Kg controls on served rows.**
  - Niger: 220 of 230 Kg sales have kg = sold quantity (rainy 151/151, 2014-15 20/22, 2021-22 45/48, 3E 4/9).
  - Mali: 495 of 495.
- **Price support on sale rows** ("usable" = positive `Value_sold` over a positive kg denominator; "now" = `transformations._sold_kg_factors` applied to `Quantity_sold`; "with" = `Quantity_sold_kg`):

| wave / where | sales | usable now | with sold kg | gain | lose |
|---|---|---|---|---|---|
| Niger 2011-12 Farm-level (3E) | 438 | 256 | 350 | 121 | 27 |
| Niger 2011-12 on a line | 1,294 | 306 | 1,286 | 980 | 0 |
| Niger 2014-15 Farm-level | 498 | 107 | 487 | 383 | 3 |
| Niger 2014-15 on a line | 583 | 251 | 562 | 321 | 10 |
| Niger 2021-22 Farm-level | 553 | 144 | 553 | 409 | 0 |
| Niger 2021-22 on a line | 1,265 | 556 | 1,265 | 709 | 0 |
| Mali 2021-22 Farm-level | 70 | 68 | 70 | 2 | 0 |
| Mali 2021-22 on a line | 1,070 | 995 | 1,065 | 73 | 3 |

  - **Share of Farm-level sales that would gain a usable price per kg:**
    - Niger 2014-15 + 2021-22: 792 of 1,051 (75.4%). Support goes 251 (23.9%) to 1,040 (99.0%), matching the investigation's 251 -> 1,041, which did not mask the codes.
    - Niger 3E: 121 of 438 (27.6%).
    - Mali: 2 of 70.
    - Not wired.
- **Tests.** 2,677 passed, 0 failed, 0 skipped at `c7cb7d625` (job 41249490, `-n 4 --dist=loadfile`, BLAS pinned, `-rs`). The `requires_s3` tests ran.
  - Scope: every `tests/test_*niger*.py` (excluding Nigeria), every `tests/test_*mali*.py`, `test_build_transform_hash.py`, `test_schema_consistency.py`, `test_derivations.py`, and the crop / sold / median-price / #1457 / #1462 / #1437 / #1476 / #1267 / #824 files. The list is in `out/pytest-final-41249490.files`.
  - New: `tests/test_niger_crop_sold_kg.py`, `tests/test_mali_crop_sold_kg.py`. Their oracles read the `.dta` files directly.
  - Re-pinned: `test_gh1462` (SALE columns) and `test_gh824_mali` (record columns).
  - At `b7c1a0d70` the run was 2,676 passed and 1 failed: `test_niger_crop_survey_kg::test_no_wave_turns_the_total_into_a_factor` was tripped by a comment naming `KgFactor_sold`, fixed in `c7cb7d625`.
- **Hash census** (38 countries, base vs `c7cb7d625`, and the same vs `b7c1a0d70`):
  - Moved: 25/25 Niger and 24/24 Mali `_table_cache_hash` values (49 of 555), and 100/100 + 96/96 `Wave._input_hash` values (196 of 2,321). No other country's moved.
  - Unmoved: 0/42 `build_transforms_fingerprint` and 0/38 `framework_imports_fingerprint` values.
  - **Cost: one Niger and one Mali re-warm.**
- **Hygiene.** In-tree parquets after each job: 0. `issues.log`: 0 lines. No `dvc` CLI was used. All reads went through `get_dataframe` / `get_data_file` into private L1. **Disclosed:** on n0035 I ran two trivial interpreter invocations: a `python3` YAML import that failed, and `.venv.lustre/bin/python -c "print(1)"`. Neither touched data or a cache. All builds, tests and analyses ran in Slurm jobs. That round used 3 Slurm jobs; the follow-up rulings used 1 more (41250719), 4 in all.

### Needs a ruling (brought back, not guessed): RULED 2026-11-12, see the next section
1. **3E cycle window.** Sold kg are served per (first) cycle beside `Quantity_sold`, NOT multiplied by `as03eq06`. On the 43 multi-cycle 3E sales, `Quantity_sold_kg / Quantity_kg` is not a sold share. This departs from GH #1436 comment 6648014838. A cycle product would be its own registered derivation.
2. **Kg-only 3E records.** Seven 3E records state sold kg and no positive sale: 8,740 kg; the sold flag says Non on 6 and Oui on 1; 6 equal their harvest kg. They are served as recorded, following the file's precedent for 3E "no sale" values. They are not sales to any consumer, and no key moves. NA them?
3. **2021-22 estimate key.** The 2021-22 sold kg are the same "Estimation" conversion as the harvest. Should they carry an identity key like `survey-kg-estimate`? Not minted, because it moves `Derivation` on 2,954 rows (Niger 1,818 + Mali 1,136).
4. **Site Q.** `Quantity_sold_kg` is not in Site Q's `_SCREENED_COLUMNS`. Mali's 1,150,000 kg sale, and sack rows whose kg equal the sack count, are served unscreened.
5. **Rainy 9999.** Confirm the rainy 9999 mask, which is narrower than the module's width.

### Follow-up rulings applied (@ligon 2026-11-12, "Sold-kg details"), one commit each
They supersede the "Needs a ruling" list above.

- **Heads:** `64b6ab515` (1) → `0b2a67b3e` (b) → `c7b1b4598` (a) → `daaa8477f` (c) → `3199f2f6b` (d, tested) → `9a62d912d` (docs and registry text) → `43632ff5b1f21a14845d4cd01766d01b9649459d` (Mali CONTENTS wording, final).
- **Job:** 41250719, cold, private root.

1. **Dry season.** Served as recorded, as the FIRST-cycle sale. This is disclosed in Niger `data_scheme.yml`, in the canonical `data_info.yml` note, in `dry-season-sale-window` and in CONTENTS. Any multiplication waits on #1529.
2. **(a) Mask, registered as `Niger::crop_production::dry-season-sold-kg-no-sale`.**
   - The rule: on a 3E record with no positive `as03eq11a` and no positive `as03eq12` (after codes), `Quantity_sold_kg` is NA, whatever `as03eq10` says. The row is a harvest and is served, carrying the key; `derivation_inputs` returns the records.
   - **Exactly 7 records match (8,740 kg).**
     - 6 have flag **Non**, with sold quantity and value both missing.
     - 1 has flag **Oui**, with both **0**. It is masked under the same rule, because the rule is the missing sale.
     - 6 of the 7 equal their harvest kg (Non 5 of 6, the Oui one too). All are one-cycle lines with no sold unit.
   - Exactly these 7 served rows carry the key, all Farm-level 2011-12.
   - Against `c7cb7d625`, `Quantity_sold_kg` moves on exactly these 7 rows.
   - 3E now serves 356 sold kg, 364,768 kg; Niger 2011-12 serves 1,642, 698,335.8 kg.
3. **(b) Rainy 9999.** Kept, and recorded as ruled in the script comment and CONTENTS.
4. **(c) Estimate key.** Sibling keys `Niger::crop_production::sold-kg-estimate` and `Mali::crop_production::sold-kg-estimate`.
   - Not `survey-kg-estimate` itself: its rule text names `Quantity_kg` / `s16cq16c`, its `inputs` are s16c harvest lines, and its tests pin keyed == `Quantity_kg`. The sold kg are s16d sale records.
   - **Actual rows: 2,954.** Niger 1,818 (553 Farm-level) and Mali 1,136 (70 Farm-level), exactly the rows with a `Quantity_sold_kg`.
   - Re-pinned: `test_gh824_mali` reads Farm-level keys by containment. Mali's `survey-kg-estimate` rule no longer says `s16dq05c` is unserved.
5. **(d) Site Q.** `Quantity_sold_kg` was added to `_SCREENED_COLUMNS` and `_NEAR_ZERO_COLUMNS` for `crop_production`, as `Quantity_kg` is: both tails on the (t,u,j) ladder, report-only.
   - **Niger:** names nothing.
   - **Mali 2021-22:** 4 rows, values unchanged.
     - High tail:
       - Arachide, on a row whose `u` is `Sac (100 Kg)`: 1,150,000 kg, 511× its (t,u,j) reference of 2,250 kg.
       - Riz, on rows whose `u` is `Sac (100 Kg)`: 437,500 kg (175×) and 400,000 kg (160×), against a reference of 2,500.
     - Low tail: Coton, on a row whose `u` is `Kg`, 5 kg, 160× below 800.
   - **Cells.** On this column, Site Q's cells key on the row's harvest `u`, as for `Quantity_kg`, whereas `Quantity_sold` keys on `Unit_sold`. This is a choice about which rows get named, not about any served value. The offender's `u` above is that harvest `u`.
   - **Tails.** The lower tail was added as well as the upper, as for `Quantity_kg`. The 5 kg Coton row comes from it.
   - **Not run:** the Make backend and `Feature()`.
   - Mali reads gain these two `QuantityImplausibleWarning` lines; Niger's warnings are unchanged.

**Verification at `3199f2f6b`.**
- **Frames.** Niger 49,042 and Mali 50,752 rows, both sides. `assert_frame_equal` to base passes after dropping `Quantity_sold_kg` and `Derivation`.
- **`Derivation`.** It changes only by ADDED keys; no key is removed:
  - Niger: 1,825 rows (1,818 `sold-kg-estimate` + 7 `dry-season-sold-kg-no-sale`).
  - Mali: 1,136 rows.
- **Tests:** 2,765 passed, 1 skipped, 0 failed. The skip is `test_quantity_screen.py:662`, which needs a warm Tanzania cache. `test_quantity_screen.py` and `test_gh1482_1485_quantity_diagnostics.py` were added to the list.
- **Hash census, 38 countries.**
  - Branch vs base: 49 of 555 tables (Niger 25/25, Mali 24/24) and 196 of 2,321 waves (Niger 100/100, Mali 96/96). No other country moves.
  - Unmoved: 0 of 42 `build_transforms_fingerprint` and 0 of 38 `framework_imports_fingerprint` values.
  - **The Site Q commit alone (`daaa8477f` → `3199f2f6b`) moves 0 / 0 / 0 / 0.** `quantity_audit`'s callables are in `_EXCLUDED_CALLABLES` and its constants are reached by no fingerprint.
- **Hygiene.** In-tree parquets: 0. `issues.log`: 0 lines.
- **Not re-tested.** The final head's `derivations.yml` assumption sentence (which record is the Oui one) was not re-tested; `derivations.yml` is not a build input.

## Part 2: measured only (GH #1559). Ruled 2026-11-12 "Also build a rung": a separate design in the next batch, NOT on this branch

> "Can we not use the same idea as with units for food_quantities, where we infer units from expenditures (assuming quantity prices the same within a period-region)?"

**Method** (`scripts/part2.py`, run on the final-head cold build).
- **Price-implied kg per unit** for (wave, region -> national, crop [, state], unit) = the median price per unit of sales in that unit (`Value_sold/Quantity_sold`) divided by the median price per kg.
- **Gates, the food ladder's:** at least 5 reports on each side, and each side's spread at most 10 (p90/p10 at n >= 10, max/min below 10).
- **Compared with** the survey's `Quantity_kg / Quantity` on harvest rows in that unit.
- **Region** is `cluster_features.Region` on (t, v).
- **3E rows are excluded**, both as donors and as recipients, because of their window.
- **Two per-kg baselines:**
  - **(A) Kg-labelled sales only.** The strict analogue.
  - **(B) Every sold unit that states a mass, at nominal** (Kg, Sac de 50/100 kg, Gramme). This is what the food ladder's metric labels do.

**Coverage**
- **Niger has very few Kg sales:** 160 / 22 / 21 / 48 of 1,711 / 1,075 / 3,226 / 1,818 sales.
  - Under (A), 11 national kg baselines pass, giving 21 implied (wave, crop, unit) cells nationally and 13 with region.
  - Survey-kg rows in units with no stated mass covered under (A):

| wave | rows covered | share of rows | kg covered | share of kg |
|---|---|---|---|---|
| 2011-12 | 1,978 of 6,937 | 28.5% | 113,541 of 948,261 | 12.0% |
| 2014-15 | 0 of 7,246 | 0% | 0 | 0% |
| 2021-22 | 2,842 of 9,798 | 29.0% | 207,052 of 2,196,224 | 9.4% |

  No Botte cell with 20+ covered rows appears under (A). Botte is the largest kg carrier.
- **Under (B):**

| wave | rows covered | share of rows | kg covered | share of kg |
|---|---|---|---|---|
| 2014-15 | 6,394 of 7,246 | 88.2% | 1.62M of 1.82M | 88.9% |
| 2021-22 | 9,020 of 9,798 | 92.1% | 2.01M of 2.20M | 91.5% |
| 2011-12 | 1,978 | unchanged from (A); the reason is not established | | |

  Implied cells under (B): 31 with region, 25 national.
- **Mali has plenty of Kg sales** (119 / 140 / 493 / 494), but almost nothing to screen: 306 survey-kg rows, all 2021-22, 70 of them in mass-less units, and 1 covered.

**Agreement (survey / price-implied, mass-less units)**
- (A) 2011-12: median 1.14, p10-p90 0.81-1.79, 92% within 2x, 7 rows beyond 10x.
- (A) 2021-22: median 1.25, p10-p90 1.15-1.52, 97% within 2x, 4 beyond 10x.
- (B):

| wave | median | within 2x | beyond 10x |
|---|---|---|---|
| 2011-12 | 1.19 | 88% | 8 |
| 2014-15 | 1.14 | 88% | 116 (1.8%) |
| 2021-22 | 1.25 | 89% | 27 |

- By unit under (B):
  - Tiya: 1.08 and 1.13, 90-96% within 2x.
  - Botte: 1.24 and 1.42, 83-86% within 2x.
- **The survey sits systematically 10-40% above the price-implied mass.** In 2021-22 that is its 2.5 kg Tiya conversion against about 2.0-2.2 implied.
- **Beyond 10x, the disagreements are a few cells, not scattered rows:**
  - 2014-15 Sorgho/Botte: 33 rows, survey 108 kg per botte against 8.
  - 2014-15 Niebe/Tiya: 25 rows, 28 against 2.5.
  - 2014-15 Mil/Botte: 24 rows.
  - 2021-22 Mil/Botte with region: 15 rows, 200 against 8.8.
  - Smaller: Sorgho/Tiya, Mil/Tiya, Sorgho/Botte 2021-22.

  These look like sacks or totals entered against a small-unit label.
- **Mali (A).** 85 covered rows, mostly stated sacks: median 0.91, 46% within 2x, and 23 beyond 10x, 21 of them low. These are the 1-2 kg per 100 kg sack rows that `kg_survey_off_label` already names.

**Second signal: sold kg per sold unit (the new column)**
- **It is not independent.** On sales in the harvest's own unit, sold kg per unit equals harvest kg per unit exactly on:

| | equal | of |
|---|---|---|
| Niger 2011-12 | 1,107 | 1,574 |
| Niger 2014-15 | 337 | 499 |
| Niger 2021-22 | 939 | 1,134 |
| Mali | 26 | 32 |

  The median ratio is 1.000 throughout. 2021-22 Tiya is exactly 2.5 on 61% of sales and 65% of harvest lines. One conversion is applied to both answers.
- **Against price-implied (A):**
  - Niger 2011-12: median 1.13, 93% within 2x.
  - Niger 2021-22: median 1.25, 88% within 2x.
  - Mali: median 1.00, but 65% within 2x and 50 of 475 beyond 10x.

  This tracks the survey-vs-price gap.

**Caveats, measured**
- **State.**
  - The survey's kg per unit does not see the state. Niger 2021-22 Arachide is 2.5 per Tiya and 100 per 100 kg sack in both states.
  - Prices do:
    - Niger 2021-22 Arachide per Tiya: 1,000 shelled against 425 unshelled (n 62/61).
    - Per kg in Kg sales: 608 against 140 (n 6/6, thin).
    - Niger 2018-19 per Tiya: 840 against 400.
    - Mali per kg: 400 against 271 (2021-22) and 300 against 125 (2018-19); cotton is equal.
  - A state-blind price ratio therefore mixes value per kg with mass.
  - Donors keyed by `Condition_sold` and recipients by `condition` exist only in 2021-22 (the ECVMA waves ask no state):
    - (A): 2,783 rows, median 1.25, 96% within 2x.
    - (B): 4,187 rows, median 1.13, 96% within 2x, 1 beyond 10x.
  - The sold state differs from the harvest state on 105 of 1,265 (2021-22) and 161 of 3,226 (2018-19) Niger sale rows. This is the #1698 question.
- **Selection.**
  - Only 21-24% of Niger's survey-kg rows belong to a household-crop that sold anything.
  - Within (wave, crop, unit), sellers' survey kg per unit match non-sellers'. Over 54 cells the ratio of medians is p10 0.82, p50 1.00, p90 1.09; the largest is 4.0, 2014-15 Niebe per 100 kg sack.
  - So there is no sign that sellers' units differ, but the factor is estimated on about a fifth of the rows.
- **Seasonality.**
  - Mali per-kg prices in Kg sales vary across interview months within (wave, crop) by a max/min of 1.0-2.8, median about 1.3 over 12 cells. Cotton: 1.32 in 2021-22 and 1.49 in 2018-19.
  - Niger could not be measured: only 69 of 251 Kg sales join an interview date.
  - Seasonality biases the ratio only where unit and Kg sales differ in timing. The cells are too thin to separate that.

**Recommendation**
- **Screen (report-only, per the 2026-11-05 "report, never refuse" ruling): worth building for Niger.**
  - A sibling of `kg_survey_off_label` in `harvest_kg_factors` (for instance `kg_survey_off_price`).
  - It names a harvest row whose survey kg per unit lies outside 0.1x-10x of the price-implied value for its (wave, region -> national, crop, unit), adding state in 2021-22.
  - It discloses the implied factor, its rung and both cell sizes. It refuses nothing.
  - It needs baseline (B), because Kg-only coverage is 0% in 2014-15. Under (B) it would name about 150 Niger rows (8 / 116 / 27), concentrated in a handful of Botte/Tiya cells.
  - Keep the 10x band: with the systematic 1.1-1.4x offset, a 2x band would flag 11-12% of covered rows under (B), and 17% of 2021-22 Botte.
  - It adds nothing for Mali, which has no mass-less survey kg to screen.
- **Price-implied inferred rung for `harvest_kg_factors`: not sound as designed today. Keep it separate from the food ladder, and do not build it yet.** [Superseded: ruled 2026-11-12, "Also build a rung", as its own design in the next batch. The caveats below are carried into that design.]
  - **The baseline is missing where it is needed.** The rows that would need such a rung have no survey kg: Niger 2018-19 (13,244 positive rows on `none`) and Mali EAC-I / 2018-19. Niger 2018-19 has 21 Kg sales of 3,226, and its sacks state no mass, so neither baseline exists there. This is inferred from sale counts; no rung was simulated.
  - **The ratio is state-confounded** where state is not recorded.
  - **The survey and price-implied masses differ systematically by 10-40%.**
  - **Baseline (B) inherits the sacks' known under-weight.** The survey reads a 100 kg sack lighter.
  - Where survey kg exist, `survey_kg` already serves them.
  - If a rung is wanted, Mali is the candidate, because it has plenty of Kg sales. It should be its own registered design: keyed on sold unit = harvest unit plus state, and validated first against Mali 2021-22's and Niger's survey kg.
