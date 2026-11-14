# Served-wave corrections (2026-11-12): Mali rice block, Senegal salt and near-zero quantities, Niger computed quantity and its revaluation, Uganda 2009-10 ea

This PR implements six maintainer rulings of 2026-11-12 on values the library already serves, and the red team's requested changes. Each correction is registered in the country's `_/derivations.yml` and recorded in its `CONTENTS.org`. Principle (@ligon): "Without a very good reason we don't want to serve up someone else's undocumented imputations."

| change | ruling | registry key | effect |
|---|---|---|---|
| Mali 2018-19 rice-ration block (#1707) | Remove, registered | `Mali::food_acquired::rice-ration-block-withheld` (`rows: 0`) | 3,357 records withheld: 3,356 served rows go and 1 summed row is corrected |
| Senegal 2021-22 salt at a fixed amount per person | Remove, registered | `Senegal::food_acquired::salt-per-capita-withheld` (`rows: 0`) | 100 records withheld (100 rows); the selector reads s07b alone |
| Niger 2018-19 computed `s07bq07a` (#1708) | Flag now, diagnose | `Niger::food_acquired::computed-last-purchase-quantity` | 15,014 purchased rows keyed |
| Niger 2018-19 purchase valuation (#1708) | Revalue at local median | `Niger::food_acquired::flagged-purchase-local-median` | the same 15,014 rows revalued: 1,361 at a cluster median, 11,661 at a region median, 1,992 NA |
| Uganda 2009-10 `ea` (#1536) | Fill, registered | `Uganda::sample::ea-same-household` | 514 of 569 NA `ea` filled; weight with no `ea` goes from 29.32% to 3.53% |
| Senegal 2018-19 near-zero quantities (#1519) | Exact-value NA rule; Expenditure NA too | `Senegal::food_acquired::near-zero-quantity-withheld` (`rows: 0`) | 3 rows served with Quantity NA, 2 with Expenditure NA; no `food_prices` value near 1e22 remains |

## What changed

### Mali 2018-19 rice-ration block (#1707)
- **Which records.** `s07b_me_mli2018.dta` holds 3,357 records with no `vague`; every other record has one. These are no longer served.
- **What they look like.** Each is `Riz local Gambiaka` in kg, one per household, answered `Plus de 30 jours`, with nothing produced, given or bought, and `s07bq03a` a whole multiple of 1.4 kg (1.4 × the welfare `hhsize` on 3,348).
- **The selector is the missing `vague`.** The build checks every such record against that fingerprint and raises `RiceRationBlockError` if one does not fit.
- **Effect on 2018-19.** 161,212 → 157,856 rows. Rice-kg purchases fall from 5,632 rows / 55,895.1 kg to 2,276 rows / 23,581.7 kg; Expenditure is unchanged. One household's block row had been summed into its own rice purchase; that row now serves its answer alone, 5.6 kg less.

### Senegal 2021-22 salt
- **Which records.** 100 salt-in-kg, size-free records whose `s07bq03a` is a whole multiple of **0.0199997** kg. On all 100 the multiple is the household's welfare `hhsize`. No other product or unit meets the constant, and no record equals 0.02 × `hhsize`.
- **The selector reads s07b alone (red team F1).** The first version read `hhsize` from `ehcvm_welfare_sen2021.dta`, which made that file a build input: `food_acquired` and the derived food tables raised `FileNotFoundError` whenever it could not be fetched. The new selector picks the identical 100 records. The `household_size:` declaration is gone, and the wave's `data_info.yml` is development's again, byte for byte. Only the `inputs` callable reads the welfare file, as evidence.
- **The tolerance (1e-9).** The 100 sit at most 3.6e-15 from a whole multiple; the nearest other salt-in-kg record is 4.5e-5 away. Every tolerance from 1e-12 to 1e-5 selects the same 100. A value typed on a 0.01 kg grid sits at least 1.5e-5 away below 600 kg.
- **Rows removed.** 95 purchased and 5 in-kind rows go: 17.04 kg, plus 17.78 FCFA on the 2 rows that had an Expenditure.

### Niger 2018-19 computed `s07bq07a` (#1708): the flag
- **Which records.** 15,264 of 107,263 positive `s07bq07a` (14.2%) are off the quarter grid, and 14,908 of them carry 13–18 decimals. The 2021-22 wave has 15 such values in 112,923. These records become 15,014 purchased rows, all keyed.
- **The premise is corrected (red team F3).** 15,263 of the 15,264 are rows where the questionnaire **enables** `s07bq07a` (`s07bq06` = 1, 2, 3), and their values were reconstructed from a reference unit value. The 21,063 records where the CAPI disables the field but the file fills it are a separate set: `s07bq08` is empty on all of them, so they feed no served value. The first ruling's summary, "CAPI disables it there", described that other set.

### Niger 2018-19 purchase valuation (#1708): revalued at a local median
- **Rule.** On each flagged purchased row, Expenditure = served purchased Quantity × the median genuine (unflagged) last-purchase unit value `s07bq08 / s07bq07a` for the same product × unit × size. The median is taken in the row's cluster (`grappe`) when the cell has at least 3 genuine reports there, otherwise in its region (cover `s00q01`) when the region has at least 1, otherwise NA.
- **Units.** A genuine report prices the cell of its own last-purchase unit and size, compared through #876's unit identity. Nothing is converted between units.
- **Unchanged.** Quantity, `Quantity_kg` and consumed quantities do not move. The rows keep their #876 and flag keys.
- **Floors.** Each genuine report was scored against the median of the other reports in its cell. Within 25% of the held-out price:

  | other reports in the cell | cluster median | region median |
  |---|---|---|
  | 1 | 67.1% | 64.6% |
  | 2 | 63.6% | 67.4% |
  | 3 | 71.7% | 69.1% |
  | 4 | 69.7% | 70.7% |
  | 5 | 73.5% | 70.8% |
  | 10 or more | 87.2% | 84.9% |

  The comparison is noisy below 5 and does not single out 3. It permits it: from 3 on, the cluster median is within a point of the region's or better, and 3 is the smallest median that one stray report cannot move. The region floor is 1 because the ruling withholds only where there is "no genuine local price". Its cost is under **For the maintainer**.
- **Gate.** Every flagged purchased row is revalued, including the 2,787 that #876 left NA, because the ruling says "for the flagged rows". One named constant, `LOCAL_MEDIAN_REVALUES_EVERY_FLAGGED_PURCHASE`, would restrict revaluation to the 12,227 rows #876 values.
- **New build input.** The cover `s00_me_ner2018.dta` supplies the region, and it is declared under `household_region:`. If it cannot be fetched, `food_acquired(waves=['2018-19'])` raises `FileNotFoundError` naming the file, and `waves=['2021-22']` still serves. Not measured: an all-waves call, and the kilogram modes of a 2021-22 subset. Both read every wave (GH #1487), so both should raise too.

| 2018-19 purchases | before | after |
|---|---|---|
| Expenditure, all (FCFA) | 53,351,991.10 | 52,486,790.70 |
| on the 15,014 flagged rows | 13,890,402.00 (26.04%) | 13,025,201.60 (24.82%) |
| flagged rows with an Expenditure | 12,227 | 13,022 |

- **Against the producer's values.** On the 10,812 rows valued both before and after, revalued unit value ÷ the producer's `s07bq08 / s07bq07a`: median 1.00 and exactly equal on 27.5%; p10–p90 0.74–1.25; within 25% 74.9%; within 2× 94.3%.
- **What else moves.**
  - `food_expenditures` and `food_prices(units='unitvalue')` in 2018-19: 1,415 rows go (now NA), 2,210 appear, and 7,932 change.
  - The kilogram modes move in **both** Niger waves, because the inferred kg factors pool across waves (GH #1487). `food_quantities` (kgs) changes 864 rows in 2018-19 and 114 in 2021-22. The totals go from 202,924.9 to 202,852.9 kg and from 208,904.3 to 208,900.9 kg. `food_prices` (kgvalue) changes 66 prices in 2021-22.
  - `food_acquired` 2021-22 and `food_quantities(units='units')` are frame-equal.

### Uganda 2009-10 `ea` (#1536)
- **What is filled.** A household whose own `comm` gives no `ea` takes the same household's label from another wave. The `comm` waves (2005-06, 2010-11 and 2011-12, by HHID) come first; 2013-14 (by `HHID_old`) is used only for the 22 households no `comm` wave records.
- **Labels must agree.** If they do not, the build raises `OriginEaError`. A new synthetic test covers the per-household guard on the comm covers.
- **Result.** 514 of 569 filled. No served `ea` changes, and `v` is untouched.

### Senegal 2018-19 near-zero quantities (#1519)
- **Which records.** Exactly three product-132 records, with `s07bq03a` = 40 × `s07bq07a`, both below 1e-3. They are served with Quantity NA.
- **Expenditure is NA too (ruled).** On the 2 valued rows (6,000 FCFA), Expenditure is NA as well, by ruling @ligon 2026-11-12, "GH1519 Expenditure on the NA rows": "A value computed from a withheld quantity is withheld with it."
- **Effect.** The 2018-19 `food_prices` maximum falls from 1.79e22 to 479,452 FCFA/kg.

### CLAUDE.md
The Site Q table's lower-tail and `food_prices` rows named Senegal's near-zero quantities and 1e22 prices as current. Both rows keep the `997f33c8a` history and now say that those values have been served NA since 2026-11-12, and that the corpus counts were not re-measured.

## Verification
- **Cold builds**, development `e5fdd2fa8` against the candidate `5f113252a` (burst jobs 41253614 and 41253953), each country in its own private root.
  - **Mali:** 3,356 rows go and 1 changes, all in 2018-19.
  - **Senegal:** 100 rows go in 2021-22, and 3 change (2 in Expenditure) in 2018-19.
  - **Uganda:** `ea` and `Derivation` change on 514 rows in 2009-10.
  - **Niger:** `Derivation` changes on 15,014 rows and Expenditure on 11,557 rows, all in 2018-19.
  - **Everything else:** every other wave and row is frame-equal, except Niger's kilogram modes in 2021-22, listed above.
  - Every commit after `5f113252a` touches only tests, `CONTENTS.org`, `derivations.yml` or `AGENTS.md`.
- **`derivation_inputs` joins for every new key.** Each `rows: 0` key returns its records (Mali 3,357, Senegal 100 and 3). Every row that carries a per-row key joins its inputs: the Niger flag 15,014 → 15,264, the revaluation 15,014 → 107,269, Uganda 514 → 1,303. The Niger registered function reproduces the served Expenditure exactly.
- **Welfare outage (Senegal).** With only `ehcvm_welfare_sen2021.dta` unfetchable, in a fresh private root, `food_acquired()` serves all 436,515 rows and `food_expenditures()` serves too. The build asked for the file 0 times, and its blob is absent from the private L1. Positive control: the old branch head raises `FileNotFoundError` for the same simulation.
- **Hash footprint**, development against the candidate:
  - `_table_cache_hash`: 96 of 544 moved (Mali 23/23, Niger 24/24, Senegal 22/22, Uganda 27/27).
  - `Wave._input_hash`: 307 of 2,258 moved (Mali 23/92 and Niger 24/96, 2018-19 only; Senegal 44/44; Uganda 216/216).
  - `build_transforms_fingerprint`: 0 of 41.
  - Nothing moved outside these four countries. **Cost:** one re-warm each.
- **Tests**, targeted selection: every test file naming Mali, Niger, Senegal, Uganda or `ehcvm`, plus `test_derivations`, `test_build_transform_hash`, `test_no_build_input_in_contents_org`, `test_failure_is_not_absence`, `test_schema_consistency`, `test_categorical_mapping_resolution` and `test_gh1578_derivation_duplicates`. Run with `-n 8 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`, in cold private roots:
  - development: 10,021 passed, 190 skipped, 8 xfailed, 0 failed (261 files);
  - candidate `5f113252a`: 10,068 passed, 3 failed. The failures were the expected Niger total pin and two bugs in new tests; all three are fixed in `e12278688`.
  - final code `e12278688`: **10,070 passed, 190 skipped, 8 xfailed, 0 failed** (262 files).
  - The one later commit, `e338fedba`, changes only registry and CONTENTS text. The hash probe at `e12278688` matches the candidate's (0 hashes moved), so no build-path file changed after `5f113252a`.
- **New tests** in `tests/test_gh1707_served_corrections.py`:
  - served-table pins for Mali 2018-19 (157,856 rows; 2,276 rice-kg purchased rows carrying 23,581.7 kg; the block absent) and Niger (exactly 15,014 rows carry the flag, and the same 15,014 the revaluation);
  - the Niger registered function reproduces the served value;
  - the salt build reads s07b alone;
  - synthetic tests of the revaluation function and of its positional write, including a refusal test;
  - the Uganda per-household guard.
- **Re-pinned:** `test_ehcvm_7day`'s Niger 2018-19 total (53,351,991.10 → 52,486,790.70) and its allowed-key set; `test_gh1500`'s two Senegal 2021-22 source digests.

## For the maintainer
- **Not frame-equal outside the target rows.** Niger 2021-22's kilogram modes move: 114 `food_quantities` (kgs) rows and 66 `food_prices` (kgvalue) rows, through the pooled kg inference (GH #1487). Its `food_acquired` is unchanged.
- **New dependency.** Niger 2018-19 `food_acquired` now depends on the cover file.
- **Region floor 1.** 831 region-level rows rest on a single genuine report and 964 on two. Reports whose cluster has fewer than 3 other reports fall back to a region median, and how often that median is within 25% of the held-out price depends on how many other reports it rests on:

  | other reports in the region cell | within 25% |
  |---|---|
  | 1 | 48.5% |
  | 2 | 43.1% |
  | 10 or more | 64.5% |

  A region floor of 3 would serve 3,787 rows NA instead of 1,992. The report counts are in the function's output, so either is a re-derivation.
- **Gate.** Revaluing the 2,787 flagged rows that #876 left NA follows the ruling's wording. Restricting to the 12,227 previously valued rows would give a 2018-19 total of 49,528,911.28.
- **Non-integer `s07bq08`.** 167 genuine reports carry a non-integer `s07bq08`, the same "cannot have been entered" signal as the flagged class. As unflagged reports, they price their cells.

## Issue drafts

**#1708: close.**
> Done (rulings @ligon 2026-11-12, "Flag now, diagnose" and "Revalue at local median").
> - **Flagged.** 15,264 records whose `s07bq07a` is off the quarter grid become 15,014 purchased rows, keyed `Niger::food_acquired::computed-last-purchase-quantity`. They are rows where the questionnaire *enables* the field (15,263 of 15,264); their values were reconstructed from a producer reference unit value per product × unit × size × region. The disabled-but-filled records are a separate set that feeds no served value.
> - **Revalued.** The same rows' Expenditure is now the purchased quantity × the median genuine local last-purchase unit value: cluster with at least 3 reports, else region, else NA. It is keyed `Niger::food_acquired::flagged-purchase-local-median`. 2018-19 purchase Expenditure goes from 53.35M to 52.49M FCFA; the flagged rows' share goes from 26.0% to 24.8%. The producer's values remain in `derivation_inputs`.
> - **Details.** `Niger/_/CONTENTS.org`.

**#1707: close.** As in the earlier draft: the 3,357 no-`vague` records are withheld under `Mali::food_acquired::rice-ration-block-withheld`, and the served pin is in `tests/test_gh1707_served_corrections.py`.

**#1536: close.** 514 of 569 filled under `Uganda::sample::ea-same-household`; 55 remain NA (3.53% of the weight).

**#1519: close.** Quantity NA on the 3 rows, and Expenditure NA on the 2 valued ones by ruling. No `food_prices` value is above 1e10.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
