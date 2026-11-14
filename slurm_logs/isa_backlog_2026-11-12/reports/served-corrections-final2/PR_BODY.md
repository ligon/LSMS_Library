# Served-wave corrections (2026-11-12): Mali rice block, Senegal salt and near-zero quantities, Niger computed quantity and its revaluation, Uganda 2009-10 ea

This PR implements the maintainer's rulings of 2026-11-12 on values the library already serves, and the red team's requested changes. Each correction is registered in the country's `_/derivations.yml` and recorded in its `CONTENTS.org`. Principle (@ligon): "Without a very good reason we don't want to serve up someone else's undocumented imputations."

| change | ruling | registry key | effect |
|---|---|---|---|
| Mali 2018-19 rice-ration block (#1707) | Remove, registered | `Mali::food_acquired::rice-ration-block-withheld` (`rows: 0`) | 3,357 records withheld: 3,356 served rows go and 1 summed row is corrected |
| Senegal 2021-22 salt at a fixed amount per person | Remove, registered | `Senegal::food_acquired::salt-per-capita-withheld` (`rows: 0`) | 100 records withheld (100 rows); the selector reads s07b alone |
| Niger 2018-19 computed `s07bq07a` (#1708) | Flag now, diagnose | `Niger::food_acquired::computed-last-purchase-quantity` | 15,014 purchased rows keyed |
| Niger 2018-19 purchase valuation (#1708) | Revalue at local median; Region floor of 3; Only previously valued | `Niger::food_acquired::flagged-purchase-local-median` | the 12,227 flagged rows #876 values are revalued: 1,107 at a cluster median, 8,248 at a region median, 2,872 NA. The 2,787 flagged rows #876 left NA stay NA |
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
- **Rulings.** "Revalue at local median" (@ligon 2026-11-12): "For the flagged rows, Expenditure = purchased quantity x the median GENUINE (unflagged) last-purchase unit value for the same product x unit x size in the same cluster, falling back to region; rows with no genuine local price get NA." Then "GH1708 revaluation details, and the release plan" (@ligon 2026-11-12):
  - **"Region floor of 3":** a local median needs >= 3 genuine reports at cluster AND at region level; otherwise NA.
  - **"Only previously valued":** revalue only flagged rows that already carried a served Expenditure under #876; rows the library left NA (2,787) stay NA. "The ruling corrects a construction, it does not add new values."
- **Rule.** On each flagged purchased row that #876 values, Expenditure = served purchased Quantity × the median genuine (unflagged) last-purchase unit value `s07bq08 / s07bq07a` for the same product × unit × size. The median is the cluster's (`grappe`) when the cell has at least 3 genuine reports there, otherwise the region's (cover `s00q01`) when the region cell has at least 3, otherwise NA.
- **Scope.** "Previously valued" is read by calling #876's own function, `ehcvm.derive_ehcvm_purchase_value`, on the inputs, and the hook checks it against the value it served before writing by position. The `LOCAL_MEDIAN_REVALUES_EVERY_FLAGGED_PURCHASE` switch of the first implementation is gone. The 2,787 rows #876 serves NA keep that NA and carry the flag key alone.
- **Units.** A genuine report prices the cell of its own last-purchase unit and size, compared through #876's unit identity. Nothing is converted between units.
- **Unchanged.** Quantity, `Quantity_kg` and consumed quantities do not move. Revalued rows keep their #876 and flag keys.
- **The cluster floor of 3.** Each genuine report was scored against the median of the other reports in its cell. Within 25% of the held-out price:

  | other reports in the cell | cluster median | region median |
  |---|---|---|
  | 1 | 67.1% | 64.6% |
  | 2 | 63.6% | 67.4% |
  | 3 | 71.7% | 69.1% |
  | 4 | 69.7% | 70.7% |
  | 5 | 73.5% | 70.8% |
  | 10 or more | 87.2% | 84.9% |

  The comparison is noisy below 5 and does not single out 3. It permits it: from 3 on, the cluster median is within a point of the region's or better, and 3 is the smallest median that one stray report cannot move. At the region, for reports whose cluster has fewer than 3 others, a region median of 1 or 2 other reports was within 25% of the held-out price only 48.5% / 43.1% of the time, which is what the ruled floor withholds.
- **New build input.** The cover `s00_me_ner2018.dta` supplies the region, and it is declared under `household_region:`. If it cannot be fetched, `food_acquired(waves=['2018-19'])` raises `FileNotFoundError` naming the file, and `waves=['2021-22']` still serves (measured on the first implementation; the cover read is unchanged). Not measured: an all-waves call, and the kilogram modes of a 2021-22 subset. Both read every wave (GH #1487), so both should raise too.

Measured cold, development `e5fdd2fa8` against `7644de2b5` (burst job 41254597), each in its own private root:

| 2018-19 purchases | before (#876) | after |
|---|---|---|
| Expenditure, all (FCFA) | 53,351,991.10 | 46,636,756.04 (−12.6%) |
| on the 15,014 flagged rows | 13,890,402.00 (26.04%) | 7,175,166.95 (15.39%) |
| flagged rows with an Expenditure | 12,227 | 9,355 |

- **Levels**, of the 12,227 revalued rows: cluster 1,107 (9.1%), region 8,248 (67.5%), NA 2,872 (23.5%). Of the NA rows, 1,415 have no genuine report in their region cell and 1,457 have 1 or 2. With the 2,787 left NA, 5,659 of the 15,014 flagged rows are served NA. The ruling's "about 3,787 NA" estimate was taken over all 15,014 rows under the first scope; it is reconciled under **For the maintainer**.
- **Against the producer's values.** On the 9,355 rows valued both before and after, revalued unit value ÷ the producer's `s07bq08 / s07bq07a`: median 1.00 and exactly equal on 28.9%; p10–p90 0.73–1.25 (p5–p95 0.62–1.31); within 10% 50.9%, within 25% 77.7%, within 2× 95.8%. Cluster level (1,107): p10–p90 0.71–1.22, within 25% 69.0%. Region level (8,248): p10–p90 0.73–1.25, within 25% 78.9%.
- **Registered function.** `derivation_inputs` returns all 107,269 records (15,264 flagged). The registered function on them gives 12,227 rows and reproduces the served Expenditure exactly (max difference 0, same NA pattern). Every revalued row joins its inputs.
- **What else moves (against development).**
  - `food_acquired` 2018-19: only Expenditure (9,618 rows, all carrying the new key) and `Derivation` (15,014 rows: the 12,227 gain the flag and the new key, the 2,787 the flag alone). The index, Quantity and `Quantity_kg` are equal. **2021-22 is frame-equal**; Niger serves `food_acquired` in these two waves only.
  - `food_expenditures` and `food_prices(units='unitvalue')` 2018-19: 2,872 rows go (now NA), 6,746 change, and none appear. 2021-22 is frame-equal.
  - The kilogram modes move in **both** waves, because the inferred kg factors pool across waves (GH #1487). `food_quantities` (kgs) changes 810 rows in 2018-19 and 93 in 2021-22; the totals go from 202,924.9 to 202,940.0 kg and from 208,904.3 to 208,906.0 kg. `food_prices` (kgvalue) loses the 2,872 rows and changes 6,894 in 2018-19, and changes 57 in 2021-22.
  - `food_quantities(units='units')` is frame-equal.
- **Against the first scope (`e338fedba`).** Expenditure changes on 3,667 rows, all flagged: 2,210 of the 2,787 that it had valued are NA again, and the region floor withholds 1,457. The 9,355 valued by both are identical. `food_expenditures` loses those 3,667 rows and changes none.

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
- **Cold builds.**
  - **Niger:** development `e5fdd2fa8`, the first implementation `e338fedba` and `7644de2b5` (burst job 41254597), each in its own private root. The results are above.
  - **Mali, Senegal, Uganda:** development `e5fdd2fa8` against `5f113252a` (burst jobs 41253614 and 41253953). Mali: 3,356 rows go and 1 changes, all in 2018-19. Senegal: 100 rows go in 2021-22, and 3 change (2 in Expenditure) in 2018-19. Uganda: `ea` and `Derivation` change on 514 rows in 2009-10. Every other wave and row is frame-equal.
  - **Why those builds still stand.** Since `5f113252a` the only build-path change is Niger 2018-19 `mapping.py` (in `7644de2b5`). Every other later commit touches tests, `CONTENTS.org`, `derivations.yml` or `AGENTS.md`, and none of those is a build input.
- **`derivation_inputs` joins for every new key.**
  - Each `rows: 0` key returns its records: Mali 3,357, Senegal 100 and 3.
  - Every row that carries a per-row key joins its inputs: the Niger flag 15,014 → 15,264, the revaluation 12,227 → 107,269, and Uganda 514 → 1,303.
- **Welfare outage (Senegal).** With only `ehcvm_welfare_sen2021.dta` unfetchable, in a fresh private root, `food_acquired()` serves all 436,515 rows and `food_expenditures()` serves too. The build asked for the file 0 times, and its blob is absent from the private L1. Positive control: the old branch head raises `FileNotFoundError` for the same simulation.
- **Hash footprint.**
  - Development against `7644de2b5`: `_table_cache_hash` 96 of 544 moved (Mali 23/23, Niger 24/24, Senegal 22/22, Uganda 27/27); `Wave._input_hash` 307 of 2,258 moved (Mali 23/92 and Niger 24/96, 2018-19 only; Senegal 44/44; Uganda 216/216); `build_transforms_fingerprint` 0 of 41. Nothing moved outside these four countries. **Cost:** one re-warm each.
  - The first implementation against `7644de2b5`: Niger 24/24 table hashes and the 24 2018-19 wave hashes, nothing else.
  - `7644de2b5` against the final head `06bac7429`: 0 of 544, 0 of 2,258 and 0 of 41 moved. That commit changes only `Niger/_/CONTENTS.org`, `Niger/_/derivations.yml` and `tests/test_ehcvm_7day.py`.
- **Tests.** The targeted selection is every test file naming Niger or `ehcvm` (125 files), including `test_derivations`, `test_build_transform_hash`, `test_no_build_input_in_contents_org`, `test_failure_is_not_absence`, `test_schema_consistency`, `test_gh1578_derivation_duplicates` and `test_gh1707_served_corrections`. It ran with `-n 8 --dist=loadfile`, BLAS pinned to one thread and `LSMS_BUILD_WORKERS=1`, in cold private roots.
  - `7644de2b5` (burst job 41254597): 5,939 passed, 15 skipped, 4 xfailed, 1 failed. The failure was the expected Niger 2018-19 total pin in `test_ehcvm_7day`, re-pinned in `06bac7429`. Every new served pin (12,227 revalued, 2,787 left NA, 12,227 function rows) passed.
  - final head `06bac7429` (burst job 41254830): **5,940 passed, 15 skipped, 4 xfailed, 0 failed** (125 files, 877 s).
  - The earlier, broader selection (every file naming Mali, Niger, Senegal, Uganda or `ehcvm`; 262 files) passed at `e12278688`: 10,070 passed, 190 skipped, 8 xfailed, 0 failed. Since then only Niger 2018-19 has changed.
- **New tests** in `tests/test_gh1707_served_corrections.py`:
  - served-table pins for Mali 2018-19 (157,856 rows; 2,276 rice-kg purchased rows carrying 23,581.7 kg; the block absent);
  - Niger pins: exactly 15,014 rows carry the flag, 12,227 of them the revaluation key, and the other 2,787 are NA;
  - the Niger registered function reproduces the served value;
  - the salt build reads s07b alone;
  - synthetic tests of the revaluation: the two floors (a region with 2 genuine reports is NA), the scope (a flagged row #876 serves NA is not revalued), the positional write, and a refusal test;
  - the Uganda per-household guard.
- **Re-pinned.**
  - `test_ehcvm_7day`'s Niger 2018-19 total: 53,351,991.10 → 46,636,756.04. It was 52,486,790.70 under the first scope; the two rulings withhold 2,872 more values. Its allowed-key set was also updated.
  - `test_gh1500`'s two Senegal 2021-22 source digests.

## For the maintainer
- **Niger 2018-19's served purchase total falls by 12.6%**, from 53.35M to 46.64M FCFA. The first scope's fall was 1.6%. All 6,715,235.05 FCFA of the fall is on flagged rows:
  - 6,254,048.04 (93.1%) is the #876 value of the 2,872 previously valued rows that are now NA;
  - 461,187.01 (6.9%) is the revaluation of the 9,355 that stay valued, from 7,636,353.96 to 7,175,166.95.

  5,659 of the 15,014 flagged rows are now served NA. Because an NA Expenditure is not served by `food_expenditures`, those 2,872 household × item cells leave that table. `CONTENTS.org` gives the fall as 6,715,235.06, the difference of the rounded totals.
- **The ruling's 3,787 estimate, reconciled.** That figure was taken over all 15,014 flagged rows under the first scope. In scope it is the 2,872 measured here. Out of scope it would be 915 of the 2,787: 577 NA at floor 1, plus 338 region rows on 1 or 2 reports. 2,872 + 915 = 3,787, but the 915 is derived from the first run's counts, not measured.
- **Not frame-equal outside the target rows.** Niger 2021-22's kilogram modes move through the pooled kg inference (GH #1487): 93 `food_quantities` (kgs) rows and 57 `food_prices` (kgvalue) rows. Its `food_acquired` is unchanged.
- **New dependency.** Niger 2018-19 `food_acquired` now depends on the cover file.
- **Non-integer `s07bq08`.** 162 genuine reports carry an `s07bq08` that is more than 1e-9 from an integer (an earlier count gave 167). That is the same "cannot have been entered" signal as the flagged class. Because they are unflagged, they price their cells.

## Issue drafts

**#1708: close.**
> Done (rulings @ligon 2026-11-12: "Flag now, diagnose", "Revalue at local median", and "GH1708 revaluation details": region floor of 3, only previously valued).
> - **Flagged.** 15,264 records whose `s07bq07a` is off the quarter grid become 15,014 purchased rows, keyed `Niger::food_acquired::computed-last-purchase-quantity`. They are rows where the questionnaire *enables* the field (15,263 of 15,264); their values were reconstructed from a producer reference unit value per product × unit × size × region. The disabled-but-filled records are a separate set that feeds no served value.
> - **Revalued.** The 12,227 flagged rows that #876 values now carry Expenditure = the purchased quantity × the median genuine local last-purchase unit value. The median is the cluster's when it has at least 3 reports, else the region's when it has at least 3, else the row is NA. These rows are keyed `Niger::food_acquired::flagged-purchase-local-median`: 1,107 at a cluster median, 8,248 at a region median and 2,872 NA. The 2,787 flagged rows #876 left NA stay NA.
> - **Effect.** 2018-19 purchase Expenditure goes from 53.35M to 46.64M FCFA, and the flagged rows' share from 26.0% to 15.4%. The producer's values remain in `derivation_inputs`.
> - **Details.** `Niger/_/CONTENTS.org`.

**#1707: close.** As in the earlier draft: the 3,357 no-`vague` records are withheld under `Mali::food_acquired::rice-ration-block-withheld`, and the served pin is in `tests/test_gh1707_served_corrections.py`.

**#1536: close.** 514 of 569 filled under `Uganda::sample::ea-same-household`; 55 remain NA (3.53% of the weight).

**#1519: close.** Quantity NA on the 3 rows, and Expenditure NA on the 2 valued ones by ruling. No `food_prices` value is above 1e10.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
