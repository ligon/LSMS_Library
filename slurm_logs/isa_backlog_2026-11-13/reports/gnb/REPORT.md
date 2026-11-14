
## 0. Changes since c35c95eaf

| SHA | change |
|---|---|
| `92bf915ee2b90647a99a44c45eeef6dce43c74b7` | **2018-19 `plot_inputs`** (ruling "Fix all three now"): `Purchased` / `Quantity_purchased` / new `Unit_purchased` now read the purchase, `s16bq08` / `s16bq09a` / `s16bq09b`. The gift answers (`s16bq05` / `s16bq07a` / `s16bq07b`) are served as `Received_as_gift` / `Quantity_received_as_gift` / `Unit_received_as_gift` (Benin's and Togo's names), declared optional. |
| `a7e0cca6dedf88decff2857a94b1fbd58c0899e7` | Asset code 26 (`Cortador de relva…`, lawnmower) maps to `Appliances`, as the global `harmonize_assets` does (coordinator default; Benin uses `Other`). The weight comment is rewritten (F2): cross-sectional, "not a panel weight", Mali 2021-22 as the exact precedent, with Benin. |
| `e0a8f40b7b2a68ae15239776abfa3c7999006226` | **Roster** (ruling "Serve leavers"): the 341 departed members are served with `Departed` True. `Departed` is an optional bool in this country's `data_scheme.yml`, NA in 2018-19 (which does not ask). `MonthsSpent` is nullable `Int64`. The `kinship.yml` comment now says 6,186 `s01` rows, 6,139 of them members (F3). |
| `b62c4c4508f88df81f2a75d8972edb1f4baaff4d` | **2021-22 `food_acquired`** (ruling "Serve, registered key"), on the Togo/Benin/CIV template through `ehcvm.food_acquired_ehcvm`. Two new keys: `Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even` on every row (identity function; the inputs return the released answers) and `…::s07b-consumed-quantity-rounded-to-zero` (a deletion). `LastPurchaseUnitCode` maps codes 107/122/126 onto `s07bq03b`'s spelling. `harmonize_food` gains 59 rows. The 7-day key's `waves` gains 2021-22. |
| `62aced0b0e0009903ec38c48bb9d05fb92458070` | Tests (§4). |
| `6fe0d3d6ff6d8ec64bc2451ed3dace96d78acb16` | `CONTENTS.org`: the purchase fix with measurements, the 2021-22 section rewritten (food, leavers, weight, asset 26), and the F3 prose: the fit is distributional, the 445 vs 439 GPS grappes are explained, the displacement argument is labelled a hypothesis, and the nephew count is corrected. Same GPS correction in `sampling.yml`. |
| `c0e2353f6ec6a4a4e9b03aa5615ffb8d50e7d9ca` | `test_ehcvm_7day`: `Tabuleiro (8)` names its egg count, like `Tabuleiro (30)`. |
| `69f2d2fb33fae1ccca2d5f37571e11ab86e7e2d3` | `data_info.yml` comments: the CONTENTS pointer for food, and the 445/439 count. |
| `a495f0eeca703cf075befd2642b5735fcb079635` | `CONTENTS.org`: restores food evidence the rewrite had dropped (second reader, the s16b/s16c halves, Senegal's 6.66%, the rejected rules, the 91.3% arithmetic, `Inteiro`'s 26 rows, the two BID sentences). |
| `90bdd6742d59390a09189a81050d56ba32466367` | `CONTENTS.org`: the `StatedVsInferredKgWarning` that wiring this food brings. |

**Ruled, and done as the defaults say:**
- DAP stays raw.
- No relabel of seeds, so `seed_kg` is still vacuous.
- The 2021-22 GPS is served as recorded (#791).
- Strata are wired as confirmed.
- I stayed out of shocks; the coping stream (#1562) owns those hooks.

**Not done, by ruling:**
- **#1598, own-country NSU weights.** It needs `ehcvm.NSU_COUNTRIES` / `NSU_TABLES`. That edit was measured in a job-local clone (job 41275537): it moves 97 of 555 `_table_cache_hash` values (Burkina Faso 25, Mali 24, Niger 25, Senegal 23) and 194 of 2,321 `Wave._input_hash` values. Guinea-Bissau moves 0, and btf and fwimp move 0. Mali and Niger are outside the allowed set, so I stopped.
- **`Inteiro`.** It is not added to `transformations._UNIT_SIZE_WORDS`; that is residual R-I.

## 1. All commits (base `f138a3d61`)

Before the fix-up (unchanged from the c35c95eaf report): `23556bae6`, `bbf58ecc5`, `7ebac5aaf`, `ef14c9d89`, `a46d70866`, `3a7520ce3`, `acb3e8a44`, `180307740`, `fd6145425`, `5fcd64d4a`, `55e39b9c9`, `c35c95eaf`.

`5fcd64d4a`'s int64 `MonthsSpent` is superseded by `e0a8f40b7`: the column is now `Int64`, because leavers are blank. The fix-up commits are in §0.

## 2. Idiosyncrasies (what is new or changed since c35c95eaf)

**2018-19**
- `s16bq05` / `s16bq07a` are the gift question and `s16bq08` / `s16bq09a` the purchase; this is now fixed.

**2021-22 food**
- **The quantities are rounded.**
  - Every positive `s07bq03a` (128,229) and `s07bq07a` (96,262) is an even integer.
  - 3,945 consumed-yes rows carry q3 = 0.
  - 1,760 rows have q7a = 0 beside a positive `s07bq08`.
  - The quantities are stored as Stata `int8`.
  - The fit 2 × round-half-up(x/2) is distributional, not row-level: ceiling-to-even fits Unidade and Monte as well, and only the Kg and Litro zeros tell the two rules apart.
  - The rule cannot be inverted, and I found it documented nowhere.
- **The 23 rows where q3 = q4 + q5 keep odd q4/q5**, so 46 served quantities are odd: 23 produced rows and 23 in-kind rows.
- **Item codes:** 182 codes, renumbered from code 12.
  - 176 `j` labels are served, and 71 of them are not in 2018-19's served vocabulary; those 71 carry 35.4% of the wave's rows.
  - Sugar's two codes fold, as in 2018-19 and Benin.
- **Unit labels:** `s07bq07b` labels codes 107, 122 and 126 otherwise than `s07bq03b` does.
- **Size `Inteiro`:** 26 raw rows, served only as `Carcaça (Inteiro)`.
- **Egg trays:** `Tabuleiro (8)` and `Tabuleiro (30)` name their counts.
- **New warning:** wiring the wave brings a `StatedVsInferredKgWarning` (none on base): `saco (25 kg)` is inferred at 0.0125 kg against its stated 25 kg. The stated factor is served. I did not count which wave's rows carry the label.

**2021-22 roster**
- 341 leavers: 246 carry a sex and 95 do not.
- 12 leavers have an answered `s01q12`.
- 9,727 new members were not asked `s01q00aa` and are served with `Departed` False.

**2021-22 GPS**
- 445 grappes are in both files; 6 have a null 2018-19 point, which leaves 439 with both points, 2 of which coincide.
- The 2018-19 point is the community village/quarter point; the 2021-22 point is the household-GPS variable, constant within each grappe.
- That these are not two displacements of one point is a hypothesis.

**2021-22 weights**
- `hhweight` is cross-sectional:
  - positive on all 5,351 households, the 454 non-panel ones included;
  - equal to `ehcvm_welfare.hhweight` on 5,351 of 5,351;
  - varies within a grappe only with `panelHH`;
  - the only weight variable in the wave's 60 files.

Everything else is as in the c35c95eaf report and the red team's F5 to F10.

## 3. Measurements (cold, private roots)

**2018-19 slice, `62aced0b0` against base.**
- Jobs: verify 41275946, of which I cancelled the all-waves half; plot_inputs check 41276406; aggregates 41276605; extra 41275945.

| table | result |
|---|---|
| `plot_inputs` | 5,534 rows on both sides. With the Phase-A relabel applied, and the purchase columns and four new columns set aside, the frames are equal. Base's `Purchased` / `Quantity_purchased` equal head's `Received_as_gift` / `Quantity_received_as_gift` row for row. |
| `plot_inputs` purchase columns | `Purchased` True 423 → **1,231**. `Quantity_purchased` 423 → 1,231 values (sum 10,783 → 28,418.03). `Unit_purchased`: Quilograma 809, Outras 199, Litro 176, Grama 28, Carroça 15, Tonelada 4. Gift units: Quilograma 238, Outras 117, Litro 45, Grama 18, Carroça 4, Tonelada 1. |
| `Feature('plot_inputs')` | The same change, 5,534 rows. |
| `food_quantities` | 39,916 of 126,925 rows change value (the kilogram pool). The 2018-19 kg sum rises 226,907 → 236,031 (+4.0%). |
| `food_prices` | 22,573 of 77,155 rows change value; the sum falls 64.91M → 58.14M (−10.4%). |
| Kilogram ladder over both waves | `metric` 24,830, `item_unit` 87,772, `item_unit_tight` 7,495, `unit` 136,208, `none` 4. |
| `food_acquired`, `food_expenditures`, `community_prices` | Equal after the Phase-A relabel. |
| `household_roster` | Values equal. `Departed` is added (all NA) and `MonthsSpent` goes from int64 to Int64. |
| `crop_production` / `harvest_kg` | 12,173 → 12,188 and 730 → 731, as in Phase A. |
| `nitrogen_kg` | 0 → 188 households, **1,012.514 kg** in total (red team: 1,012.5). |
| everything else | Plain frame-equal: assets, cluster_features, food_security, housing, individual_education, interview_date, livestock, people_last7days, plot_features, plot_labor, sample, seed_kg, shocks, subjective_well_being. |

**2021-22 served values**

*Food (job 41275719, at `b62c4c450`)*

| measure | value |
|---|---|
| `food_acquired` rows | 129,384: purchased 95,094, produced 20,263, in kind 14,027 |
| rows carrying the rounding key | all 129,384, and 0 rows of 2018-19 |
| purchased rows carrying the 7-day key | all 95,094 |
| index | unique |
| purchased `Expenditure` | 60,522,116.51 FCFA, NA on 15.2% of purchased rows |
| raw positive `s07bq08` total | 101,787,610 |
| `derivation_inputs` of the rounding key | 128,229 rows, whose (i, j) pairs equal the served pairs exactly |
| deletion key's rows | 3,945, none of which meets a served row |
| `food_quantities` | 129,382 rows, 248,610 kg on `u = kg` |
| `food_prices` | 80,647 rows |

*Other tables (job 41275945, at `62aced0b0`)*

| measure | value |
|---|---|
| roster rows | 41,501, `Departed` True on 341 |
| `household_characteristics` rows | 5,348 |
| `household_characteristics` total | 50,240.5 → 50,250.7 (Q1) |
| `assets` | 29,713 rows; the raw Portuguese lawnmower label is gone |

**Warnings** (job 41276876, at `69f2d2fb3`, against base):
- `food_acquired` raises none.
- `food_quantities` and `food_prices` raise the one `StatedVsInferredKgWarning` above.
- `household_characteristics` raises none (10,696 rows across both waves).

**Hash census:**

| run | btf | fwimp | tables | waves |
|---|---|---|---|---|
| base → `62aced0b0` (job 41275947) | 0/42 | 0/38 | 22/555, all GNB | 44/2,321, all GNB |
| base → `69f2d2fb3` (job 41276512) | 0/42 | 0/38 | 22/555, all GNB | 44/2,321, all GNB |

The commits after `69f2d2fb3` touch only `CONTENTS.org`, which is not a build input.

**Not re-measured:**
- `scan.py` was not re-run.
- 2021-22 `sample`, `cluster_features` and `shocks` were not frame-compared against c35c95eaf. Since then their config has changed only in comments, and the test pins hold.
- No fresh-process warm reads, `assume_cache_fresh` or strict levers.

## 4. Tests

| run | SHA | result |
|---|---|---|
| 36-file list, job 41275948 | `62aced0b0` | 1 failed (`Tabuleiro (8)` was not in the size-test regex; fixed in `c0e2353f6`), 3,324 passed |
| 36-file list, job 41276339 | `c0e2353f6` | **3,325 passed**, 87 skipped (none Guinea-Bissau), 4 xfailed, **0 failed** |
| `tests/ -m "not requires_s3 and not slow"`, job 41276340 | `c0e2353f6` | **15,520 passed**, 35 skipped, 4 xfailed, **0 failed** |

The head differs from `c0e2353f6` by YAML comments and `CONTENTS.org` only.

**New or changed tests:**
- **`test_gnb_2021_wave.py`:**
  - the roster serves everyone and flags the departed;
  - leavers keep their recorded sex;
  - 2018-19 `Departed` is NA and `MonthsSpent` is Int64;
  - both rounding keys are registered;
  - the purchase-unit map;
  - every 2021 row is keyed and no 2018 row is;
  - served quantities are integers, with exactly 46 odd ones (23 produced, 23 in kind);
  - the zero deletion holds 3,945 rows;
  - the inputs join the served rows.
- **`test_ehcvm_7day.py`:**
  - Guinea-Bissau 2021-22 added (7-day scope, the rounding key beside it, BEFORE 101,787,610, AFTER (60,522,116.51, 14,027));
  - size `Inteiro`;
  - the `Tabuleiro (8)` regex.
- **`test_food_kg_size_pool.py`:** `SIZES_NOT_YET_IN_THE_VOCABULARY = {"Inteiro"}`, an exception with its reason.

## 5. Issues

| issue | status |
|---|---|
| #1697 | All six brief tables are wired (food served as released). It closes if #1697's scope is those six; otherwise R4 carries the rest. |
| #1107, #1539 | closes |
| #1119, #1226, #1118 | partial; Guinea-Bissau's half is done (as before) |
| #1565 | Guinea-Bissau serves its leavers flagged `Departed`; **Benin 2021-22 still drops its leavers** |
| #1598 | stopped: the footprint is outside the allowed set (§0) |
| #1556 | adopted |
| #1566 | measured |
| #791 | GPS served as recorded |
| #1710 | pre-existing duplicate asset index |
| R2, 2018-19 gift/purchase mix-up | **fixed**; no residual |

**Drafted residuals:**
- **R-I (tier B). "Add `Inteiro` to `transformations._UNIT_SIZE_WORDS`."** Guinea-Bissau 2021-22 has `Carcaça (Inteiro)`, 26 rows. Today the label is a unit of its own: it neither pools with bare `Carcaça` nor is size-pooled. The edit moves GhanaLSS, Uganda and Ethiopia through `framework_imports_fingerprint`, so bundle it with their next re-warm and remove `SIZES_NOT_YET_IN_THE_VOCABULARY` from `test_food_kg_size_pool.py`. Precedent: Benin's `Très grand`.
- **R-N (#1598 comment).** Wiring `ehcvm_nsu_gnb2021.dta` (4,419 rows; region × milieu × product × unit × size → `poids`; the BID: "collected in 2018 and updated in 2021") needs `NSU_TABLES`. Measured footprint: 97/555 tables and 194/2,321 waves in Burkina Faso, Mali, Niger and Senegal; 0 in Guinea-Bissau; btf/fwimp 0.
- **R-R (integration). Recall record for Guinea-Bissau 2021-22.** `recall.yml` is generated and lists only 2018-19. A `.coder/coverage/food_recall.csv` row is needed, then a regenerate:
  ```
  Guinea-Bissau,2021-22,"7B.03a. Qtité tot du [PRODUIT] conso par le MEN au cours des 7 derniers jours",7,1,7,,variable-label,"2021-22/Data/s07b_me_gnb2021.dta, variable label of s07bq03a",,"Quantities released rounded to even integers: Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even",
  ```
  The basis is `variable-label`, not `questionnaire`: the 2021-22 questionnaire PDFs are held but I did not transcribe them (no PDF library).
- **R-H (tier B). "Departed members can enter `household_characteristics`"** (Q1).
- **R1 (tier C, carried over and re-framed).** "Guinea-Bissau 2021-22 s07b quantities are rounded to even integers; seek an unrounded release (#791 route)." The data is now served under a key.
- **R3, R4, R5:** unchanged (R3 Tonelada/Litro have no mass; R4 the unwired 2021-22 tables; R5 the GPS under #791).

## 6. Questions

Q1 to Q5 are at the top of this message. One more:

6. Seeds: relabelling the seed rows `input = 'Seed'` so `seed_kg` reads them was deferred by the coordinator default; it stays open for later.

## 7. Must-states for integration

- 4,417 of 10,842 shocks are served.
- `household_characteristics` and the derived food tables (`food_quantities`, `food_prices`, `food_expenditures`) are now served automatically for 2021-22.
- `panel_weight` = `hhweight` is not longitudinal.
- Leavers are served with `Departed`, which is a proposal. Benin is asymmetric (#1565). A two-wave read serves `MonthsSpent` as Int64.
- 12 leavers with an answered `s01q12` move `household_characteristics`.
- Shocks: code 25 gets a constructed rank on 34 rows; `Aucune stratégie` is served as HowCoped0 on 961 rows.
- 14 owned asset lines are not served.
- The generic NPK share is applied; `Tonelada` is undeclared.
- The strata registry relies on a spelling fold.
- Nitrogen totals 1,012.5 kg.
- Food is served as released under the rounding key:
  - `Expenditure` inherits the rounding;
  - 3,945 zeroed rows are deleted (registered);
  - 46 served quantities are odd;
  - 71 of 176 served 2021-22 `j` labels are not in 2018-19's vocabulary.
- The kilogram pool moves 2018-19 (+4.0% kg, −10.4% on the `food_prices` sum).
- One new `StatedVsInferredKgWarning`.
- `Inteiro` is excluded by a test exception.
- Merge surfaces with other streams:
  - **`PARTIALLY_WIRED_SAMPLE` and the coverage regrade:** for Sue, at integration;
  - **recall CSV row:** R-R;
  - **gift column names:** they assume the Togo and Benin streams keep `Received_as_gift` / `Quantity_received_as_gift` / `Unit_received_as_gift`;
  - **shared test files:** `test_ehcvm_7day.py` (other EHCVM streams edit it) and `test_food_kg_size_pool.py`;
  - **`kinship.yml`:** a comment edit;
  - **CONTENTS shocks prose:** the #1562 stream may also edit it.

## 8. Disclosures

- **Two accidental `python3` no-ops on n0035** (from before the fix-up): `python3 -` on empty stdin, and `python3 -c 'print(1)'`. Neither imported anything. All other Python ran in burst jobs.
- **Two failed submissions, which touched no data:**
  - 41276400: a submit-line variable was missing;
  - 41276519: `burst-env` needs a full SHA.
- **I cancelled the all-waves half of verify 41275946** with `scancel`; its 2018-19 slice had finished.
- **Private material:** a new frames directory, `private/gnb/frames-41275946`, sits beside the earlier `frames-41271716`, `frames-41272011` and `frames-41272255`, plus `docs/`.
- **`out/` is not aggregates only:**
  - the discover JSON holds three interview timestamps and some count-1 free text;
  - the new files hold aggregates and unit labels only.
- **`scripts/compare_tables.py` was re-indented under a `__main__` guard after its last run as a script.** The 41275946 compare used the old file; the 41276406 check imported the new one.
- **Measurement SHAs:**

| what | SHA |
|---|---|
| food21 | `b62c4c450` |
| verify, extra, first census, first tests | `62aced0b0` |
| final tests | `c0e2353f6` |
| final census, warnings | `69f2d2fb3` |
| head | `90bdd6742` (CONTENTS only after `69f2d2fb3`) |

---

