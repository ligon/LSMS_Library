
# Nigeria stream report (2026-11-13)

Worktree `/local/job41201840/worktrees/nigeria`, branch `feature/nigeria-tables-20261113`, base `f138a3d61`. **Head `2ebdad8b3`.** Every commit printed `SUCODER: published`.

## 1. Commits
| SHA | content |
|---|---|
| `97c733da3` | #1537: label-only `harmonize_food` row `Pepper` → Aggregate `Fresh Pepper`; test re-pin; CONTENTS |
| `1c906836a` | (subject says WIP) the #1502/#1518/#587 implementation in `nigeria.py`, the W2/W3 `food_acquired.py`, `crop_production.py`, `derivations.yml` |
| `e2c1a51d5` | `tests/test_gh1502_nigeria_food_conversion.py` (new) |
| `c821bba57` | per-round `inputs` for the two W2 keys; deletion-entry count; CONTENTS for #1502/#1518/#587/#1713 |
| `f2129b000` | re-pins in three tests that pinned the old PH W3 Kg/l drop; regex fix in my test |
| `2ebdad8b3` | docs only: what carries W2's rise; national-column comparisons labelled |

## 2. Idiosyncrasies (Nigeria)
- **Two rounds per wave dir, each with its own `t`.** The W2 tables are per round: PP is keyed on the planting unit code `s7bq2b`, PH on `nsu_cd`. W3 ships one table for both rounds. `crop_production` is post-harvest only.
- **Item codes collide between quarters (#1144).** Food rows are decoded with the round's own quarter labels. The W2 PP-vs-PH comparison excludes the 33 shared keys whose item meaning differs.
- **Unit codes collide across waves (#607).** So the tables are joined on native codes within the round, not on the served `u`. The unwired `units.py` scripts are not reused: the W2 one swaps PP/PH and ignores `g_cl`, the W3 one drops the Other key and keeps the first duplicate.
- **W3 puts the size in the unit code; W4 asks it separately (#1033).** The W4 `*_cvn` comparison goes through W3's sized labels.
- **W3 code 900 ("other specify") has per-source Other text (`*b_os`).** It is matched literally: 647 of 770 rows match.
- **#1035 litre decision.** Jerry cans stating litres are held on the crop side. Food volume rows are served as litres read as kg, as W4's `*_cvn` already are. That differs from #1035 and is listed for review.
- **`_canon_unit` is size-agnostic.** So the `ag_conv_w3` lookup uses the raw sized code at build time.
- **"A factor belongs to the quantity block it was asked beside" (#859).** `KgFactor` is untouched; the shipped crop table goes into `Quantity_kg`.
- **#843 sale keys.** 2,511 crop rows carry a sale key plus the new conversion key, joined with `+`.
- **C8 W3.** It is the one post-harvest wave with two price locations (`obs`), and its free-text size `c8q2b` is blank on every Kg/l row; 20 rows hold a lone ".".
- **Pepper is served unqualified by design (#1144).** The new row changes only the Aggregate, never `j`.
- **Post-planting education.** W1/W2 post-planting field a full education section; W3–W5 post-planting do not. The W3 PP questionnaire's contents say "2. EDUCATION … PH ONLY".
- **W2 PP table release mismatch.** The catalog lists 385 cases keyed `nsu_cd`; the held file has 417 keyed `s7bq2b`. Only the PH table's `g_cl` is documented.

**Contradictions of record (quoted, not resolved):**
- `data_info.yml` `crop_production.KgFactor` says a shipped table "is applied AT READ TIME instead, by `harvest_kg(shipped_factors=...)`, as its own `shipped` layer". `Quantity_kg` is "AS THE SURVEY WROTE IT DOWN". The #1518 ruling puts `ag_conv_w3` in `Quantity_kg`. I implemented it as ruled and recorded the contradiction; the canonical note is not edited.
- `c8-unrecorded-size-drop` says "13,767 g/cl rows", but its own inputs return 11,170 (14,603 = 11,170 + 3,433 before this change). Not reconciled.

## 3. Measurements
All runs were cold, in private roots, in co_carleton burst jobs. Aggregates are in `reports/nigeria/out/`, household-level frames in `private/nigeria/`.

| job | what | output |
|---|---|---|
| 41271653 | base build; Pepper trace; table schemas; coverage; references; base derived tables; `ag_conv_w3`; post-planting education; PDF fetch | `out/m1/` |
| 41271760 / 41271791 | #587 test; W2 fit by unit; W4 crop factors vs `ag_conv_w3`; PDF text | `out/m1/m1.json` |
| 41271865 | W3 post-harvest Kg/l size text | `out/m4/` |
| 41272041 | candidate `1c906836a` vs base, 9 tables plus derived tables and derivation inputs | `out/v1-1c906836a/` |
| 41272509 | W3 location pairs (for the test re-pin) | `out/m5/` |
| 41272664 / 41272682 | W2/W3 movement decomposition | `out/m6/`, `out/m7/` |
| 41272551 | final head: frames equal v1; per-round W2 inputs; Site Q base vs candidate | `out/v2-f2129b000/` |

**#1537.**
- `food_acquired`: 2010Q3 3,496, 2011Q1 3,496, 2012Q3 3,454 rows. All are code 76, and the counts per round and source equal the raw trace.
- `community_prices` (post-planting): 396 and 412 rows. The raw files hold 427 / 420 priced code-76 rows; the gap is the #587 holds.
- No other code decodes to Pepper. Code 2140 decodes to Pepper in the post-harvest quarter maps but occurs in no food or community file. `crop_production` serves no Pepper.
- After the change, Nigeria passes no label through on either table.

**#1502, measured before wiring.**
- **Coverage of non-metric rows:** 2012Q3 14,726/15,207; 2013Q1 18,044/18,235; 2015Q3 55,564/57,079; 2016Q1 51,287/52,383.
- **Duplicate served keys:** 0.
- **Reconciles with the 11-08 investigation:** 106,851 rows; inference/table median 0.5556; 164,939 vs 332,209 kg (W3 national column). The ruling's 106,152 / 164,823 / 330,793 come from an earlier, unprovided cohort.
- **W2 PP vs PH tables:** on 336 shared keys with the same item meaning, `conv` and `g_cl` are identical.
- **Against W4 `*_cvn` on the same served label:** W2 PP median 0.94, 69% of cells within 2x; W2 PH 0.99, 65%; W3 (national column) 1.000, 99%.
- **Against household price-implied kg (table / inference):** W2 PP 1.03 / 0.93, 82.6% / 85.9% within 2x; W2 PH 0.96 / 0.875, 79.1% / 82.2%; W3 0.81–0.885 / 0.46–0.50, 64–67% / 44–48%.
- **W2 passes the brief's gate:** 97–99% coverage, and most cells within 2x of W4.
- **Zone versus national (W3):** W4's `*_cvn` equals the own-zone value on 16,595/16,595 (2018Q3) and 13,476/13,489 (2019Q1) of the rows where the columns differ; it equals national on 83 / 45. I implemented own-zone. The kilogram difference is 0.08%.

**#1502 movement (`food_acquired` rows, keys, Quantity and Expenditure unchanged; 139,621 rows gain `Quantity_kg` and a key).**

| t | kg (`food_quantities` 'kgs') | `KgFactorSource` | `food_prices` median |
|---|---|---|---|
| 2010Q3 | 191,707 → 192,250 | unchanged; 3,679 factors move (median ×1.13) | 187.5 |
| 2011Q1 | 245,539 → 245,539 | unchanged | 200 |
| 2012Q3 | 189,161 → 260,124 (+37.5%) | item_unit 14,444 → 37, unit 763 → 444, survey_kg 0 → 14,726 | 250 |
| 2013Q1 | 147,523 → 169,675 (+15.0%) | item_unit 17,582 → 57, unit 653 → 134, survey_kg 0 → 18,044 | 266.7 |
| 2015Q3 | 119,919 → 206,722 (+72.4%) | item_unit 35,507 → 785, unit 21,572 → 728, none 14 → 16, survey_kg 0 → 55,564 | 400 → 260.9 |
| 2016Q1 | 108,068 → 189,117 (+75.0%) | item_unit 32,646 → 567, unit 19,737 → 529, survey_kg 0 → 51,287 | 400 → 272.1 |
| 2018Q3 | 251,251 → 251,489 | unchanged; 308 factors move (×2.53) | 390.6 → 389.6 |
| 2019Q1 | 214,080 → 214,135 | unchanged; 213 factors move (×1.93) | 372.1 → 370.4 |
| total | 1,467,249 → 1,729,051 (+17.8%) | | |

- **`food_prices`:** all 481,191 rows kept, key for key.
- **What carries the W2 rise:**
  - 2012Q3 is paint rubber +32,385 kg, mudu +25,509, tiya +11,443.
  - 42 rows of 100 or more units carry 47,795 of the 70,963 kg.
  - The largest cells are vegetables far above both references: Tomatoes / paint rubber 30 kg (inference 1.6, price-implied 2.5), Tomatoes / mudu 10 kg (1.0 price-implied), Onions / mudu 7.5 kg (1.5).
  - Grain cells sit near both references.
- **Spillover:**
  - W1-PP: 3,679 rows, mostly the `piece` label, whose pooled factor goes 0.109 → 0.124.
  - W4 rows with no `*_cvn`: 308 / 213 rows.
  - 2 W3 rows (on `medium bunch`) fall from `unit` to `none`.
- **`food_expenditures` and the native-unit modes** read no kg factor, so they cannot move. Not re-measured.

**#1518.**
- **Lookup:** 8,576 harvest rows are tabled clean. Held: 69 rows on the 9 identity-conflict table rows, and 71 jerry-can rows. W4's reported crop factor equals the own-zone value on 319 of 366 rows where the columns differ (national on 40).
- **After:** 8,436 served lines gain `Quantity_kg` (8.51M kg). 2016Q1 `none` goes from 12,144 to 3,708. Other waves and all reported columns are unchanged.
- **Site Q:** one new report, a 2016Q1 `Quantity_kg` low tail of 4 rows.

**#587.**
- **(a) W3 post-harvest blank-size Kg/l (2,597 / 836 rows):** against the household unit value, Kg median 1.000 (91.6% within 2x, 640 cells) and l 1.14 (85.0%, 213 cells). The controls (W3 post-planting 1.07 / 1.00; W4 post-harvest 1.00 / 1.00) behave the same way, so the rows are served. 2016Q1 rows go 23,153 → 26,586. The deletion-entry inputs go 14,603 → 11,170.
- **(b) Post-harvest blank units:** 0 rows in W3, W4 and W5. The code is aligned (a raw blank unit becomes `Unknown`); 0 served rows move.

**#1713.**
- There is no per-round declaration: `Wave.data_scheme` is the folder's `.py` stems plus its `data_info.yml` keys, shared by both rounds.
- W1/W2 post-planting `sect2` files hold 17,116 / 17,403 attainment answers.
- W3/W4/W5 post-planting do not ask education: the questionnaire text says "PH ONLY" (W3), and W4/W5 post-planting have no section 2. The post-harvest twins do print "SECTION 2: EDUCATION".
- The text came from a crude text-layer extraction of the PDFs. W2's PDFs did not extract.
- Verdict rows are drafted in `out/1713-draft-verdicts.csv`. With them, the W3–W5 cells would grade `correctly-empty`, and W1/W2 would stay `dropped` carrying their evidence. This is read from the grader code; it was not run.

## 4. Hash census (`f138a3d61` → `2ebdad8b3`, `out/t3-2ebdad8b3/hashdiff.json`)
| kind | moved / total | countries |
|---|---|---|
| `build_transforms_fingerprint` | **0 / 42** | – |
| `framework_imports_fingerprint` | 0 / 38 | – |
| `Country._table_cache_hash` | 24 / 555 | Nigeria only |
| `Wave._input_hash` | 240 / 2,321 | Nigeria only |

The same counts held at `e2c1a51d5` and `f2129b000`.

## 5. Tests
| run | command | result |
|---|---|---|
| main suite on `f2129b000`, warm private root | `pytest -n 12 --dist=loadfile` over `tests/test_*nigeria*.py`, `test_gh1537_*`, `test_quantity_kg`, `test_build_transform_hash`, `test_schema_consistency`, `test_derivations`, `test_community_prices_variety`, `test_categorical_mapping_resolution`, `test_962_unit_sentinel`, `test_u_code_leak`, `test_feature_attrs_carriers` | **1978 passed, 4 skipped, 0 failed** (base 1965 / 4) |
| first cold run on `e2c1a51d5` | same files | 8 failed, then fixed: three tests pinned the old post-harvest Kg drop (re-pinned with measured values and the ruling cited), plus my own regex |
| extra files naming Nigeria with food or crop terms (15 files) | `test_coverage_matrix`, `test_gh1460_*`, `test_gh1452_older_rows`, `test_feature_canonical_index`, and 10 more | cand 540 passed / 12 skipped; base 540 / 12 |
| final head `2ebdad8b3` | `test_derivations`, `test_gh1502…`, `test_schema_consistency`, `test_gh1537_partial_curation` | **350 passed** |

- `test_nigeria_w2_education_source` skips for a source-cache reason.
- No pinned number in `test_quantity_kg` belongs to Nigeria.

## 6. Issues and residuals
- **#1537 closes.**
- **#1502 closes** (W2 PP, W2 PH and W3 wired as ruled). Questions Q1–Q4 below.
- **#1518 partial.** Drafted residual, Tier B:
  > **Nigeria W1/W2 harvest kilograms: quantity-mode split before the agns cards.**
  > - W1/W2 `crop_production` serve kilograms only on metric units: 2011Q1 6,710 metric / 6,275 none; 2013Q1 4,484 / 8,464.
  > - Each wave ships its own crop card (`w1agnsconversion.csv`, `w2agnsconversion.csv`), unwired.
  > - #1035 showed a W1 card weight fits a fraction of a container (median 0.65) and not a count of them (0.032).
  > - Split the rows by quantity mode as #1035 did, and score each part against the household's sale price per kg, before wiring either card as `Quantity_kg`.
- **#587:** both post-harvest follow-ups are done; the issue stays open.
- **#1713:** not closed (Q7). Optional residual, Tier B:
  > **Nigeria W1/W2 post-planting education is unwired.**
  > - `sect2_plantingw1` has 23,321 members, with `s2q7` (highest level completed) on 17,116; `sect2_plantingw2` has 27,165, with `s2q8` on 17,403.
  > - The served 2011Q1 / 2013Q1 series are the post-harvest new-member blocks (643 / 313 rows).

## 7. Questions (the rulings don't answer these)
- **Q1 (W3 zone versus national).** I used own-zone, because W4's per-row factors reproduce it on both food and crops. The #1452 precedent chose national. Keep own-zone?
- **Q2 (volume rows).** W2 `g_cl = 0` rows (1,124 + 843) and W3 liquid containers are served as litres read as kg, as W4 does; W1's #1035 left litre containers to the inference. Keep them, or hold them?
- **Q3 (spillover).** W1-PP and W4 no-cvn rows move through the pooled estimate. Extending #1452's option B to Nigeria needs a `transformations.py` edit, which re-warms GhanaLSS, Uganda and Ethiopia. Leave it as is?
- **Q4 (W2 cells).** Some W2 cells sit far from both references: derica at about half (1,250 rows); vegetables at 4–20x (Tomatoes / paint rubber 30 kg, Tomatoes / mudu 10 kg, Onions / mudu 7.5 kg, Groundnuts / tiya 16 kg). Serve as tabled (done), or hold those cells?
- **Q5 (9 held crop rows).** W4's BUNDLE S/M/L rows carry exactly the held rows' factors (0.48 / 0.62 / 0.83, 165 rows), which suggests a table labelling slip. Release them under the card's reading?
- **Q6 (canonical note).** Edit the `data_info.yml` `KgFactor` / `Quantity_kg` notes to admit a shipped table in `Quantity_kg`?
- **Q7 (#1713).** Which do you want: (a) the drafted verdicts (`correctly-empty` for W3–W5, `empty-pending-machinery` for W1/W2); (b) wiring W1/W2 post-planting education; or both?

