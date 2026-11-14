This PR serves Malawi 2024-25 `sample` and `food_acquired`, and so the derived food tables. Kilograms are borrowed from the released IHS5 conversion factors (maintainer item 49), and item 119 is labelled as maize flour (item 50). It also adds 1,948 2024-25 panel links.

**Hold status.** The decision-48 / whole-wave-promotion hold was lifted on 2026-11-12 ("Lift hold, rebase, merge") **for this PR's content only**. Decision 48's joint-inclusion question is still unruled. The stacked-weight question it raises is now #1704 (ruling of 2026-11-12: "Merge, disclose, file issue"). No other wave's weights change.

Head: `538eec00d9eb354d0dd7f296660295a8488f61bd`. Base: development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

## What 2024-25 serves

**`sample`: 14,836 households.** 12,888 are IHS6 households (`cs-24-` ids) and 1,948 are retained IHPS households (walked to their canonical panel ids). The 815 IHPS refreshment households are the same interviews as IHS6 households, so each is served once, on its IHS6 row. The Panel copy is dropped, and the hook fails closed if a copy has no IHS6 row.

| column | IHS6 rows (12,888) | retained IHPS rows (1,948) | API (within-wave mean 1) |
|---|---|---|---|
| `weight` | `hh_wgt`, raw mean 383 | `wt_cross`, raw mean 1,258 | mean 1.00 (IHS6 0.77, retained 2.53) |
| `panel_weight` | missing | `wt_long`, raw mean 2,531 | mean 1.00 |
| `ihps_weight` | `wt_cross` on the 815 refreshment rows | `wt_cross` | **raw scale**, mean 1,784.6 over 2,763 rows |
| `ihps_hhid` | `y5_hhid` on the 815 | `y5_hhid` | 2,763 rows |

`ihps_weight` is **not** normalised; only `weight` and `panel_weight` are. It is there to rebuild the IHPS 2024 cross-section (`s[s.ihps_weight.notna()]`) as a ratio weight within its own rows. `data_scheme.yml` and `CONTENTS.org` now say so (red team F4).

**`food_acquired`: 331,677 rows over the same 14,836 households.**

| | rows | households | borrowed `Quantity_kg` (stamped) | purchased / produced / in-kind |
|---|---|---|---|---|
| IHS6 (`cs-24-`) | 283,610 | 12,888 | 149,199 | 209,596 / 36,842 / 37,172 |
| retained IHPS | 48,067 | 1,948 | 24,827 | 38,590 / 4,213 / 5,264 |
| **total** | **331,677** | **14,836** | **174,026** | |

Every stamped row carries `Malawi::food_acquired::borrow-ihs5-factor`. `Country.derivation_inputs` returns 174,026 input rows: the key join with the stamped rows is a bijection (0 / 0), and Σ native quantity × `ihs5.factor` reproduces all 174,026 served values (rtol 1e-9). Refused or unsupported keys keep `Quantity_kg` missing and fall to the existing kg ladder.

## Stacked weights (#1704)

2016-17, 2019-20 and 2024-25 each stack two samples' national-scale weights in one wave column: the cross-section's and the retained panel's. After normalisation, the retained panel's share is:

| wave | panel share of rows | panel share of weight |
|---|---|---|
| 2016-17 | 17% (2,508 / 14,955) | 59% |
| 2019-20 | 22% (3,178 / 14,612) | 63% |
| 2024-25 | 13% (1,948 / 14,836) | 33% |

This PR serves 2024-25 the same way as the two earlier waves. One rule for all three belongs to #1704.

## Unit code 26 buckets now borrow (red team F1)

`5 Litre Bucket(Chigoba)` (unit code 26) parses as 5 litres. The shared metric choke point (`malawi.py`) used to rewrite every such row to `u='kg'` at 5 kg per bucket. IHS5 released per-item factors for this unit (median 3.88 kg, range 2.29–5.33), and 2019-20 already serves it natively with them.

A 5-litre bucket states a capacity, not a mass, so `food_borrowing.py` now admits code 26. Every other metric-labelled code with an admitted factor states a mass (`Satchet/Tube (25G)`, `Loaf (600G)`, `Packet (1Kg)`, …) and stays at the choke point. The completeness check and `derivation_inputs` now send each contributor to the key the served frame gives it. Without that, an unmatched bucket landing on `u='kg'` could block a matched bucket's kilograms.

- Of 5,814 active code-26 contributions, **4,031 borrow**: 5,950 buckets carry 22,563 kg, where density 1 gave 29,751 kg.
- The other **1,783 have no admitted factor**. They still reach the shared choke point and are served `u='kg'` at 5 kg. That choke point applies the same rule in every wave; how many 2019-20 code-26 rows reach it was not measured. Keeping these rows native would mean editing `malawi.py`'s choke point, which every wave uses, so it is left for a separate decision.
- **Old head `7735489cc` → this head, 2024-25 `food_acquired`:** exactly 4,031 rows change, each moving from `u='kg'` to `u='5 Litre Bucket(Chigoba)'`, with 0 value changes elsewhere.
  - Stamped rows rise from 169,995 to 174,026.
  - Served `u='kg'` rows fall from 5,972 to 1,941.
- **2024-25 `food_quantities(units='kgs')`:** 713,917 → 706,492 kg (−1.04%).

## Older waves

- **Frame-equal to development on every pre-2024 wave:** `sample` (the new `ihps_*` columns are empty there), `household_roster`, `food_acquired` (`Derivation` is empty there) and `food_expenditures`.
- **Derived kilograms move** because adding a wave extends the cross-wave kg-inference pool (GH #1487). The delivered factor is one number per `(j, u)` estimated over every wave, so 2024-25's evidence moves some older rows from the pooled `unit` rung to the item rung. For example, the verifier measured 2019-20 `unit` rows going 4,794 → 3,130 and `item_unit` 22,933 → 24,598.

| wave | `food_quantities(kgs)` total kg, development → this head | change | rows changed | `food_prices()` rows changed | `food_prices()` sum |
|---|---|---|---|---|---|
| 2004-05 | 605,265 → 613,698 | +1.39% | 32,387 | 19,068 | −0.28% |
| 2010-11 | 607,929 → 608,460 | +0.09% | 4,434 (+12 moved key) | 2,309 | −0.26% |
| 2013-14 | 235,130 → 236,108 | +0.42% | 8,219 | 6,370 | −0.98% |
| 2016-17 | 534,552 → 535,656 | +0.21% | 19,840 (+16 moved key) | 15,055 | −0.37% |
| 2019-20 | 584,955 → 585,391 | +0.07% | 15,380 (+1 moved key) | 12,264 | −0.73% |

So older-wave totals move **+0.07% to +1.39%** (the reviewed head measured +0.08% to +1.40%). The F1 fix accounts for −0.001% to −0.010% of that movement; the rest was already present at the reviewed head. No external accuracy improvement is claimed.

## 2024-25 label choices (`categorical_mapping.org`, `harmonize_food` 2024-25 column)

All 148 existing rows keep every older-wave cell. Ten rows are new; none of them has an older-wave entry.

- **Item 119 `Grain Meal (Gramill)`** (5,324 rows). The Preferred Label is kept, GD Category `Maize`, own aggregate / short label `Gramil` (item 50). This establishes no refined / mgaiwa / madeya subtype and no IHS5 crosswalk: IHS5's 119 is yellow maize flour, and its factor is refused. `Gramil` has no entry in `aggregate_short.yml`, so rerunning `util/orgtbl` cannot regenerate that cell.
- **Unit 74.** IHS5 labels 74 "TIN 1 KG"; the 2024 instrument labels it "BUNDLE". 2024-25 rows are served by their 2024 label, `Bundle` (1,414 rows), and the IHS5 factor is refused.
- **Unit 26.** Borrows, as described above.
- **Broadened items mapped onto existing Preferred Labels.** Borrowing refuses both, because the dictionary labels differ, while the `j` label asserts continuity:
  - 112 "Buns, scones, muffins" → `Buns, Scones` (2,895 rows);
  - 509 "Other poultry - guinea fowl, doves, quails etc" → `Other Poultry - Guinea Fowl, Doves, Etc.` (180 rows).
- **Item 120 `Popcorn`** maps by label to the existing `Popcorn` row (1,566 rows). Its factor is refused: IHS5 had no code 120 (its Popcorn was 835).
- **Ten new Preferred Labels**, with their GD / aggregate classes:
  - `Grain Meal (Gramill)` Maize / Gramil;
  - `Soya Pieces` and `Bean, Mixed`: Legumes / Beans+;
  - `Eggplant` and `Carrots`: Vegetables / Cabbage;
  - `Offal` and `Processed Meat`: Meat / Red Meat;
  - `Peaches` and `Water Melon`: Fruits / Local Fruit;
  - `Energy Drinks`: Beverage / Soda.

  Only item 119 is ruled.
- **Unmapped.** 2024-25 `Other (specify)` has no Preferred Label (`---`), so its 927 rows pass through with the raw label. Fifteen existing Preferred Labels have no 2024-25 entry.

## `check_panel_consistency('Malawi')` now fails one check

- `panel_ids_targets_exist` reports "1948/7634 current-wave targets MISSING (26%)".
- The 1,948 2024-25 links have no `household_roster` row, because `household_roster` is not wired for 2024-25. The households exist in `sample`.
- `attrition_monotonic` also warns "(2019-20,2024-25): zero overlap", for the same reason.
- Development: 7 pass, 1 warn. This head: 5 pass, 2 warn, 1 fail.
- No test or workflow gates on it. Wiring the 2024-25 roster clears both. This is recorded in `CONTENTS.org` (red team F2).

## Panel ids

- `panel_ids` grows from 5,686 to 7,634 entries, and no existing entry changes. `make panel-ids` regenerates both JSONs byte-identically: sha256 `01f98fd6…` and `173b1be8…`.
- From 2024-25, a minted `_N` split id also avoids every id an earlier wave issued (`Panel IDs: guard_minted_from`). This moves 33 of the 2024-25 ids, so 25 re-mints of departed lineages' ids do not happen.
- 2019-20's own 10 such re-mints are left for a separate issue.

**The `country.py` hunk is dropped (red team F3).**
- Both backends serve the committed JSON; the guarded Country branch was measured at 0 calls. Yet any edit to `country.py` moved every build fingerprint in the corpus.
- `country.py` is now byte-identical to development. `local_tools.panel_ids_guarding` / `update_id_reserving` stay, because `Malawi/_/panel_ids.py` uses them to write the committed JSON.
- **Residual:** the Country DataFrame fallback ignores `guard_minted_from`. It is reachable only if the committed JSON is absent. A test pins it and `CONTENTS.org` records it.
- **Tests removed or adjusted**, each with a one-line reason in commit `b13327088`: two `Country`-level tests in `test_malawi_panel_id_guard.py` removed, one adjusted to pin the residual, one kept (committed-JSON branch); two `test_gh970` assertions removed.

## Cache-hash footprint (after F3)

Measured with `build_transforms_fingerprint`, `Country._table_cache_hash` and `Wave._input_hash` over all 38 countries with a config, development vs this head:

| | moved |
|---|---|
| `build_transforms_fingerprint` | **0 / 41** |
| `Country._table_cache_hash` | **25 / 544** (Malawi 25/25; other 37 countries 0/519) |
| `Wave._input_hash` | **150 / 2,258** (Malawi 150/150; others 0/2,108; 30 are `None` on both sides) |

- Positive control: the reviewed head `7735489cc` moved 41/41, 544/544 and 2,228/2,258.
- The code head `1ca84789a` and this head are identical (0/0/0), since `CONTENTS.org` and `derivations.yml` are not build inputs.
- **Re-warm owed after merge: Malawi only.** None was performed here.

## Make

The 2024-25 `food_acquired` wave parquet now depends on `2024-25/_/food_borrowing.py` and `borrow_dictionaries.json` as well as its script (red team F5). A new `test_gh1237` case asks make itself (`-n -W <helper>`).

## Validation

Each library invocation ran under private data roots (`LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR`, `LSMS_ISSUES_LOG`), with BLAS threads pinned and `LSMS_BUILD_WORKERS=1`.
- The build, probe and hash scripts asserted the import path and the private `data_root()`.
- Pytest and `make panel-ids` inherited the same environment without asserting it themselves. The `make` log prints the import path, which was the job's own clone.
- Job 41240388's pytest ran on the new arm's root, warm from that arm's cold build at the same SHA.
- The frame and hash comparison scripts import no `lsms_library`.

None ran the `dvc` CLI. Every clone ended with 0 in-tree parquets and a clean `git status`.

- **Job 41240388** (`savio4_htc`, code head `1ca84789a`):
  - The hash probe above, over three arms.
  - Cold builds of development, `7735489cc` and `1ca84789a`: `sample`, `household_roster`, `food_acquired`, `food_quantities(kgs)`, `food_prices`, `food_expenditures`, and `check_panel_consistency`.
  - Derivation-join and rederivation probe (4,031 / 4,031 on the bucket rows alone), code-26 scope probe, sample and weight components, stacked shares.
  - `make panel-ids` byte-identical.
  - Targeted pytest (`tests/test_*malawi*` (37 files), `test_panel_ids_persistence`, `test_id_walk`, `test_gh970*`, `test_recall*`, `test_build_transform_hash`, `test_derivations`, `test_no_build_input_in_contents_org`, `test_gh1578_derivation_duplicates`, `test_notes`, `test_gh1190_selected_farm_carrier`, `test_gh1210_invalid_food_waves`, `test_failure_is_not_absence`, `test_schema_consistency`, `test_categorical_mapping_resolution`): **2,503 passed, 1 skipped** (the `LSMS_EXPECTED_ROOT` identity check), 0 failed.
  - `test_sample.py -k Malawi`: **16 passed**.
- **Job 41242295** (`50113e8ac`, the CONTENTS commit): hash re-probe (identical to the code head), `make panel-ids` byte-identical, and the served-label check above. Doc/drift, panel and borrowing selection (`test_notes`, `test_no_build_input_in_contents_org`, `test_derivations`, `test_gh1578_derivation_duplicates`, `test_malawi_food_borrowing`, `test_malawi_panel_id_guard`, `test_gh1237_malawi_make_prereqs`, `test_build_transform_hash`, `test_panel_ids_persistence`, `test_recall_record`, `test_recall_period`, `test_gh970_no_json_make`, `test_gh1190_selected_farm_carrier`, `test_schema_consistency`): **841 passed**, 0 failed.
- **Job 41242500** (this head `538eec00d`, after the wording fix and the `derivations.yml` `validated_against` update): hash re-probe identical to the code head (0 / 41, 0 / 544, 0 / 2,258 moved; development vs this head 0 / 41, 25 / 544 Malawi only, 150 / 2,258 Malawi only), `make panel-ids` byte-identical, and the same 14-file selection: **841 passed**, 0 failed.
- **Earlier, on `7735489cc`:**
  - Red team: jobs 41237339, 41237351 and 41238038.
  - Re-verification: 41232294, 41232295 and 41234994.
  - Neither found a defect in panel ids, the 2024-25 sample, the food household set or the derivation stamp. F1–F5 above are their findings.
- **Not run on this head:** the full offline suite, `pr-data-validation`, and a Malawi re-warm. No external accuracy check of the borrowed IHS5 factors exists.

Addresses #1275 (not Closes).

<details>
<summary>Draft narrowing of #1275 to its residual</summary>

**Delivered by #1604:** 2024-25 registration of `sample`, `panel_ids` (1,948 links, committed JSON) and `food_acquired` with registered IHS5 factor borrowing, plus the derived food tables. Acquisition of 8507 / 8520 was done earlier (CONTENTS 2026-11-04).

**Still open here:**
1. The remaining 2024-25 tables. Start with `household_roster`, which also clears the new `check_panel_consistency` `panel_ids_targets_exist` FAIL and the `attrition_monotonic` warn. Then `cluster_features` (no geovariables in either entry; compare #1260), the agriculture tables (`crop_production`, `plot_*`), `livestock`, `shocks`, and the rest.
2. The conditional #1174 follow-up: when 2024-25 plot inputs are wired, add their excluded-line deletion provenance and raw-input replay coverage.
3. Confirm `discover_waves('Malawi')` reports 8507 and 8520 as held. Not checked in #1604.

**Tracked elsewhere:** the stacked-weight / decision-48 question for 2016-17, 2019-20 and 2024-25 is #1704; 2019-20's 10 re-minted split ids are a separate issue.

**Known and disclosed, not open here:**
- 1,783 code-26 bucket contributions with no admitted IHS5 factor are served `u='kg'` at 5 kg, through the shared choke point.
- `Gramil` has no `aggregate_short.yml` entry.
</details>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
