VERDICT: pass-with-fixes -- the declaration is correct and every number reproduces cold, but the branch states in four places that Wave 5 "ships NO household geovariables file ... by source, not by omission", and the WB catalog's current release of this study (v02_M, changed 2026-09-16) distributes `NGA_HouseholdGeovariables_Y5.dta` (4,883 x 55); the repo holds v01. The report also declares "Residual issue: None needed", which is wrong given that file.

Reviewer: red-team, Slurm job 39922737, private `LSMS_DATA_DIR`, branch `fix/nigeria-w5-cluster` @ df3b211f6, base worktree at `origin/development` (c7377eca4; merge-base 2ed85b49b; development has not touched Nigeria since). Import assertion passed for both clones. GitNexus unavailable; call-site sweeps done with `rg`/`grep` (no library code is touched, so no symbol blast radius).

## 1. Refutations

1. **"W5 ships NO household geovariables file" (data_info.yml comment, CONTENTS.org heading, test docstring, commit body) -- REFUTED as a statement about the survey; holds only for this repo's acquisition.**
   Ran: `curl https://microdata.worldbank.org/index.php/api/catalog/NGA_2023_GHSP-W5_v02_M/data_files` and `.../catalog/6410?id_format=id`, diffed against `find Nigeria/2023-24/Data -name '*.dta.dvc'`.
   Got: catalog idno is now `NGA_2023_GHSP-W5_v02_M` (repo `SOURCE.org` records `v01_M`); version notes: *"Version 02: Includes two new datasets ('totcons_final.dta' and 'NGA_HouseholdGeovariables_Y5.dta') and updated documents and two updated datasets ('sect2b_harvestw5.dta' and 'sect3d_harvestw5.dta')"*. Catalog 108 files, repo 106; the two missing are exactly `nga_householdgeovariables_y5` (case_count 4,883, var_count 55, "Household geo variables") and `totcons_final`. The fix agent checked the repo tree (`find ... -iname '*geo*'`: none -- I reproduce that) and inferred a property of the instrument. The correct statement is "absent from the v01 extract this repo acquired; distributed since WB v02 (2026-09-16)". Declaring W5 without `df_geo` is still the right immediate step, because the file is not in DVC; the prose and the "by source" / "not by omission" framing are what is wrong, and they are the kind of unevidenced-absence claim CLAUDE.md's Albania example warns about.
2. **"Residual issue: None needed. The fix resolves #841 in full" -- REFUTED.** The lat/lon gap for 2023Q3/2024Q1 is an acquisition gap with a known source, and the branch leaves the Site B warning firing on every `cluster_features()` read. A residual must be drafted (draft below, §6).
3. **Cache cost: "every Nigeria L2-country table with a 2023-24 wave re-warms ... other waves' L2-wave parquets stay" -- understated; PROVEN against the branch's own parent.** Ran `Country('Nigeria')._table_cache_hash(t, c.waves)` for 8 tables under the merge-base (2ed85b49b, a worktree), under `origin/development` (c7377eca4) and under HEAD. Merge-base and development give byte-identical hashes for all 8 (so the 13 development commits do not touch Nigeria's hashes, and the pre-fix warm parquets match both). HEAD moves **all 8**, including `assets`, `shocks`, `food_acquired`, `plot_features`, which the W5 `data_info.yml` does not declare (it declares only `sample`, `cluster_features`, `individual_education`). The country hash folds every wave's `data_info.yml` regardless of declaration, so the cost is a **full Nigeria L2-country re-warm**. "No other country moves" -- held (no library code touched). I did not separately verify that W1-W4 L2-wave parquets are served rather than rebuilt; my cold build of the three tables took 61 s, so it is not a cost that changes the decision.
3b. **Cross-wave label consistency (not claimed; checked because `zone` arrives as `'3. North West'` and `District: state` has no mapping)** -- HELD. On the warm branch build, Region/District/Rural value sets for 2024Q1 vs 2019Q1 and vs all of W1-W4: 6/37/2 distinct in every wave, symmetric difference empty for all three columns. No `#811`-class label split.
4. Symptom on base -- HELD. Warm read of the pre-fix parquet under `origin/development`: 5,248 rows, t in 2010Q3..2019Q1 only; 2024Q1 community_prices 27,318 rows / 507 clusters, 0 join cluster_features.
5. Fix on branch, cold (`cache clear --country Nigeria` on my private root, then `cluster_features()`/`sample()`/`community_prices()`) -- HELD, every figure (table §2).
6. "zone/state/sector disagree in 0 clusters; Latitude/Longitude in 5/24/50; audit 70/78/129" -- HELD. Re-measured from the raw cover page + geovars via `get_dataframe`, excluding `ea==0`/`'0. Moved'`: zone/state/sector >1 value in 0/0/0 clusters each wave; lat-or-lon in 5/24/50 (lon alone 49 in W4); whole-row distinct tuples per cluster >1 in 69/78/129 (framework `grain_reports()` says 70/78/129; the W2 off-by-one is my moved-filter, not a finding). Note the audit's `destroyed`/`conflicting_groups` on W2-W4 are identical before and after.
7. "518 real + 393 movers = 911; 719 tuples; 201 (state,lga,0) groups; 404 ea codes; cover-page `cluster` 1:1 with composite on the 518; movers share {lga}-0" -- HELD from the raw `secta_plantingw5.dta` (5,067 x 35; no lat/lon column; cluster col 519 distinct = 518 real + 214 mover values that are NOT all one `{lga}-0` but one per LGA -- the CONTENTS wording "all movers share `{lga}-0`" means per-LGA, which is accurate but could be read as a single value).
8. New tests are meaningful -- HELD. Branch test file run against `origin/development` code in a fresh root: `4 failed, 30 passed` -- the two new W5 tests, `test_cluster_features_carries_both_rounds_and_they_agree[2023Q3-2024Q1]`, and `test_sample_and_cluster_features_share_one_keyspace` fail there. No assertion weakened: `test_every_wave_has_coordinates_in_every_round` keeps exactly the same 8 quarters (`GEO_QUARTERS == old CF_QUARTERS`).
9. "No GrainCollapseWarning for W5" -- HELD: `grain_reports()` has entries for 2012Q3..2019Q1 only; `null_index_reports()` empty; `null_read_reports()` = Latitude and Longitude for `['2023Q3','2024Q1']` (Site B, as stated).
10. No in-tree parquet, no writes outside the private root -- HELD (`find lsms_library/countries -name '*.parquet'` = 0 in both clones).
11. Pre-existing noise, NOT caused by the branch: on a cold Nigeria root the first read prints `make: *** No rule to make target 'panel_ids.json'` then falls back to wave aggregation (Nigeria is Design B, GH #894). Appears on base too; the branch touches no Makefile.

## 2. Claimed vs measured

| claim | claimed | measured (branch, cold) | base |
|---|---|---|---|
| cluster_features rows / quarters | 5,248 / 8 -> 7,070 / 10 | 7,070 / 10 | 5,248 / 8 |
| (t,v) 2023Q3 / 2024Q1 | 911 / 911 | 911 / 911 | absent |
| real / moved-{hhid} per round | 518 / 393 | 518 / 393 | -- |
| sample.v vs cf.v orphans, W5 | 0 both directions | 0 / 0 both rounds | 911 sample-only |
| community_prices 2024Q1 clusters / rows joining | 507/507, 27,318/27,318 | 507/507, 27,318/27,318 (Region after join 27,318) | 0/507, 0 |
| W5 nulls Region/District/Rural | 0 | 0 / 0 / 0 | -- |
| W5 nulls Latitude/Longitude | 1,822 / 1,822 | 1,822 / 1,822 | -- |
| W1-W4 (t,v) per quarter | 500/647/792/685 | 500/500/647/647/792/792/685/685 | same |
| W1-W4 lat notna per quarter | 500/645/784/671 | same | same |
| duplicate (t,v) | 0 | 0 | 0 |
| GrainCollapse for W5 | none | none | -- |
| W2/W3/W4 zone,state,sector disagreeing clusters | 0 | 0 | -- |
| W2/W3/W4 lat/lon disagreeing clusters | 5/24/50 | 5/24/50 | -- |
| audit conflicting tuples W2/W3/W4 | 70/78/129 | 70/78/129 (`grain_reports`) | same |
| 719 / 201 / 404 / 519 cover-page counts | as stated | 719 / 201 / 404 / 519 | -- |
| W5 geovars file exists | "ships NONE" | repo: none; **WB v02: `nga_householdgeovariables_y5`, 4,883 x 55** | -- |
| cache: tables that re-warm | "with a 2023-24 wave" | all 8 probed Nigeria tables incl. assets/shocks/food_acquired (merge-base == development; HEAD moves all) | -- |
| W5 Region/District/Rural labels vs W1-W4 | (not claimed) | 6/37/2 distinct, symmetric difference empty | -- |
| issue's 24,184 rows | 27,318 now | 27,318 | 27,318 |

## 3. Regression

Branch, recipe `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 pytest -n 16 --dist=loadfile`:
`tests/test_nigeria_cluster_identity.py tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py` -> `392 passed, 4 warnings in 400.79s (0:06:40)`.
Branch test file on `origin/development` code (fresh root): `4 failed, 30 passed in 155.13s` (the four named in §1.8).
No framework change, so no full suite / scan.py run (country-scope rule).

## 4. Scope findings
- `git diff --stat 2ed85b49b..HEAD`: exactly the three named files (data_info.yml +58/-6, CONTENTS.org +41/-4, test +26/-5). No baseline/golden/blessed/lockfile/dependency edit; `.coder/coverage/*` untouched (stop-list; `latest.csv` still grades the two cells `absent` until `make matrix`).
- Block copied from W4 verbatim except the file path and the removed `df_geo`; same `converted_categoricals`, same Rural mapping, same `final_index (t,i,v)`. `data_scheme.yml` not edited (already declares the table).
- The `Nigeria/_/data_scheme.yml` comment still says core reduces Latitude/Longitude with `.mean()` (removed 2026-07-13) -- pre-existing, correctly left alone, noted in the report.
- `SOURCE.org` records `v01_M`; catalog is `v02_M` -- pre-existing provenance drift, not introduced here, but it is what hid the geovars file.

## 5. Residual risks / what a human must decide
- **Follow-up #1 (the reword) is REQUIRED before merge, not optional.** The data and test changes are right, but the branch writes an unevidenced "no module here" claim into a YAML comment, CONTENTS.org and a test docstring -- exactly what CLAUDE.md's Albania rule forbids -- and it is already contradicted by the catalog. Merged as-is it would be cited to close the lat/lon cell as `not-asked`. Follow-ups #2-#4 can land in the same PR or immediately after.
- **The all-NaN W5 test** (`test_w5_has_geography_but_no_coordinates_in_either_round`) is a deliberate ratchet and fine as such, but its docstring's "if a W5 geovariables file is ever acquired" should say the file exists in WB v02 so the next person knows it is a queued acquisition, not a hypothetical.
- **Full Nigeria re-warm on merge** (all tables, not just W5-bearing ones), ~1 min for the three tables I built; the script-path tables (food_acquired etc.) will cost more. Not a blocker.
- Acquiring v02 is more than the geovars file: it also updates `sect2b_harvestw5.dta` and `sect3d_harvestw5.dta` (household_roster-adjacent) and adds `totcons_final.dta`; that is an `add-wave`-skill refresh with its own review.

## 6. Recommended follow-up commits (pass-with-fixes)
1. `lsms_library/countries/Nigeria/2023-24/_/data_info.yml` (cluster_features comment) and `lsms_library/countries/Nigeria/_/CONTENTS.org` (heading "Wave 5 ships NO household geovariables file" and body): replace "ships NO ... BY SOURCE, not by omission" with: the v01 extract this repo acquired (SOURCE.org `NGA_2023_GHSP-W5_v01_M`, 106 files) has no geovariables file; WB catalog 6410 v02 (changed 2026-09-16) adds `NGA_HouseholdGeovariables_Y5.dta` (4,883 households, 55 vars) and `totcons_final.dta` and updates `sect2b_harvestw5`/`sect3d_harvestw5`; Latitude/Longitude are NaN pending that acquisition. Keep the "do not add `optional: true`" sentence.
2. `tests/test_nigeria_cluster_identity.py`: same correction in the `GEO_ROUNDS` comment and the new test's docstring ("retire this test when the v02 geovars file is wired as `df_geo` on `hhid`, as W4 does").
3. Report/PR body: correct the cache-cost sentence to "every Nigeria L2-country table re-warms (the country hash folds every wave's data_info.yml, declared or not); measured: assets, shocks, food_acquired, plot_features, household_roster, sample, community_prices, cluster_features all moved."
4. **File the residual issue** (draft): *"Nigeria W5: acquire WB v02 (NGA_2023_GHSP-W5_v02_M) and wire `NGA_HouseholdGeovariables_Y5.dta` as cluster_features `df_geo`"* -- body: #841 declared cluster_features for 2023Q3/2024Q1 from the cover page only, so Latitude/Longitude are all-NaN there and Site B warns on every read; the repo's v01 extract (106 files) predates WB v02 (2026-09-16), which adds `NGA_HouseholdGeovariables_Y5.dta` (4,883 x 55) and `totcons_final.dta` and revises `sect2b_harvestw5.dta`, `sect3d_harvestw5.dta`. Work: `add_wave`/`populate_and_push` refresh of 2023-24 to v02 (update `SOURCE.org` idno), add `df_geo` keyed on `hhid` with `lat_dd_mod`/`lon_dd_mod` (verify names; W4 geovars joined on `hhid` because it has no `ea`), retire `test_w5_has_geography_but_no_coordinates_in_either_round` and move 2023Q3/2024Q1 into `GEO_QUARTERS`, re-measure the Site 2 lat/lon disagreement count for W5 (expect >0, as W2-W4), then `make matrix`. Acceptance: Site B silent for Nigeria cluster_features; lat notna > 0 in both W5 quarters; the two revised harvest files re-checked by whatever tables read them.
