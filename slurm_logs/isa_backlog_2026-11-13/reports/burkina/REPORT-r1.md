
## Stream burkina: report

Worktree `/local/job41201840/worktrees/burkina`, branch `feature/burkina-bundle-20261113`, base `f138a3d61c203f892148592675a7ef72e160a322`. Every commit printed `SUCODER: published`. Nothing was pushed to GitHub, and no issue was commented on.

### 1. Commits

| SHA | Item |
|---|---|
| `f710f848399f99f9ce4e3e10758c038594583d51` | #1118: a blank input unit is `Unknown`, in `2018-19/_/plot_inputs.py` and `_/s16b_2021.py` |
| `e205e099bd8422dc500d1d7106d119c4f5ee9768` | #1045: `interview_date` uses `datetime:` specs in 2018-19 and 2021-22 |
| `adc10b44d32de7892759695f9d81949a58bfebb4` | Q6 (#1436, #824): the 2021-22 `crop_production` driver and its registry entries |
| `df898c3e1d707759b423a9e773f825bef054d583` | #1442: archived `d81358091`, brought in and reviewed (conflicts in CONTENTS.org and one test) |
| `e52d10335216cac39c405228b886499cc2d6c854` | measured CONTENTS.org sections; `plot_id` written as `str`; `unknown` pin 178 → 208 |
| `346db3f06bd96377777dab65ad4a705260aaecde` | registry `inputs` keyed at the served line grain `(t, i, plot_id, crop)`, plus a join test |
| `2e5b9dcd76100a0b48ee9f04dcc4cda22005c134` | CONTENTS.org: the measured cost of serving s16c's crop spellings (documentation only) |

**Branch head: `2e5b9dcd7`.**

### 2. Idiosyncrasies named

I read `Burkina_Faso/_/CONTENTS.org` in full, including the WAITING entries for #1148 and #1226.

**Recorded there already, and relevant here:**
- EMC 2014 has four passages, and the passage number is only in the filename.
  - Section C has three disjoint blocks (C04, C05, C06), each with its own quantity, unit and value.
  - Only passage 1 has a quantity grid.
  - The #864 ruling serves each block as recorded, so money keyed into two blocks is counted twice.
- 2021-22 agriculture covers the panel component only (#1276).
- The 2021-22 `i` is `grappe + menage.zfill(3)` and is walked once to the 2018-19 id. 0 s16c households are off `sample()` after the walk.
- The printed route sends a finished harvest (Oui) to Q16C.16, yet `s16cq16a` is blank on finished rows.
- 2021-22 `s16cq16d` uses the same five codes as 2018-19's `s16cq12c`.
- Survey Solutions writes `##N/A##` in the cover timestamps.

**New, now recorded in CONTENTS.org:**
- **One crop list, two label sets.** `s16cq04` and `s16dq01` are roster ids on the same list.
  - s16c has 3 `Riz`, 8 `Niébé/Haricot`, 9 `Voandzou (pois de terre)`, 27 `Laitue (salade)` and 55 `Autre (à préciser)`; it leaves 58, 64 and 65 bare.
  - s16d has 3 `Riz Paddy`, 8 `Niébé`, 9 `Voandzou`, 27 `Laitue` and 65 `Autre`; it leaves 53–58 and 64 bare.
  - Every sibling EHCVM-2 file (Mali, Togo, Niger, Benin, Senegal) has 55 Anacarde, 58 Banane douce, 64 Soja and 65 Autre.
  - Matched by label, 332 of 1,548 sales find no harvest line; matched by code, 0 do.
- Unit codes above 7 are bare in both 2021 files. Code 3 is `Plat Yoruba` in s16c and `Yorouba` in s16d.
- **#1437:** `s16cq12` to `s16cq16d` are filled on 0 of 7,663 finished rows. On the 813 unfinished rows, `s16cq12` is filled on 813, `s16cq16a` on 179 and `s16cq16c` on 116.
- s16c repeats (household, field, parcel, crop) on 2 pairs of lines; neither pair records a harvest.
- `Manquant` is not a value label of any s16b unit variable in either wave.

### 3. Measurements

All Python ran in co_carleton burst jobs through `tools/burst-env.sh`, in private, initially empty data roots. Frames stayed node-local; only counts were written out. Scripts are in `scripts/`, logs in `logs/`, outputs in `out/`.

| job | what | output |
|---|---|---|
| 41271518 | base census: #1118 counts, the date stamps, s16c/s16d | `out/census_base.json` |
| 41271580 | crop and unit code→label maps in every EHCVM-2 file; the repeated keys | `out/census_crop2.json` |
| 41271946 | Mali's dispositions rule by code, as a measurement only | `out/census_disp.json` |
| 41271920 | cold build of every table, cand `df898c3e1` vs base, then `compare.py` | `out/measure-m1-*.json`, `out/compare-m1.json` |
| 41272395 | the same at `e52d10335` (the newer diff is callables, tests and docs) | `out/measure-m2-*.json`, `out/compare-m2.json` |
| 41272525 | seed-to-harvest match (#1119) | `out/seed_match-t3.json` |

**Cold base vs candidate:** 23 of 33 calls are frame-equal, in values and dtypes. Among them: `interview_date`, `plot_inputs`, `nitrogen_kg`, `seed_kg`, default `food_expenditures`, `sample`, `household_roster`, `cluster_features` and `shocks`.

| table | base → candidate |
|---|---|
| `crop_production` | 15,587 → 24,278 rows. +8,691 for 2021-22: 8,474 plot lines and 217 Farm-level rows. 2018-19 rows are equal, plus a new `Derivation` column that is NA on them. |
| 2021-22 sales | 1,548 rows carry a sale: 1,331 keyed on their single line, 217 Farm-level. `Value_sold` = 270,464,270 FCFA = the raw `s16dq06` sum. |
| 2021-22 harvest | 179 rows carry a `Quantity`, all keyed `harvest-to-date`. `harvest_kg` serves 56 rows (2018-19: 1,606); `yield_kg` serves 53 (2018-19: 1,485). |
| `food_acquired` 2014 | 559,180 → 672,718 rows (+113,538 money-only); 12,843 existing produced/inkind rows gain `Expenditure` |
| `food_expenditures(basis='total')` 2014 | 342,110,515 → 503,053,960 FCFA |
| `valuation='median_price'` | `reported` 683,735 → 810,115; 11,274 rows move from the price ladder to the survey's answer |
| `food_prices()`, `units='unitvalue'` | 234,524 → 247,336 rows; **0 existing rows move** |
| `food_quantities()` kg 2014 | 334,742 → 334,842 (+0.03%); other waves unchanged |
| kg factors | 17 rows move, all 2014 produced, all `unit` → `item_unit_nonpurchase`; 0 rows on a purchase rung move |

Against the survey's own unit weights, the 17 moved rows go from median absolute log error 1.87 with 0% within 2x to 0.77 with 35% within 2x. Feature equals Country for `crop_production`, `plot_inputs` and `interview_date`, with unique indexes. A fresh-process warm read equals the cold build in both arms. No in-tree parquet was written.

### 4. Hash census, base `f138a3d61` → head

Jobs 41271921, 41272396 and 41272524; outputs in `out/hashdiff-h{1,2,3}.json`. The three runs agree.

| kind | moved / total | by country |
|---|---|---|
| `build_transforms_fingerprint` | **0 / 42** | – |
| framework imports | **0 / 38** | – |
| `Country._table_cache_hash` | 25 / 555 | Burkina_Faso (all of its tables) |
| `Wave._input_hash` | 75 / 2,321 | Burkina_Faso |

No other country moves.

### 5. Tests

Command: `pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -p no:cacheprovider -rfE`.

| run | head | scope | result |
|---|---|---|---|
| job 41271924 | `df898c3e1` | 38 files | 2,260 passed, 1 failed (the `unknown` pin, since re-pinned), 4 skipped |
| job 41272525 | `346db3f06` | 70 files: every test file mentioning Burkina plus the files the brief names, including the Mali #1466 tests (`out/pytest-t3-files.txt`) | **3,403 passed, 0 failed, 10 skipped** |
| job 41272526 | `346db3f06` | whole suite, Makefile recipe, cold private root | 18,197 passed, **1 failed**, 2,992 skipped, 8 xfailed, 1 xpassed |

- **The whole-suite failure** is `test_table_structure::test_at_least_one_country_has_cached_data`. It fails because the private root was empty and the run did not pass `--rebuild-caches`. It is environmental, not this branch.
- **What the whole-suite run does not cover:** its 2,992 skips are mostly data tests for countries with no warm cache. Burkina's data tests ran in the warmed targeted run.
- **Superseded:** job 41272397 (tests at `e52d10335`) was cancelled once `346db3f06` replaced that head.

### 6. Per issue

**#1118 (Burkina site): closes the Burkina half.**
- 0 rows were moved, measured.
- **Still open in the umbrella:** Benin, Togo, Senegal, Côte d'Ivoire, Guinea-Bissau, GhanaLSS.

**Q6: #1436 (second residual), #824 (Burkina 2021-22 sale wiring): partial.**
- The driver, `condition` and `condition_native` are built.
- **Not built:**
  - survey kg (`s16cq16c`), sold kg (`s16dq05c`) and sold condition (`s16dq05d`);
  - harvest from dispositions;
  - #824's 2014 sale wiring.

Draft residual, Tier B: *"Burkina 2021-22 crop_production: survey kg, sold kg, sold condition."*
- `s16cq16c` is filled on 108 rows, `s16dq05c` on 1,537 sales and `s16dq05d` on 1,548 sales.
- The rulings name Mali and Niger only.

Draft residual, Tier C: *"Burkina 2021-22: one crop list, two label sets."*
- 55 is `Autre` in s16c and Anacarde in the EHCVM-2 list.
- 3, 8, 9 and 27 differ from the 2018-19 spellings.
- Codes 55, 58, 64 and 65 are served as source-qualified identities.

**#1437 (Burkina half): partial.** Diagnosed in §2; not imputed. The decision is question 1.

**#1045 (Burkina): partial for the issue, done for Burkina.**
- Frame-equal to base. 12 reports with 0 `unparsed`, `out_of_range` or `ambiguous`.
- Sentinels counted: 2018-19 3 / 1 / 11 / 1 / 11 across the five affected columns; 2021-22 18 in `int_end_v3`.
- 2014 declares no `interview_date`.
- **New behaviour:** a read now warns `DatetimeParseWarning`, and `LSMS_DATE_STRICT=1` refuses the table. This matches Ethiopia.

**#1442: closes.**
- Ruling, from the issue's "Resolved so far": `autocons` and `cadeau` "are served as `Expenditure` on `produced` and `inkind` rows, as direct survey answers with no derivation (the Uganda `value_own` / `value_inkind` precedent)".
- The 2026-11-12 #1509 ruling ("All purchase rows") "Unblocks the archived #1442 Burkina implementation (d81358091)".
- Re-measured as in §3.
- Disclosed: 1,090 `(i, j, visit)` carry the same money as purchase and own production, and 353 as purchase and gift. `basis='total'` counts each twice.

### 7. Questions for the user

1. **Harvest from dispositions for Burkina 2021-22?** Mali's rule, by code:
   - It would give a harvest to 3,958 of the 6,397 household-crops that are finished with no recorded quantity: 3,421 on a single line, 537 on a Farm-level row.
   - 2,272 span units and would stay NA; 167 record no disposition.
   - By unit: Sac gros 1,424, Yorouba 1,231, Kilogramme 530, Autres 315, Sac moyen 256, Tine 162, and 40 on bare codes.
   - Today `harvest_kg` serves 56 rows for 2021-22.
2. **Wire 2021-22 `Quantity_kg`, `Quantity_sold_kg` and `Condition_sold` for Burkina?** Coverage is 108 rows, 1,537 sales and 1,548 sales respectively.
3. **Crop labels.**
   - (a) Serve code 55 as Anacarde?
   - (b) Serve 2021-22's harvest crops in the 2018-19 spellings? Today, 0 of 984 cowpea-seed rows and 0 of 291 rice-seed rows in 2021-22 `plot_inputs` match a harvest row at `(t, i, crop)`. In 2018-19 the same check gives 2,097/2,156 and 525/532; the other 2021-22 crops match 97.8–100%.
4. **The 2 repeated s16c lines.**
   - Option A: keep the audited collapse. That is 1 `GrainCollapseWarning` (1 of 8,693 rows), one silent lossless de-dup, and `LSMS_GRAIN_STRICT=1` refuses `crop_production`.
   - Option B: add Niger's `production_obs` (#1360). That changes the declared 2018-19 index too.
5. **A `window:` for `interview_date`?** 2018-19 would need two fieldwork spans and the spec takes one. Served stamps run 2018-08-31 to 2019-07-28 and 2021-05-04 to 2022-09-01.

### 8. For the record

- **Two slips against "no Python on n0035"; neither processed survey data:**
  - a `python3 -c` that summarised the archived lane's report JSON;
  - a stray `python3 -` at the end of a heredoc append. It sat idle on stdin, was stopped within a minute, and read and ran nothing.
- **Shared generated files:** none were regenerated. `.coder/coverage/latest.csv` still shows base counts for Burkina `crop_production` and 2014 `food_acquired`; it needs `make matrix` at integration.
- **Coverage verdicts:** `absent_verdicts.csv` has no Burkina rows.

---
