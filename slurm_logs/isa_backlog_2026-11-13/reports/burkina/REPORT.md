
## Stream burkina: report (after the 2026-11-13 rulings and red team)

Worktree `/local/job41201840/worktrees/burkina`, branch `feature/burkina-bundle-20261113`, base `f138a3d61`. **Head: `d4d063fa11dfe582409e830c7b488f34fdae468d`.** Every commit printed `SUCODER: published`.

### Changes since 2e5b9dcd7 (for the delta review)

**`02c716c09ab57b68b38aa7c5a9ab45776891b02f`**: code, registry and tests.

- **Ruling "Extend Mali's rule".** A finished 2021-22 crop with no recorded quantity and all dispositions in one unit gets their sum as its harvest.
  - Keyed `harvest-from-dispositions`, through the shared `ehcvm.derive_hh_crop_harvest_total` (as Benin and Côte d'Ivoire), and registered.
  - Its `inputs` callable returns the four s16d quantities and units.
- **Ruling "Add production_obs".**
  - The declared index is now `(t, i, plot_id, crop, u, condition, production_obs)`.
  - 2021-22 s16c lines carry Niger's constructed `row:<source_fingerprint>:<n>` (#1360); 2018-19 and every Farm-level row carry `unresolved_observation`.
  - Both lines of each repeated pair are served.
  - The guard refuses a keyed row, or a quantity, in a group that repeats the plot-crop key.
- **Coordinator defaults.**
  - Codes 55/58/64 are served as Anacarde, Banane douce and Soja. The new registered key `crop-code-from-roster-text` has basis `derived-from-data`.
  - Code 65, which s16c leaves bare, takes s16d's own label `Autre (à préciser)`; there is no conflict once 55 is resolved. **This goes slightly beyond the default; please confirm.**
  - `Condition_sold` and `condition_sold_native` are wired from `s16dq05d` with 2018-19's #1528 fold.
  - `harvest_month` has no source in either file and stays NA.
  - Survey kg and sold kg are left to a residual.
- **Red team:**
  - **F1:** the code-identity assumption's basis is now `derived-from-data`, with counts.
  - **F3:** every unit field is decoded by code on one UML list and served through the read-path `u` table (`ehcvm.served_unit_labels`).
  - **F4:** the `harvest-to-date` entry states that those rows are partial harvests.
  - **F6:** `lost_fraction` is pinned at 0.19950 instead of `< 0.2`.

**`d4d063fa11dfe582409e830c7b488f34fdae468d`**: CONTENTS.org only.
- The 2021-22 crop section is rewritten from the new measurements.
- F1's prose is corrected: Niger is not a sibling with 55 Anacarde.
- F5's and F7's must-states are added to the #1442 and #1045 sections.

### 1. Commits

| SHA | content |
|---|---|
| `f710f848399f99f9ce4e3e10758c038594583d51` | #1118 `Unknown` input unit |
| `e205e099bd8422dc500d1d7106d119c4f5ee9768` | #1045 date specs |
| `adc10b44d32de7892759695f9d81949a58bfebb4` | 2021-22 `crop_production` driver |
| `df898c3e1d707759b423a9e773f825bef054d583` | #1442 (archived `d81358091`, reviewed) |
| `e52d10335216cac39c405228b886499cc2d6c854` | CONTENTS, `plot_id` dtype, pin |
| `346db3f06bd96377777dab65ad4a705260aaecde` | registry `inputs` at line grain, join test |
| `2e5b9dcd76100a0b48ee9f04dcc4cda22005c134` | CONTENTS seed-match cost |
| `02c716c09ab57b68b38aa7c5a9ab45776891b02f` | rulings and red-team fixes |
| `d4d063fa11dfe582409e830c7b488f34fdae468d` | CONTENTS after the rulings |

### 2. Idiosyncrasies (named; all now in CONTENTS.org)

- **EMC 2014.**
  - Four passages, numbered only in the filename.
  - C04, C05 and C06 are disjoint blocks, each with its own quantity, unit and value.
  - Only passage 1 has a quantity grid.
  - Equal money keyed into two blocks is counted twice (#864).
- **2021-22 is panel-only for agriculture.**
- **One crop list, two label sets.**
  - The code is the link between s16c and s16d: all 6,603 s16d crop records have an s16c line of the same code in the same household (5,080 by label). The 23 code-55 records sit with 55, the 24 code-65 records with 65.
  - s16c carries the labels of the list that ends at 55 `Autre` (Burkina 2018-19, Niger and Côte d'Ivoire 2021-22).
  - Its codes are those of the list that runs to 65, where 55 is cashew, 58 Banane douce, 64 Soja and 65 Autre (Mali, Togo, Benin, Senegal, Guinea-Bissau).
  - The roster text names anacarde or cajou on 54 of 59 code-55 lines, banane on 5 of 5 code-58 lines and soja on 88 of 92 code-64 lines. Précisez is filled on 38 of 38 code-65 lines and 0 of 59 code-55 lines. The code-55 households are in Cascades 26, Hauts-Bassins 17, Sud-Ouest 12 and Centre-Ouest 2.
- **Units.**
  - One UML list. Code 3 is spelled `Plat Yoruba` in s16c and `Yorouba` in s16d; 2018-19 has `Plat yorouba`.
  - Codes 8 and up are bare.
- **#1437.**
  - `s16cq12` through `s16cq16d` are blank on all 7,663 finished rows.
  - The 179 recorded quantities are all on unfinished rows, with a median 50% of the plot still to harvest (IQR 15–70).
- **Two repeated (household, field, parcel, crop) pairs** are distinct lines. 2018-19 has none.
- **2018-19 holds the harvest and all four dispositions on one row.**
  - That gives an in-instrument check of the dispositions rule.
  - On 12,490 of 14,956 finished rows (all dispositions in the harvest unit), the sum equals the harvest exactly on 87.9% and within 10% on 90.5%, median ratio 1.000.

### 3. Measurements

Cold builds in private roots, base `f138a3d61` against candidate `02c716c09`, job 41275751 (`out/measure-m3-*.json`, `out/compare-m3.json`).

**Evidence censuses (base):**

| job | what | output |
|---|---|---|
| 41275486 | code-55/58/64/65 text evidence, code identity, regions | `out/census_text.json` |
| 41275699 | 2018-19 in-row dispositions check, partial-harvest share | `out/census_disp2.json` |
| 41271946 | Mali's rule by code | `out/census_disp.json` |
| 41276125 | `Feature('crop_production')` 2018-19, base vs candidate | `out/feature2018-*.json` |

**What changes:** 23 of 33 calls are frame-equal. The rest:

| | base → cand |
|---|---|
| `crop_production` rows | 15,587 → 24,705 = 15,587 + 9,118 (2021-22). The 2021-22 rows are 8,476 s16c lines, every line kept with a `row:` token, plus 642 Farm-level rows. |
| 2021-22 sales | 1,548: 1,331 keyed on their single line, 217 Farm-level. `Value_sold` 270,464,270 FCFA = the raw `s16dq06` sum. `Condition_sold` on all 1,548 (shelled 858, unshelled 357, shell_not_applicable 333). |
| 2021-22 `Quantity` | 4,137 rows: 3,958 `harvest-from-dispositions` (3,421 single-line, 537 Farm-level), 179 `harvest-to-date` |
| units of the 3,958 totals | Sac gros 1,424, Plat Yoruba 1,231, Kg 530, Autres 315, Sac moyen 256, Tine 162, Unité 8, and 32 on bare UML codes |
| crop labels from the roster text | 159 rows (Anacarde 60, Banane douce 6, Soja 93); 0 source-qualified crop labels remain |
| unit vocabulary | `Unit_sold` is `Kg` (242) and `Plat Yoruba` (363), as `u` is; no `Kilogramme` |
| `harvest_kg` / `yield_kg`, 2021-22 | 586 / 506 rows (were 56 / 53); mostly `Kg` derived totals |
| 2018-19 crop rows | equal in every value; new `production_obs` = `unresolved_observation`; new `Derivation`, all NA |
| `Feature('crop_production')` 2018-19 | digest-equal to base (15,587 rows, same index) |
| grain | no report on `crop_production`. `LSMS_GRAIN_STRICT=1` passes on all-waves, 2021-22 and 2018-19 requests. |
| Site Q 2021-22 | 1 low `Quantity`, 2 high `Sale_price` |
| #1442 / food | as before: 113,538 new money-only rows; no existing `food_prices` row moves; 17 kg factors move; 2014 kg +0.03% |
| `interview_date`, `plot_inputs`, `nitrogen_kg`, `seed_kg` | frame-equal |

Warm equals cold in both arms, and no in-tree parquet was written.

### 4. Hash census

`f138a3d61` → `02c716c09`, job 41275752 (`out/hashdiff-h4.json`). The head commit `d4d063fa1` changes only CONTENTS.org, which is excluded from cache hashes.

| kind | moved / total |
|---|---|
| `build_transforms_fingerprint` | **0 / 42** |
| framework imports | **0 / 38** |
| `Country._table_cache_hash` | 25 / 555, all Burkina_Faso |
| `Wave._input_hash` | 75 / 2,321, all Burkina_Faso |

There were 0 errors and no other country moved.

### 5. Tests

Command: `pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -q -p no:cacheprovider -rfE`.

| run | head | scope | result |
|---|---|---|---|
| job 41275753 | `02c716c09` | 70 files: every test file mentioning Burkina plus those the brief names (`out/pytest-t4-files.txt`) | **3,411 passed, 0 failed, 10 skipped** |
| job 41275804 | `02c716c09` | whole suite, cold private root | 18,206 passed, 3 failed, 2,989 skipped, 8 xfailed, 1 xpassed |
| job 41276377 | `02c716c09` and base | the two non-Burkina failures, each in a fresh private root | candidate: 59 passed, 1 skipped; base: `test_gh932` fails |

**The three whole-suite failures:**
1. `test_table_structure::test_at_least_one_country_has_cached_data`: the root was empty and the run did not pass `--rebuild-caches`; it is environmental.
2. `test_sample::test_covers_all_waves[Ethiopia]`: passes in a fresh root on both base and candidate (job 41276377).
3. `test_gh932_safe_ci_failure_locations`: passes on the candidate and fails on base in that rerun.

So 2 and 3 are run-environment effects, not this branch.

### 6. Per issue

**#1436 (Burkina second residual): partial.**
- Built: the driver, `condition`, `production_obs`, `Condition_sold`.
- Draft residual, Tier B: *"Burkina 2021-22 crop_production: survey kg and sold kg."* `s16cq16c` is filled on 108 rows and `s16dq05c` on 1,537 sales; the rulings name Mali and Niger only.

**#1437 (Burkina half): partial.**
- Diagnosed; harvest served from the dispositions as ruled.
- Why `s16cq16a` is blank stays open (tier D).

**#824: partial.**
- 2021-22 sale wiring is done.
- 2014 (EMC `Y22A`–`Y23`) is not.

**#1045 (Burkina): done for Burkina.** The issue remains open corpus-wide.

**#1118 (Burkina): done for Burkina.** The umbrella remains open for the other countries.

**#1442: closes.**

**Draft residual, Tier C:** *"Burkina crop spellings across waves."*
- 2021-22 serves `Riz`, `Niébé/Haricot`, `Voandzou (pois de terre)` and `Laitue (salade)`, and the unit `Plat Yoruba`; 2018-19 serves `Riz Paddy`, `Niébé`, `Voandzou`, `Laitue` and `Plat yorouba`.
- Cost: 0 of 984 cowpea-seed rows and 0 of 291 rice-seed rows in 2021-22 `plot_inputs` match a harvest row. In 2018-19 the same check gives 2,097/2,156 and 525/532.

**Lead (red team, outside this PR):** Côte d'Ivoire 2021-22 codes 55–57.

### 7. Questions for the user

1. **F9:** confirm option (d) for Burkina 2021-22. Its authority is the batch brief, not a ruling that names Burkina.
2. Confirm code 65 served as s16d's `Autre (à préciser)`; this extends the 55/58/64 default.
3. Harmonise the 2021-22 crop and unit spellings to 2018-19's? See the residual above.
4. A `window:` for `interview_date`? Low value; 2018-19 would need two spans.

### 8. Must-state items (red team)

- **F4:** the 179 `harvest-to-date` rows are partial harvests (median 50% of the plot left); do not read them as yields.
- **F5:**
  - `basis='total'` 2014 now includes self-valued own production and gifts, while the EHCVM waves have none; it is not comparable across waves.
  - Equal money is double-counted: 1,090 / 353 served `(i, j, visit)`, and 1,074 / 350 raw source rows.
  - Passages 2–4 rest on variable labels only.
  - 45 money-only rows keep a recorded unit.
- **F7:** every `interview_date` read emits six `DatetimeParseWarning`s, and `LSMS_DATE_STRICT=1` refuses the table, where base passed.
- **F8:** a cold 2018-19-only `crop_production` request has no `Derivation` column.
- **F9:** see question 1.
- **F10:** resolved. `Condition_sold` is wired; `harvest_month` has no source.
- **F11:** Site Q flags on 2021-22, and `PopulationHeterogeneityWarning` from `Feature('crop_production')`.
- **F2:** resolved; the strict grain lever passes.

---

