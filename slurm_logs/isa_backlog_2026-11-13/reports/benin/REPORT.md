
# Stream benin: Benin becomes a panel country (#1705); 2018-19 plot_inputs fixes (#1118, gift/purchase)

- **Branch:** `feature/benin-panel-1705-20261113`, worktree `/local/job41201840/worktrees/benin`, base `f138a3d61c203f892148592675a7ef72e160a322`.
- **Head:** `2aa1e428736450e5190992019c1b5eaa0b71b5ba`. Every commit printed `SUCODER: published`.

### Changes since `d5fdf326c`

**Red-team fixes**
- **F1, the Makefile does not change failure behaviour.** The base already raised `CalledProcessError`, from the direct script run. Corrected in `Benin/_/Makefile`, `CONTENTS.org`, the PR body and the old question 3, which is withdrawn. What does change:
  - the "Makefile not found" warning is gone;
  - the error names the make target instead of the script;
  - wave scripts run in the stage-1 builder, which runs in parallel when `LSMS_BUILD_WORKERS` > 1;
  - `_probe_country_builder` calls `make -n`.

  `CONTENTS.org` lists all four; the Makefile comment lists the first two. `AGENTS.md`'s "Still open" paragraph is marked stale for Benin, and its count is corrected from 14 to 13 countries with wave scripts.
- **F2, unit spelling.** The 2018-19 `Unit_purchased` and `Unit_received_as_gift` columns are now decoded with `ehcvm.served_unit_labels('Benin', …)`, as 2021-22 does.
  - `'Kilogramme'` becomes `'Kg'` on 2,363 and 272 rows.
  - Every other label already matched 2021-22's spelling.
- **F3, disclosure.** The 63 households (the red team's 8.1% against 10.9% roster match) and the 19 with no pre-loaded roster (24/64 against 8/64) are in `CONTENTS.org` and the PR. The first ruling below then decided the 63.
- **F5.** The three purchase quantities that look like FCFA amounts are recorded and served as recorded. Two more lines of the same shape are listed as unconfirmed.
- **F6.** The verification scripts are copied to `slurm_logs/isa_backlog_2026-11-13/1705-benin-panel/` with a README. The CONTENTS citations point there.
- **F7, tests.**
  - `panel_ids.py` is now functions (`link`, `check_single_pass`, `all_preloaded_left`, `write`), with its data reads under `__main__`.
  - The tests run the generator itself: on the committed JSON, on each hazard built by hand (each matched by its own message), on invented covers, and on the real files.
  - The head-sex floor is now 0.90 with its reason stated, plus a margin of 0.2 over the control.

**Ruling "Unlink the 63"**
- A household flagged Panel whose whole pre-loaded 2018-19 roster is recorded as left (`s01q00aa` = 2 on every pre-loaded member) is not linked and keeps its `E2_` id.
- Registered as `Benin::panel_ids::all-preloaded-left-not-linked` (`rows: 0`; table slot `panel_ids`; function `panel_ids.py:all_preloaded_left`; the inputs callable returns the 63 households' `s01` rows and cover flags).
- `__main__` asserts the counts on this release (`EXPECTED`) and keeps both hazard assertions.
- The JSON was regenerated with `make panel-ids` (job 41279824): 6,815 links.

**Ruling "NA for unlinked"**
- `2021-22/_/mapping.py:sample` sets `panel_weight` to `poids` on the linked households and NA on the 1,217 others.
- It is disclosed as a cross-sectional weight in `mapping.py`, `data_info.yml`, `sampling.yml` and `CONTENTS.org`.

### 1. Commits

- `c1b428a1ee981de79e31e569ed515aee2bb6406d` 2018-19 plot_inputs reads the purchase (s16bq08 / s16bq09a / s16bq09b) and serves the gift in its own columns; the blank unit becomes `Unknown`.
- `ec270d4d7518b1adc3224cd7096854dcf8835309` `panel_ids.py`, and Benin's first `_/Makefile`.
- `547f0ab8698819cc1cb1b20dc2d5b4f6442a398c` the first committed crosswalk (6,878 links), the `panel_ids` declaration, tests and docs.
- `2cdcca347525d989a8f8fa09487293bfe1689b98` `CONTENTS.org`; a test fix.
- `d5fdf326cd2bcec359e3c1a89e63c732cb6e0815` member-number evidence with its control.
- `41073b3641b71c3afd49da1dfb22123f9a982dd8` F1, F2 and F7.
- `efea3af54413d214c08535c939b7c0cae97d20ae` both rulings, the F3/F5/F6 disclosures, and the tests. **Not bisect-safe:** the generator says 6,815 while the committed JSON still holds 6,878.
- `8b278cb964e07762c1e0c6b50c5c928051656cf8` the regenerated JSON (6,815) and the new `sampling.yml` note.
- `65810199d5e8da339bd126aec7d80c74184057a2` `CONTENTS.org` re-measured.
- `9eff034dd64405601209d60b77cadd9e1bb405fe` `slurm_logs` README.
- `fabf9bf8921595da12db69245de75d54bb84075e` the exception's inputs callable now returns only the 63 (the roster condition alone holds on 81); `test_gh1575`'s invented frame now expects the `panel_weight` hook. No served frame changes, and the JSON regenerates byte-identical.
- `2aa1e428736450e5190992019c1b5eaa0b71b5ba` `slurm_logs` scripts only.

**Which SHA each check ran at:**
- The cold all-table build and compare ran at `8b278cb96`.
- The census, the regeneration check, Feature parity and the final tests ran at `fabf9bf89`.
- Later commits touch only `CONTENTS.org` and `slurm_logs/`, neither of which is a build input.

### 2. Idiosyncrasies (Benin)

**From `CONTENTS.org`:**
- **The household key** is `(grappe, menage)`, with each grappe fielded in one vague.
- **2018-19 `i`** is `benin.i`.
- **2021-22 `i`** is built `E2_`-prefixed (`mapping.ID_PREFIX`) because 6,994 of its keys recur. The prefix makes both single-pass hazards impossible by construction, so I kept it.
- **`ehcvm_individu.age` was set from 2018-19 for panel members.** All age checks use the raw answers.
- **2021-22 `s01` lists 6,138 people who have left.** That is also the field the exception reads.
- **`s00q07d` separates nothing.** Confirmed.
- **All 670 grappes recur**, with the same GPS point.
- **`panel_weight` was `poids`**, a cross-sectional weight.
- **`s16bq05` is the gift question in both waves.**
- **`interview_date`'s `visit` level is `int64` cold and `Int64` warm**, the same on base.

**Found here, added to CONTENTS:**
- Benin had no `_/Makefile`.
- No 2021-22 file records a 2018-19 id.
- The rosters are pre-loaded, and member numbers carry over from 2018-19.
- The 63 replacement-like households, and the 19 with no pre-load.
- The 116, the 11 and the 96.
- The unit-spelling split across waves (F2).
- The FCFA-shaped purchase quantities.

**No contradiction with CONTENTS.** Its 2021-22 figures reproduce. Its 6,878-household figures predate the exception and are kept as history.

### 3. Measurements

Outputs are in `reports/benin/out/`; scripts are in `slurm_logs/.../1705-benin-panel/scripts/`.

**Link counts** (`make panel-ids`, job 41279824; asserted in the generator):

| 2021-22 households | count | served `i` |
|---|---|---|
| PanelHH = Panel, key in 2018-19 | **6,815** | 2018-19 id |
| the same, but whole pre-loaded roster gone (exception) | 63 | `E2_` |
| Panel, no 2018-19 key | 11 | `E2_` |
| Non panel, key recurs | 116 | `E2_` |
| Non panel, new key | 1,027 | `E2_` |

There are 0 duplicate targets. The 2021-22 files record no 2018-19 id: they hold only `PanelHH`, `s00q07d` and `s00q07e`, and `hhid` is a 2021-22 id.

**Link quality** on the committed crosswalk (`link_quality.py`, job 41279824). The control pairs each household with another 2018-19 household of the same grappe.

| group | n | head sex | head age +1..+5 | head birth year ±1 | size within 2 | member sex | member birth year ±1 |
|---|---|---|---|---|---|---|---|
| linked | 6,815 | 95.1% | 60.8% | 52.9% | 86.5% | 98.5% | 63.9% |
| its control | 6,815 | 68.6% | 10.7% | 5.7% | 54.0% | 61.6% | 10.9% |
| excepted 63 | 63 | 54.0% | 9.5% | 6.3% | 58.7% | – | – |
| their control | 63 | 55.6% | 6.3% | 4.8% | 46.0% | – | – |
| 116 recurring Non panel | 116 | 83.6% | 43.1% | 34.5% | 78.4% | 95.5% | 46.4% |

- Member-level checks use pre-loaded members found by member number: 100% found on the link against 70.0% for its control.
- **Red-team figures, not re-measured here:** 8.1% against 10.9% on the 63, 24/64 against 8/64 on the 19, and 978/1,653 on an ordinary linked sample.

**Every Benin table, cold, base against `8b278cb96`** (jobs 41279985 / 41279986; compare 41280134):
- No row count moves in any of the 37 calls.
- Base mapped through the crosswalk equals the branch on every column except two:
  - `plot_inputs` 2018-19;
  - `sample` 2021-22 `panel_weight`.

2021-22 rows re-keyed per table:

| table | re-keyed | table | re-keyed |
|---|---|---|---|
| sample | 6,815 | food_acquired | 174,703 |
| household_roster | 37,126 | food_expenditures | 117,495 |
| individual_education | 37,126 | food_prices | 118,564 |
| people_last7days | 37,126 | food_quantities | 173,389 |
| household_characteristics | 6,807 | plot_features | 6,830 |
| housing | 6,815 | plot_inputs | 10,443 |
| food_security | 6,800 | plot_labor | 9,309 |
| subjective_well_being | 6,719 | crop_production | 8,392 |
| interview_date | 20,443 | livestock | 5,893 |
| assets | 33,231 | shocks | 4,096 |

`cluster_features` and `community_prices` carry no `i`.

- **Mover rows:** 0 on both sides.
- **`nitrogen_kg` / `seed_kg`:** unchanged after the mapping.
- **Warm reads** (same process, `assume_cache_fresh`, fresh process): equal to cold everywhere except the known `interview_date` dtype quirk. `IdWalkError`: 0.
- **`check_panel_consistency`:** 8 of 8 pass (6,815 endpoints; 85% attrition).
- **`LSMS_BUILD_BACKEND=make`:** equals the default backend for `sample`, `plot_inputs` and `household_roster` (job 41279988).
- **`panel_weight`, 2021-22** (`pw_check.py`):
  - non-NA on 6,815 of 8,032;
  - every linked value is ×1.09887, a single constant, because the framework renormalises each weight column to mean 1 over its non-null rows;
  - `weight` and all 2018-19 weights are unchanged.

**plot_inputs 2018-19** (job 41279397; parity at `fabf9bf89`, job 41280493):
- 10,605 rows, unchanged.
- **Purchases:**
  - `Purchased` 383 → 4,719;
  - `Quantity_purchased` 21,714.25 → 354,908.23;
  - `Unit_purchased` 0 → 4,718 non-NA;
  - 5,015 `Purchased` and 5,048 `Quantity_purchased` cells change.
- **Gifts:** 383 in their own columns.
- **Units:** `u` Manquant 1 → 0 and Unknown 0 → 1. Unit columns are now spelled as in 2021-22 (`Kg` 2,363 / 272).
- **Feature against Country:** they agree on rows, `(i, t)` and every column, and the Feature index is unique. `household_roster`, `sample` and `food_acquired` are also equal.
- **F5 rows**, served as recorded:
  - grappe 221 / menage 37: 60,000 Sac(50kg) of NPK, 13,000 FCFA;
  - 33 / 19: 28,000 Sac(50kg) of urea, 14,000 FCFA;
  - 177 / 101: 10,000 kg of NPK, 10,000 FCFA.

### 4. Hash census (base `f138a3d61` → `fabf9bf89`, job 41280493)

| measure | moved |
|---|---|
| `build_transforms_fingerprint` | 0 of 42 |
| `framework_imports_fingerprint` | 0 of 38 |
| `Country._table_cache_hash` | 23 of 556, all Benin (22 existing tables plus the new `panel_ids`) |
| `Wave._input_hash` | 46 of 2,323, all Benin |

- **Errors:** 0.
- **Outside Benin:** nothing moves. One Benin re-warm.
- **Later commits:** the commit after `fabf9bf89` touches only `slurm_logs`.
- **Regeneration:** `make panel-ids` at `fabf9bf89` regenerates byte-identical JSON (git status clean).

### 5. Tests

| run | result |
|---|---|
| `fabf9bf89`, targeted: test_benin_panel_ids, test_benin_2018_plot_inputs, test_panel_ids_persistence, test_derivations, test_gh1575…, test_benin_2021_wave, test_id_walk, test_schema_consistency, test_build_transform_hash (`requires_s3` cold) | **590 passed** |
| `fabf9bf89`, `-k Benin` on test_sample.py and test_sampling.py | 17 passed |
| `fabf9bf89`, data-free suite (`-m "not requires_s3" tests/`) | 15,532 passed, 27 skipped, 4 xfailed, **1 failed: `test_gh932`** |

- **The `test_gh932` failure** (CI-wrapper junit parsing) passes alone (23 passed) and under xdist with `test_private_ci_check` (54 passed), on both head and base (job 41281041). It also passed in my three earlier full runs, and the red team saw the same thing. I read it as an order or load flake, not a defect of this branch.
- **27 skipped, not 35:** this run reused a warm root, so some cache-gated tests ran.
- **Two failures at `8b278cb96`, both fixed in `fabf9bf89`** (job 41279987):
  - the inputs test returned 81 households, not 63;
  - `test_gh1575` did not expect the hook to change `panel_weight`.
- **Not run:** the multi-country `requires_s3` cases for other countries, and `make matrix`.

### 6. Issues

- **#1705: closes.** Both hazards are asserted and tested; the link is verified against controls; the exception is registered; `sampling.yml` is updated. I added no `cluster_id_measurement` block: the boundary is `stable`, and a test pins exactly the 13 incomparable boundaries that carry one.
- **#1118: partial, Benin box only.** The other countries belong to other streams.
- **Gift/purchase:** implements the 2026-11-13 ruling; there is no issue.

**Residuals (drafts):**
- **Tier B:** "Benin 2021-22: households PanelHH leaves unlinked whose rosters point back." The 116 recurring Non-panel households: about 55-70% are the same household by the red team's estimate. Of the 96 new keys with pre-loaded rosters, about 28 look renumbered. Of the 11 Panel households with no 2018-19 key, one matches a household that is already linked. Any link would need a roster-linkage rule, which is a ruling of its own.
- **Tier C:** "Site Q: screen `plot_inputs.Quantity_purchased`." Motivated by the FCFA-shaped values in Benin 2018-19.
- **Tier C:** "Burkina 2018-19 plot_inputs unit columns decoded through the country `u` table only," the same shape as F2. Not measured.

### 7. Questions for the user

1. **Where the exception is registered.** It sits under table slot `panel_ids`, so no served frame's `attrs` lists it; it is visible in `derivations()` and `derivation_inputs`. Is that the right disclosure? The alternative, an empty table slot, would attach a `rows: 0` summary to every Benin table.
2. **`panel_weight` across the family.** Should the "NA for unlinked" rule carry to the other EHCVM-2 countries that serve `poids` as `panel_weight`, such as Togo and Côte d'Ivoire? Not in scope here.

**Must-states:**
- Sue regenerates the coverage snapshot for the new Benin `panel_ids` cells.
- `panel_ids.py` inserts its own directory into `sys.path` and is now imported in-process when the registry resolves the exception's callables. No data is read at import.

### Process

- Two earlier no-op `python3` calls ran on n0035 (`python3 -` with an empty heredoc, and `python3 -c 1`). Neither imported anything or touched data.
- Job 41275780 was cancelled for a wrong SHA, and job 41280426 was cancelled because the head had moved on.

---

