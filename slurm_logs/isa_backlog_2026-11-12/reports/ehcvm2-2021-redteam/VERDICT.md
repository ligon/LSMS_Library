# Red team: EHCVM-2 2021-22 wiring for Togo, Benin and CotedIvoire (GH #1697)

- **Reviewer:** Claude Opus 5.5 (`claude-opus-5-5`), an independent red team. I did not write the candidate, and I did not read the author's report.
- **Date:** 2026-11-12.
- **Candidate:** `feature/552-ehcvm2-2021-wiring-20261112` @ `6538bc8b88c331835c9f78a316f3bf2092bae2b5`.
- **Base:** development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

## Verdict: CHANGES

The wiring is sound where it can be measured:
- Every non-derived table of every older wave is frame-equal to base, including the CILSS waves.
- Ids and weights are unchanged.
- The five EHCVM countries outside the PR move neither a hash nor a served value.
- The 2021-22 `sample`, `shocks` and `household_roster` reproduce the raw files exactly.
- The generated records regenerate byte for byte.
- 2,201 tests pass, with no data-gated test skipped.

The verdict is CHANGES, not APPROVE, for two reasons:
- **F1:** a provenance-contract defect in Cote d'Ivoire's strata registry entry. It is small and fixable.
- **F2:** two known-defective 2021-22 food quantity columns are served as filed. The archive marked them WAITING, and no ruling of 2026-11-12 covers them. The PR must put them to the maintainer as merge questions, not as footnotes.

Nothing found warrants REJECT.

**What turns this into APPROVE:** F1 is fixed, and the maintainer answers F2a and F2b (serve as filed, flag, or hold). Everything else is a must-state.

## Findings

| # | Sev | Where | Evidence | Requested change |
|---|-----|-------|----------|------------------|
| F1 | Medium | `lsms_library/countries/CotedIvoire/_/derivations.yml:247-284`; `CotedIvoire/2021-22/_/mapping.py:48`; `CotedIvoire/2021-22/_/data_info.yml:45` | One key, `CotedIvoire::sample::region-milieu-stratum` (waves `2018-19`, `2021-22`), registers `function: ehcvm:derive_region_milieu_stratum` (`:264`). But 2021-22 is served through `strata_label_upper_region`, which upper-cases the region first. The entry admits the gap (`:257-263`): "The registered function applied to the raw 2021-22 inputs therefore gives the lower-case spelling ...". So `rec.resolve()(derivation_inputs(...))` does not reproduce 23 of the 33 regions' served value. The entry's own assumption (`:284`, "retain each cover's recorded labels") is false for 2021-22. CLAUDE.md (Derived Values): "a rule that genuinely differs by wave is a different *name*". | Register 2021-22 under its own key, for example `...::region-milieu-stratum-upper-region` with `waves: ['2021-22']`, whose `function` is the wave's `strata_label_upper_region` or an equivalent that reproduces from the raw inputs. Alternatively, make the registered function reproduce the value for both waves. Either way, fix the assumption text. The VALUE is fine (see Q3); only the registry contract is broken. |
| F2a | High (must-state / maintainer) | `Togo/_/CONTENTS.org:966-1009`; served `Togo` `food_acquired` 2021-22 | Recorded as "WAITING: with the maintainer": `s07bq03a` carries an undocumented regional factor (Centrale 1.67 ... Grand Lomé 0.91/1.2, by row). This leaves positive residual "purchases" on 11,952 of 12,963 `Jamais` rows. The served symptom agrees, derived by subtraction from my counts: `Expenditure` is NA on 66,465 of 136,214 served 2021-22 rows. Less the 28,373 produced and in-kind rows, that leaves about 38,092 of 107,841 purchased rows (35.3%; CONTENTS gives 19.0% for 2018). This is not a per-row measurement. `food_quantities` and `food_expenditures` of 2021-22 are therefore not comparable across regions or waves. The GO ruling (decisions.org, 2026-11-12, "EHCVM-2 2021-22 wiring ... GO") does not mention this. | Put it to the maintainer in the PR as an open merge question, with the archived patch options. Do not merge it silently as "as filed". The alternative is to hold Togo 2021-22 `food_acquired` purchased quantities until ruled. |
| F2b | High (must-state / maintainer) | `CotedIvoire/_/CONTENTS.org:1224-1238`; `CotedIvoire/2021-22/_/data_info.yml` (`food_acquired` comment) | Recorded as WAITING: 6.7% of positive `s07bq03a` rows (H) read as the household's answer replaced by a typical per-adult-equivalent amount, and 8.6% (L) as partly converted through NSU weights. 2018-19 has no H rows. This is a wave break at the top of the distribution: per-capita `q3` ratio 0.90 at the median, 0.57 at p99. It is served as filed and the rows carry no flag. | As F2a: put it to the maintainer explicitly. The CONTENTS already offers "a per-row key on H rows" as a tested patch, so say whether it is wanted. |
| F3 | Medium (must-state) | `lsms_library/transformations.py:2124` (`'Très grand'` added to `_UNIT_SIZE_WORDS`) | The hash probe, base against head over the whole corpus, moves GhanaLSS 21 of 21 `_table_cache_hash` and **147 of 147** `Wave._input_hash`. That is a full cold GhanaLSS re-warm, at both tiers, beside Togo 22/22 + 44/44, Benin 22/22 + 44/44 and CotedIvoire 21/21 + 126/126. `build_transforms_fingerprint` moves 0 of all tables; every other country moves 0. GhanaLSS served values were not built here. The CONTENTS quote "GhanaLSS 21 of 21" omits the 147 wave hashes. | State the GhanaLSS cold re-warm, wave tier included, in the PR. The standing hold says no shared-cache rebuild, so the merge itself stales GhanaLSS everywhere. |
| F4 | Medium (scope vs. ruling) | decisions.org 2026-11-12 GO: "then food (with #1598's own-country NSU weights)"; `Togo/_/CONTENTS.org:1009` "not wired (not ruled for single-wave countries)" | The GO text names #1598 inside the food step. The candidate wires none of the three `ehcvm_nsu_*2021.dta` files, and its CONTENTS says "not ruled", which conflicts with the GO wording. #1598's own body says "sequence after the relevant wave wiring", so deferral is defensible, but the candidate's reason is wrong. Kilograms for 2021-22 come from the price-ratio ladder. Measured: the old-wave kg movement is 2018 Togo 63,624 → 66,987 kg (91,261 rows), Benin 168,465 → 186,631 (126,788 rows) and CIV 358,536 → 359,170 (128,245 rows). | Say in the PR that #1598 is deferred, not unruled, and keep #1598 open. Correct the "not ruled" wording in the three CONTENTS files. |
| F5 | Low (must-state) | `Benin/2021-22/_/data_info.yml:13,92`; `Benin/2021-22/_/mapping.py:56` | #1565 says the leavers decision "should precede their `household_roster` step". The candidate drops the 6,138 departed people (`s01q00aa` = non; measured 6,138 of 48,798 `s01` rows, 0 served), matching `ehcvm_individu` (42,660), before that decision. The drop is reversible and disclosed. Togo and CIV `s01` carry no leavers (0 blank rows). | State it in the PR as a de facto choice pending #1565. |
| F6 | Low (must-state) | `Togo/2021-22/_/categorical_mapping.org:157-159` | Codes 3, 4 and 5 (`Tubercule`, `Gousse`, `Régime`; 1,032 served rows) map to `other_condition` "per the coordinator's direction". The answer is kept in `condition_native`. That is outside the ruled fold of #1436, and a ruling is asked for. | State it in the PR. |
| F7 | Low (pre-existing; extended) | `Togo/_/categorical_mapping.org:545` and the `assets` index `(t, i, j)` of all three | The served `assets` index is not unique, in old and new waves alike. Head 2021-22 has Togo 958, Benin 1,871 and CIV 2,088 duplicated `(t,i,j)` entries; the old waves have their own, unchanged from base. No `GrainCollapseWarning` fires. This is consistent with a read-time `harmonize_assets` relabel merging native items after the collapse. The candidate adds such rows for 2021-22 (`Bicyclette, vélo de course` → `Bicycle`). The mechanism was not traced row by row. | It is not this PR's defect, but the new wave enlarges it. File an issue, or check whether the new rows add collisions. |
| F8 | Info | `lsms_library/ehcvm.py` | `harvest_totals` / `derive_hh_crop_harvest_total` duplicate `mali.py`'s `_harvest_totals` / `derive_hh_crop_harvest_total`, so an edit to one rule will not reach the other. `harvest_totals` is code-identical by AST, apart from the docstring, annotations and the tuple name. `derive_hh_crop_harvest_total` takes `key` as a parameter and stamps with `union_keys` inline, where Mali calls `_with_key` (`mali.py:1076`), which does the same `union_keys` on a mask. It is the same rule, not the same code. This matches the ruling ("Extend Mali's derivation"), and Mali stays out of the fingerprint. | None required. Note it in the PR. |

## Answers to the questions

### Q1. Older waves
**Frame-equal.** Cold builds in separate private roots (job 41242587; compare re-run in job 41242828 as `results/job1_compare_v2.json`). I filtered head to the `t` values present in base's frame. A first pass that filtered to `Country.waves` was wrong, because base already lists the unwired 2021-22; I discarded it.

**Togo 2018, Benin 2018-19, CotedIvoire 2018-19 and its four CILSS waves:**
- All 24, 24 and 23 tables were compared, including `sample`, `household_roster`, `cluster_features`, `food_acquired`, `crop_production`, `plot_*`, `shocks`, `assets`, `nonfood_expenditures` and `community_prices`.
- Every non-derived table is `equal` on base's columns, with identical dtypes and identical row sets.
- The comparison is `assert_frame_equal`, default float tolerance, after sorting the index.
- New optional columns (`Unit_sold`, `Derivation`, the four `plot_inputs` gift/purchase columns, `HowCoped2`) are 100% NA on old waves.

**Household ids and weights:** unchanged, since `sample` and `household_roster` are frame-equal. Independently, `git diff --stat e5fdd2fa8 6538bc8b8` is empty for `Togo/2018`, `Benin/2018-19` and the five older `CotedIvoire` wave directories, and no `.dvc` sidecar changes anywhere. No old-wave config, script or source moved, which backs "unchanged" beyond the comparison's float tolerance.

**Derived tables:** only kilogram modes move: `food_quantities` and `food_prices` in `kgvalue`, the #1487 pooled-factor effect. Numbers are as in F4 above. `food_expenditures`, `household_characteristics`, `food_quantities(units='units')` and `food_prices(units='unitvalue')` are equal. Benin `food_prices` gains 1 row (2 rows gain a factor), as disclosed.

### Q2. 2021-22 tables
Checks are independent of the author's tests (`results/job3_checks.json`).

**Sample:**
- Served households equal the weights file exactly: Togo 6,462, Benin 8,032, CIV 12,965. CIV's 728 non-interviewed cover rows are excluded.
- `weight` equals `poids / mean(poids)` (max abs diff 0.0), and `panel_weight == weight`.
- `v == grappe` and `Rural` equals the `s00q04` map on 100%.
- Each country has ids unique and bijective with `(grappe, menage)`, and the `E2_` prefix means they cannot meet older ids.
- No duplicate or NaN index level on any 2021-22 table, apart from `assets` (F7).
- `GrainCollapseWarning` fires only on CIV `shocks` (44 destroyed, 38 NaN-key) and CIV `plot_inputs` (55 destroyed). Both are disclosed in CONTENTS as damaged source keys.

**Roster:**
- Kinship expands with no unrecognised-label warning. NaN G/D/A are EHCVM "autres parents du CM ou du conjoint", `[null,null,null]` by design.
- `MonthsSpent` is 12/0 only. `Sex` is M/F only.
- 0 served members are absent from `s01`.
- Departed members: see F5.

**Shocks:**
- Served `AffectedConsumption` equals `s14bq04f` and `AffectedFoodStock` equals `s14bq04e` on every joined row: Togo 2,666, Benin 4,756, CIV 7,508.
- Label-free True totals agree exactly in Togo (f 1,858, e 1,811) and Benin (3,777; 3,668). In CIV they differ by the disclosed collapse: raw 5,270 against served 5,254 on f, and 4,845 against 4,829 on e. That is 7,550 keyed raw follow-up rows against 7,528 served.
- Across vagues, d, e and f are filled on the same rows (no per-vague layout in 2021-22).
- `d` behaves as the herd (Non-concerné 44% Togo, 41% Benin, 66% CIV; e and f 4-18%).
- Benin's variable labels (d "cheptel", e "stock", f "achat") are asserted by `test_the_labels_are_what_the_wiring_assumes`, which passed in my run.
- My own variable-label read failed (harness bug: `StataReader.close`), so the **Togo and CIV variable labels are not verified by me**. The evidence that remains:
  - behaviour, as above;
  - #1556's body, a record independent of this candidate: "`s14bq04d` is the livestock consequence and `s14bq04e` the food stock (labels read in Togo and Niger)";
  - Niger, Mali, Senegal and Burkina 2021-22 wire the same `s14bq04f` / `s14bq04e`.
  
  The fixed script and an **unsubmitted** `scripts/job4_labels.sh` (about 10 minutes) close this gap, if the coordinator authorises a fourth job.

**Food:**
- Sized `u` suffixes are within the pinned `SIZES`. Benin serves `Très grand` 228 rows (raw 223 consumed) and CIV 254 (raw 252).
- No numeric `u` is served. CIV `j` `119` is a disclosed unlabelled code.
- `Derivation` (`7day-purchase-at-last-purchase-unit-value`) sits on every purchased row and no other. `inputs` joins pass in `test_ehcvm_7day` for the new cells and in each wave test.
- Kg factors come from the ladder (F4).
- Quantity problems: F2a and F2b.

**Right files?** Measured from the raw files, independent of the author's tests:
- `sample` and weights (`s00_me`, `ehcvm_ponderations`);
- `shocks` (`s14b`);
- `household_roster` (`s01`, cross-checked against `ehcvm_individu` counts);
- `food_acquired` size labels (`s07bq03c`).

Resting on the author's own oracle tests, which pass in my root but share the author's file choice:
- `housing` (`s11q19` / `s11q20`), `assets` (`s12`), `food_security` (`s08a`), `subjective_well_being` (`s20aq05`);
- `plot_*` and `crop_production` (`s16a`-`s16d`), `livestock` (`s17`), `nonfood_expenditures` (`s09`), `interview_date`.

As corroboration only (names outlive meanings), the variables read are the ones Niger, Senegal, Mali and Burkina 2021-22 already read: `s00q01`, `s00q04`, `poids`, `s01q01`, `s01q02`, `s01q12`, `s07bq03a/b/c`, `s07bq07a`, `s07bq08`, `s11q19`, `s11q20`, `s12q01`, `s12q09`, `s20aq05`, `s08aq01`, `educ_hi`, `s14bq04e/f`.

**Crops:**
- Derived counts match the CONTENTS:
  - harvest-from-dispositions: Benin 5,237, CIV 8,932;
  - harvest-to-date: Togo 111, CIV 3,595.
- CIV crop codes 56-60 are served as numbers (disclosed).

### Q3. Strata
**#1575 construction:** reused exactly. The shared `ehcvm.strata_label`, `stamp_region_milieu_stratum` and `inputs_strata` are used, with one stratum per grappe in every wave. Every household is keyed and none is NA. Benin's `strat` is not served (`sample` columns: v, strata, Rural, weight, panel_weight, Derivation).

**Togo:** code 6 is `Lomé commune` in 2018 and `Grand Lomé` in 2021-22, on the same 98 grappes.
- Served literally: `Lomé commune Urbain` 1,034 in 2018 and `Grand Lomé Urbain` 1,169 in 2021-22.
- No Lomé Rural cell exists in either wave, and nothing is renamed.
- The BID quote (`Togo/_/CONTENTS.org:96-122`) is untouched by the diff.
- The other five codes have byte-identical labels.
- All 540 grappes keep their region code.

**Côte d'Ivoire upper-casing is a spelling normalisation, not a crosswalk.** All four discriminators hold:
1. `upper(2021-22 label) == 2018-19 label` on **33 of 33** codes (10 already identical; 23 lower-case or mixed, e.g. `gÔh` → `GÔH`, `gbÔkle` → `GBÔKLE`).
2. All 1,081 grappes common to the two waves keep their code. 31 of the 33 codes cover the same number of grappes in both waves. The two that do not are the three grappes that do not recur: code 1 `AUTONOME D'ABIDJAN` (84 → 82) and code 9 `SAN-PEDRO` (38 → 37).
3. The transform is an unconditional `str.upper()` with no lookup (`mapping.py:44-62`).
4. 2018-19's raw labels are all upper case.

The served strata of 2021-22 are a subset of 2018-19's 66 labels.

The value is defensible under "Same rule as #1575". The registry entry that describes it is not; see F1. Benin's labels are identical (lower case) in both waves, on all 12 codes.

### Q4. `ehcvm.py`
**AST scan:** head has 77 top-level names and no duplicate definition.
- All 66 of development's names are present with identical AST, except `__all__`, which is purely additive.
- The 11 added names are byte-identical by AST to the Benin archive's, which is also identical to Togo's 8.
- The CIV archive's copies of `harvest_totals` / `derive_hh_crop_harvest_total` differ only in the tuple's name (`DISPOSITION_VARIABLES` = `DISPOSITIONS`, same four variables) and an added `.astype(bool)` on the plot mask. The CIV script imports `DISPOSITIONS as DISPOSITION_VARIABLES`, so nothing is lost.

**Countries outside the PR:**
- **Hashes:** 0 of every table and wave hash moves in Senegal, Mali, Niger, Burkina_Faso and Guinea-Bissau.
- **Data:** cold `food_acquired`, `crop_production`, `food_quantities` (kgs and units) and `food_prices` are frame-equal base against head, all waves, in all five countries (job 41242588).

### Q5. Generated files
- The candidate's `recall.yml` (20 files) and `population.yml` (39 files) regenerate byte for byte. I ran `promote_recall_records.py` and `promote_population_records.py` in a throwaway job-local clone at head, and `git status` was empty afterwards.
- Count pins agree: `test_recall_record` (70 rows) passes, and the recall headers' "37 of 61" are generator output.
- `docs/guide/recall.md` "58 of 70" matches the CSV, by my count of single declared exposures. `tests/test_recall_period.py`, which pins that sentence, was **not run**.

### Q6. Tests
None weakened in substance.
- **Removed goldens:** `nunique() == 11/23` and `len(old_cells) == 66` are stratum-count goldens that the #1575 ruling forbids. The per-household oracle that replaced them is stronger.
- **Narrowed strata tests:** the three were narrowed in `03f438a54` and re-widened at head to assert the 2021-22 construction. Their dropped `KeyError` pins for an out-of-scope 2021-22 call are moot now that 2021-22 is in scope.
- **Re-pinned by necessity:**
  - `test_gh675` now checks the 2018-19 slice, because 2021-22 GPS comes from the cover.
  - The Togo nonfood pins are restricted to 2018; 2021-22 is pinned in `test_nonfood_expenditures_schema`.
  - `test_gh323` cluster counts are now per wave.
- **Strengthened:** `test_sample.PARTIALLY_WIRED_SAMPLE` loses three exemptions.

### Q7. Scope
- **No panel link:** the `E2_` ids cannot meet, and there is no Benin `panel_ids.json`.
- **No production-weight change** for any existing wave (Q1).
- **Harvest-from-dispositions:** only in Benin and CIV. The Togo registry has `hh-crop-sale-single-row` and `harvest-to-date` only.
- **Beyond the rulings:**
  - the shared `_UNIT_SIZE_WORDS` edit (F3);
  - the option-(d) sale attachment and `harvest-to-date` extended from Mali to all three countries (archived wiring, under the GO);
  - Togo condition codes (F6);
  - leavers (F5);
  - a new closing verdict in `.coder/coverage/absent_verdicts.csv` (CIV `plot_labor` 2021-22, `asked-not-distributed`, with C1/C2/C4 evidence).

## PR must-state items
1. **F2a, F2b:** Togo `s07bq03a` regional factor and CIV H/L rows, served as filed. These are open maintainer questions, with options.
2. **F1:** the CIV registry fix, or why not.
3. **Cache cost:** Togo, Benin and CotedIvoire whole-country re-warms, plus a **GhanaLSS cold re-warm at both tiers** (21 table and 147 wave hashes), from `Très grand`. 0 elsewhere.
4. **Kg pooling at read time moves the older waves** (Q1 figures, F4). #1598's NSU weights are deferred, not unruled.
5. **Shocks:** only rows with follow-ups are served in 2021-22 (Togo 2,666 of 7,229, Benin 4,756 of 10,982, CIV 7,528). A cross-wave incidence reads as a false fall.
6. **CIV damaged keys:** the GrainCollapse warnings on every read of `shocks` and `plot_inputs`.
7. **Leavers and conditions:** Benin leavers dropped pending #1565; Togo condition codes 3-5 pending a ruling.
8. **Unlabelled codes:** CIV crop codes 56-60 served as numbers; CIV `plot_labor` 2021-22 has no rows (verdict filed).
9. **`assets` duplicate index (F7):** pre-existing, extended to the new wave.
10. **No `Quantity_kg`** on 2021-22 `crop_production`, unlike Mali 2021-22.

## Jobs run
All jobs were on `savio4_htc`, account `co_carleton`, with `--exclude=n0035.savio4`.
- **Setup in every job:**
  - full-SHA clones of the mirror, and `bin/savio_venv.sh mount`;
  - import identity asserted against the clone;
  - a fresh private `LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` per library-using invocation, with `data_root()` asserted private;
  - BLAS threads pinned to 1, and `LSMS_BUILD_WORKERS=1`.
- **Two exceptions to that:**
  - The regen step's four generator calls shared one private root.
  - Slurm could not create the inherited `TMPDIR` and fell back to `/tmp` for the clone and mount steps, before any private `TMPDIR` was set.
- **Never more than 3 jobs at once, and 3 in total:** `job4_labels.sh` is written and NOT submitted.
- **Clean afterwards:** 0 in-tree parquets, and clean `git status`, in every clone.

| job | what |
|---|---|
| 41242587 | cold builds of every table of Togo, Benin and CotedIvoire, base and head, in six private roots |
| 41242588 | corpus hash probe, base and head; cold `food_acquired`, `crop_production`, `food_quantities`, `food_prices` for Senegal, Mali, Guinea-Bissau, Niger and Burkina_Faso, base and head |
| 41242828 | generator regeneration in a throwaway clone; `pytest` of 36 files: **2,130 passed, 4 skipped** (none data-gated), `-n 6 --dist=loadfile`; `test_schema_consistency` + `test_sample` for the three countries: **71 passed, 3 skipped**; corrected compare; independent 2021-22 checks |

Artefacts are under this directory: `scripts/`, `logs/`, `results/` and `frames/`.

**Disclosure:** on n0035 I ran system `/usr/bin/python3` (3.6) for AST scans of `ehcvm.py` / `mali.py`, for reads of the result JSON and the food-recall CSV, and to patch my own scripts. None of it imported `lsms_library` or read data.

**Script correction:** my harness's first `compare.py` filtered head to `Country.waves`. Base already lists the unwired 2021-22, so that pass compared nothing new. The corrected `job*_compare_v2.json` files are the ones cited.

## Unverified limits
- **Variable labels:** I did not read the `.dta` labels for Togo and CIV `s14b` or for the other declared variables (harness bug).
  - Benin's shock labels are covered by a passing test.
  - Togo's rest on #1556's record and on behaviour.
  - CIV's rest on behaviour alone.
  - The unsubmitted `scripts/job4_labels.sh` closes this.
- **Built nowhere here:**
  - GhanaLSS served values after `Très grand`;
  - warm-cache and fresh-process reads;
  - `assume_cache_fresh`;
  - `Feature()` assembly beyond what the test files exercise;
  - `market=`;
  - strict levers (`LSMS_*_STRICT`).
- **Not run:** `tests/test_recall_period.py` and the full suite.
- **Not independently re-measured:** the Togo regional factor (F2a) and the CIV H/L classification (F2b). I confirmed only the served symptom for Togo (about 35.3% NA purchased `Expenditure`, derived by subtraction).
- **Not traced:** the F7 duplicate mechanism, row by row.
- **Comparison tolerance:** frame equality uses pandas' default float tolerance (rtol 1e-5), not `check_exact`.
