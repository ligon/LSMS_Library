## Verdict: CHANGES

Candidate `d5fdf326cd2bcec359e3c1a89e63c732cb6e0815`, base `f138a3d61c203f892148592675a7ef72e160a322`.

The core of the change holds up under independent measurement:
- the link rule;
- `id_walk` running exactly once;
- the Makefile not changing any served frame;
- the hash census.

I found no blocker. Four should-fix items remain: one false claim about behaviour, one cross-wave spelling inconsistency, and two disclosure gaps.

**Benin idiosyncrasies that bear on this change** (from `CONTENTS.org`, checked):
- `i` is `(grappe, menage)` and each grappe is fielded in one vague.
- 2021-22 ids are built as `E2_`, because 6,994 of its 8,032 keys recur.
- `ehcvm_individu.age` was set by UEMOA from 2018-19 values, so all age checks must use the raw answers.
- 2021-22 `s01` lists 6,138 people who have left.
- `s00q07d` does not separate households.
- `panel_weight` is `poids`, a cross-sectional weight.
- `s16bq05` is the gift question in both waves.
- Benin had no `_/Makefile` before this change.

### Findings

**F1 (should-fix): "a failing wave script now fails the build" is false.** Base already raises.
- Job 41277364 put `raise SystemExit` at line 1 of `2021-22/_/plot_labor.py` in job-local clones and called `plot_labor()` cold:
  - base raised `CalledProcessError` from `python …/plot_labor.py`, after the "Makefile not found" UserWarning;
  - the candidate raised `CalledProcessError` from `make -s -j2 ../2021-22/_/plot_labor.parquet`, with no warning.
- I measured one route only: serial, cold, all waves, one script.
- The false statements are at:
  - `Benin/_/Makefile:13-14`
  - `Benin/_/CONTENTS.org:1411-1412`
  - `PR_BODY.md:19`
  - `REPORT.md:176` (question 3)
- `AGENTS.md:807` (pre-existing, "Still open") says that on a country without a Makefile "`try_script` catches the failure and warns, and the wave is skipped". That is contradicted for Benin at base. Its count "14 have wave scripts" is now 13. I am not ruling on other countries.
- **Fix:** state that failure behaviour is unchanged. What does change:
  - the warning disappears;
  - the error names the make target instead of the script;
  - wave scripts now run in the stage-1 builder, which runs in parallel when `LSMS_BUILD_WORKERS` > 1 (measured equal at 2);
  - `_probe_country_builder` now calls `make -n`.
- Reframe question 3 accordingly, and mark `AGENTS.md:807` as stale.

**F2 (should-fix): the 2018-19 unit columns spell kilograms differently from 2021-22.**
- 2018-19 `Unit_purchased` serves "Kilogramme" on 2,363 rows and `Unit_received_as_gift` on 272 rows.
- 2021-22 (`Kg` on 2,522 rows) and the `u` index in both waves spell it "Kg". A filter on `== 'Kg'` silently drops every 2018-19 kilogram purchase.
- Cause: `2018-19/_/plot_inputs.py:161,164` decodes through the country `u` table only (`_unit_labels(..., unit_map)`). 2021-22 uses `ehcvm.served_unit_labels('Benin', …)`.
- So the docstring's "as this country's 2021-22 script already reads it" and `CONTENTS:1425` are not true for units. Every other label agrees across the waves.
- **Fix:** decode both columns with `served_unit_labels`.
- I measured Benin only. Burkina's 2018-19 script has the same shape; I did not measure what it serves.

**F3 (should-fix, disclosure): 63 linked households look like different households.**
- On all 63, every pre-loaded member is recorded as having left (`s01q00aa` = Non). 52 are in the same dwelling (`s00q07e`).
- Their 261 current members are 156 "Nouveau membre" and 102 "Oublié".
- I matched current members to the same key's 2018-19 roster on sex and birth year ±1, ignoring member numbers (job 41278460):

  | group | members matched | same-grappe control |
  |---|---|---|
  | the 63 | 21 / 258 (8.1%) | 28 / 258 (10.9%) |
  | ordinary linked sample | 978 / 1,653 (59.2%) | 245 / 1,653 (14.8%) |

  On the 63, households with 2 or more matches are 4 against 4 in the control.
- So the 63 match at control level: these look like replacement households carrying a Panel flag.
- 19 linked households have no pre-loaded roster. They match at 24/64 against 8/64, so they are mostly the same people.
- Linking the 63 follows the ruling ("from PanelHH"). But `CONTENTS:1296-1299` presents the link as uniformly supported.
- **Fix:** record both counts in `CONTENTS.org` and the PR body, and ask the user whether to keep these 63 linked.

**F4 (note): the link rule is verified.**
- I derived the crosswalk from the raw covers myself; it equals the committed JSON byte for byte. Counts are 6,878 / 11 / 116 / 1,027, with 0 duplicate targets.
- BID 7.1 (crude text extraction of `ben_ehcvm2_information_de_base_eng.pdf`, job 41277523): "only households marked as panel in the "PanelHH" variable will have valid values for merging with the previous wave". The quote is accurate.
- UEMOA's `hhid` equals the grappe followed by the 3-digit menage on 100% of households in both waves. No `panel` or 2018-19 id variable exists in the `ehcvm_*` files. So nothing else on file points back to 2018-19.
- Member-level birth year is sharper evidence than sex. On linked, still-member, number-found members:

  | measure | linked | same-grappe control |
  |---|---|---|
  | same sex | 98.5% | 61.2% |
  | birth year ±1 | 67.1% | 11.5% |
  | birth year exact | 48.0% | 4.1% |
  | head sex | 94.8% | 68.3% |
  | head birth year ±1 | 55.2% | 6.3% |

  Exact birth-year agreement of 48% means the birth year was not pre-filled from 2018-19.

**F5 (note): `Quantity_purchased` now serves 3 values that look like FCFA amounts keyed into the quantity field.**
- 28,000 bags of 50 kg urea where 100 kg was used; 10,000 kg of NPK against 50 kg used; 60,000 bags of 50 kg NPK against 5 kg used.
- They are served as recorded, and the Site Q screen does not cover `Quantity_purchased`. Record them in `CONTENTS.org`.

**F6 (note): `CONTENTS:1303,1330,1360` cite `reports/benin/scripts/*.py`.** Those are scratch paths outside the repo. Copy the scripts under `slurm_logs/` or label them as external.

**F7 (note): two weak tests.**
- `test_the_generator_asserts_both_hazards` only greps the source for two words. I ran `make panel-ids` in a job-local clone: the assertions executed and passed, and the regenerated JSON is byte-identical (git status clean).
- `agree > 0.94` sits against a measured 0.9477, which is a tight margin.

### What I re-ran, each side in its own private empty root, at the head
- **Cold builds** (job 41277362), 73 calls per side: every table, both per-wave calls, `units=` variants, `market='Region'`.
  - No row count moves.
  - Base mapped through my crosswalk equals the candidate on every call except `plot_inputs` and its 2018-19 slice. There the only index change is the 166133 Manquant → Unknown row.
  - That row is a "bought, not given" line (`s16bq05` = 2, `s16bq08` = 1). That is why `Purchased` changes on 5,015 cells and not 5,016.
  - `Quantity_purchased` changes on 5,048 cells.
  - The only `attrs` difference is the `sampling` cluster_id_note and locator prose.
- **ID audit:** every base 2021-22 id is `E2_`. Every candidate 2021-22 id is either `E2_` or a link target, with 0 unwalked keys and 0 `E2_` ids in 2018-19.
- **Re-keyed rows** per table equal the worker's figures (for example 176,180 in `food_acquired`, 6,869 in `household_characteristics`).
- **Mover** rows: 0 on both sides.
- **Warm reads** (same process, `assume_cache_fresh`, fresh process): 70 of 73 equal on both sides. The other 3 are the known `interview_date` `visit` dtype quirk plus two `UnitsUnavailableError` calls, identical on both sides.
- **IdWalkError:** 0.
- **Feature:** equals Country on rows and `(t, i)` for all 22 tables, on both sides.
- **`derivation_inputs`:** 0 served keys missing from the raw inputs, on both sides.
- **`check_panel_consistency`:** 8 of 8 pass.
- **Candidate code over a copy of the base-built root:** equals the candidate cold build.
- **Other routes** (job 41277363):
  - Subset-first calls: equal on both sides. The one `crop_production` 2021-22 column-shape difference is pre-existing and identical on base.
  - `LSMS_BUILD_WORKERS=2`: equal.
  - `LSMS_BUILD_BACKEND=make` on all 7 script tables plus sample, roster and characteristics: equals the default backend, and base mapped equals the candidate.
- **`nitrogen_kg` / `seed_kg`:** equal on both sides (1,474,131.92 and 10,772,713.04).
- **Make checks:**
  - `make -n panel_ids.json` / `updated_ids.json`, with the script 120 s newer: "Nothing to be done".
  - `make -n clean` only removes `$(VAR_DIR)/*.parquet`.
  - No rule names a JSON in `make -pn`.
  - The country-builder probe is identical on both sides apart from `panel_ids`.
- **Hash census:** `build_transforms_fingerprint` 0/42, `framework_imports_fingerprint` 0/38, table hashes 23/556 and wave hashes 46/2,323, all of them Benin.
- **Tests at the head:**
  - the brief's list plus Benin files: 833 passed, 2 skipped;
  - `-k Benin` on the multi-country files: 43 passed, 1 skipped;
  - the data-free suite: 15,525 passed, 1 failed.
  - The failure is `test_gh932…`, which passes alone on both sides. I did not run base under xdist, so I cannot say for certain the failure is order-dependent.

### Not run
- `assume_cache_fresh` in a fresh process.
- Failure injection under `LSMS_BUILD_WORKERS` > 1 or on a per-wave call.
- Any other country's builds.
- `make matrix`.
- A full roster-linkage model for the 116, 96 and 11.

### Must-state items for the PR body
- 63 linked households have all pre-loaded members marked as left and match at control level; 19 linked households have no pre-load.
- `panel_weight` is the cross-sectional `poids` for all 8,032 households of 2021-22, including the 1,154 unlinked ones. That is contrary to the `sample()` convention ("NaN/zero for refreshment").
- The Makefile does not change failure behaviour (correct the current claim).
- The 3 implausible `Quantity_purchased` values are served as recorded.
- After F2 is fixed: the unit columns use the same spelling in both waves.
- Sue regenerates the coverage snapshot for the new Benin `panel_ids` cells.
- Cost is one Benin re-warm.

### The worker's three questions
1. **The 116 recurring keys flagged Non panel.** "A mix" is a fair reading:
   - head sex 83.6%, head birth year ±1 36.7%;
   - members' birth year ±1 49.5%, against 13.6% in their control;
   - 32 of the 96 with a pre-load match their own key strongly.

   My estimate is that roughly 55–70% are the same household. But the option "only those with a pre-loaded roster" does not discriminate: 96 of 116 have one. The user should also know two things:
   - 28 of the 96 new keys with a pre-load strongly match a 2018-19 household. None of those 28 has a 2021-22 record under its own key, against 87% recurrence overall, which points to renumbering. Caveats: the scorer is lenient on missing birth years and validates at 461 of 686.
   - 1 of the 11 Panel-without-key households matches a household that is already linked.
2. **`panel_weight` for the unlinked households.** Fair, but one option is missing: NA for all of 2021-22, because no longitudinal weight is shipped. The BID says the shipped weight is "calculated for each household, taking into account the panel dimension", which makes it cross-sectional.
3. **Accepting the Makefile.** Misframed (F1). The structural change is inert on every frame I measured, and the failure behaviour does not change.

### Process disclosures
- I ran two no-op `python3` calls on n0035: `python3 -c 1` inside a `sacct` command, and `python3 -` with an empty heredoc. Neither imported anything or touched data.
- All jobs (41277362–41278652) have finished. No in-tree parquets were written and every clone's `git status` is clean. Scripts and outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/benin-redteam/`.
