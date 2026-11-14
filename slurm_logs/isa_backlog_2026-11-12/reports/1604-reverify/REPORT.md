# PR #1604 re-verification after merging development

Verification-and-fix agent (Claude Code, model Claude Opus 5.5), dispatched by Sue (Slurm job 41201840, n0035.savio4), 2026-11-12.

Verdict: READY at `c0fb6fdbbe50600f94bbd987f466f535a153dcb4` (later head: see the addendum). The dispatched merge `621833da3faf8c1ca5e3467a23eb41a97ddcfd3d` was NEEDS-FIX. GitHub's PR head is still `621833da3`, and its `unit-tests` stays red until the coordinator pushes. Nothing was pushed, merged or written to GitHub.

## 1. Malawi idiosyncrasies that matter here (from `Malawi/_/CONTENTS.org`)

- **2024-25 is two catalog entries that overlap.**
  - IHS6 (8507) sits in `Cross_Sectional/`; IHPS 2024 (8520) sits in `Panel/`.
  - The 815 IHPS refreshment households are the *same interviews* as IHS6 households: `y4_hhid` is blank, and the Panel `case_id` is their IHS6 id. Stacking the two covers would count them twice.
  - Measured: 12,888 IHS6 households; 2,763 Panel households (815 refreshment, 1,948 retained). All 815 refreshment ids appear in IHS6.
- **2024-25 weights.**
  - `hh_wgt` and `panelweight_2019` are null on the 815; `wt_cross` covers all 2,763; `wt_long` covers the 1,948 retained.
  - The PR serves `weight` = `hh_wgt` for IHS6 rows and `wt_cross` for retained rows, and `panel_weight` = `wt_long`. In-tree comments still call this policy ("decision 48") held.
- **Household id prefixes are per wave** (`cs-17-`, `cs-19-`, GH #1278). Bare `case_id`s coincide across surveys, so the PR prefixes IHS6 ids with `cs-24-`; unprefixed, they coincide with 3 IHS3, 202 IHS4 and 83 IHS5 ids.
- **The panel crosswalk is a committed source.**
  - `make panel-ids` must regenerate it byte-identical.
  - Split-off households get `_N` suffixes. `update_id` used to check only the current wave, so 2024-25 reissued 25 ids that earlier households had held. The PR adds an opt-in `guard_minted_from: '2024-25'` to stop that.
  - 2019-20's own 10 reissued ids are deferred to a separate issue.
- **No geovariables** in 2024-25, so `cluster_features` has no 2024-25 rows on either side.
- **Food kg factors.**
  - IHS2 has no conversion table; IHS3 onward key factors by region.
  - Older CONTENTS text recommends "the mean of conversion factors" and fuzzy matching. The PR's own entry overrides both with maintainer item 49 (exact four-field keys, no averaging). That contradiction is recorded in CONTENTS itself.
  - Code 119 means maize yellow flour in the IHS5 factor table but Grain Meal (Gramill) in 2024. Unit 74 changed from "TIN 1 KG" to "BUNDLE".
- **Adding a food wave moves older waves' inferred kg factors** (GH #1487). This is expected; see §6.
- **Statements this PR now contradicts** (I reported these, not rewrote them):
  - Line 45: "Malawi has 5 rounds of data".
  - Line 5222: "No table of this wave is served: `_/data_info.yml` declares nothing". `sample`, `food_acquired` and `panel_ids` are now wired.

## 2. Interactions with development (`c964c6c4f..e5fdd2fa8`)

| Development change | Interaction with this PR | Result |
|---|---|---|
| #851 recall regime: `recall.yml` is now generated from `food_recall.csv`; the guide states live coverage; a test pins the record count | The PR hand-edited Malawi's `recall.yml` | 3 CI failures, fixed in `3c42756cc` |
| #1694: `test_gh1190_selected_farm_carrier.py` pins `malawi.py`'s syntax tree to a fixed baseline | The PR adds an `import functools` and 4 symbols to `malawi.py` | 1 CI failure, fixed in `3c42756cc` (additions named exactly; the pinned fixture is untouched, as its README asks) |
| `b90ec6e40` R1/R3 crop sales; #1694 H rows in `plot_inputs`; #1288 livestock `id_walk` | All are 2010-20 only | `crop_production`, `plot_inputs`, `livestock` frame-equal to base |
| `id_walk`, `panel_ids`, `update_id`, `_compute_panel_ids` | Not changed by development since c964 | The PR's `panel_ids_guarding` is still a faithful copy of the current function |
| `food_kg_factors` / `conversion_to_kgs` | Not changed by development | kg movement comes from the added wave alone |
| `tests/test_sample.py` `PARTIALLY_WIRED_SAMPLE` | Exempts Malawi 2024-25 from "sample covers every wave", but this PR wires it | Stale exemption removed in `c0fb6fdbb` |

## 3. Jobs and commands

Every job ran on `savio4_htc`. Each clone was checked out at a full SHA with the venv image mounted. Every cache and data directory was private to the job, and thread counts were pinned to 1. The library's code identity and the private data root were asserted in the preflight, `build_all.py` and `check_2024.py`; pytest and `make` inherited that environment but did not assert it themselves. I never set `LSMS_SKIP_AUTH` and never ran the `dvc` CLI. No clone ended up with a parquet inside `countries/`.

**Shared cache untouched:** zero files under `/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library` were modified after 06:50 today.

| Job | What | Result |
|---|---|---|
| GitHub run 43239300979 (`621833da3`) | full offline suite | 4 failed, 14,939 passed, 5,673 skipped. The 4: `test_recall_period::…live_count`, `test_recall_record::…csv_generates`, `test_recall_record::test_all_67_…`, `test_gh1190_selected_farm_carrier::…inverse_exact` |
| 41232296, candidate, cold root | targeted selection (27 files) | 4 failed (the same 4), 1,646 passed; `test_sample -k Malawi` 16 passed |
| 41232296, base, cold root | same selection (25 files; the PR's 2 new test files don't exist on base) | 1,611 passed, so no failure was pre-existing |
| 41232295, base | cold build of everything, then the selection on the warm root | build exit 0; warm run 1,611 passed |
| 41232294, candidate | panel-ids regeneration, cold build, Feature checks, then the selection on the warm root | all exit 0; warm run shows the same 4 failures and 1,646 passed |
| 41232377, `3c42756cc` | selection plus every test file that reads recall records | 1,650 passed, 0 failed; recall set 280 passed |
| 41232918, `c0fb6fdbb` | `test_sample -k Malawi` | 16 passed. Negative control (same edit on a base checkout) fails `test_covers_all_waves[Malawi]`, so the strengthened test does discriminate |
| 41232889 | compare job | failed at launch (`fusermount3` not found); resubmitted as 41232891, which completed |

## 4. Base vs candidate (cold builds, separate private roots)

I built 21 data-scheme tables plus 3 derived tables; `panel_ids` is a pair of dicts, compared separately.

- **All 21 non-derived tables are exactly equal on every wave before 2024-25**, and the set of household ids is unchanged.
- Row counts, base → candidate:
  - `sample` 57,118 → 71,954.
  - `food_acquired` 971,531 → 1,303,208.
  - The other 19 are unchanged; examples: `household_roster` 260,612, `crop_production` 141,661, `plot_inputs` 222,662, `plot_features` 68,339, `livestock` 39,126.
- Columns the candidate adds are empty on old waves: `ihps_hhid` and `ihps_weight` in `sample`, `Derivation` in `food_acquired`.
- `food_expenditures`, which uses no kg factor, is also exactly equal on old waves.
- No table has a duplicate index on either side.
- Crosswalk: `panel_ids` grows from 5,686 to 7,634 entries with every base entry unchanged. The earlier `updated_ids` maps (4,000 / 2,508 / 3,178) are identical, plus 1,948 for 2024-25.
- **Warnings:**
  - Identical by class everywhere except the food tables. There, `QuantityImplausibleWarning` goes 10 → 12 (`food_acquired`, `food_expenditures`, `food_quantities`) and 15 → 18 (`food_prices`).
  - `GrainCollapseWarning` (`cluster_features` 3, `shocks` 1), `NullReadWarning` and `NullIndexKeyWarning` are unchanged.
  - `LegacyFallbackWarning` 0 on both sides; no `IdWalkError`.

## 5. 2024-25

- **`sample`: 14,836 rows**, made of 12,888 IHS6 households (`cs-24-` ids) and 1,948 retained panel households (walked to canonical ids). No duplicate ids, `v` present on every row, and none of the 815 refreshment copies survives.
- **Raw weights (before normalisation) match the cover file on every row:**
  - IHS6 `weight` = `hh_wgt`: 12,888 of 12,888.
  - Retained `weight` = `wt_cross` and `panel_weight` = `wt_long`: 1,948 of 1,948.
  - The 815 refreshment households' `ihps_hhid` and `ihps_weight` ride on their IHS6 rows: 815 of 815.
  - Through the API, `weight` and `panel_weight` each have mean 1.0 within the wave.
- **`food_acquired`: 331,677 rows** across 14,836 households, all of which appear in `sample`. Borrowed kilograms are on exactly 169,995 rows, each tagged `Malawi::food_acquired::borrow-ihs5-factor`.
- **Crosswalk guard:** of 1,948 entries, 1,338 keep the id they carried forward and 610 are new `_N` ids. None of the 610 equals an id an earlier wave issued, so the guard works. This matches the PR body's 1,338 / 610.
- **Feature:** `Feature('food_acquired')(['Malawi','Uganda'])` and `Feature('sample')(['Malawi','Tanzania'])` both assemble with a unique index, and the Malawi rows equal the Country frames exactly. Feature only adds columns: `currency` and an empty `Price` for food, an empty `Derivation` for sample.

## 6. Kg movement

Older waves keep the same row counts; their kilogram totals rise slightly.

| wave | total kg, base → candidate | change | changed `food_quantities` rows | changed `food_prices` rows |
|---|---|---|---|---|
| 2004-05 | 605,265 → 613,728 | +1.40% | 32,387 | 19,068 |
| 2010-11 | 607,929 → 608,474 | +0.09% | 4,434 | 2,309 |
| 2013-14 | 235,130 → 236,110 | +0.42% | 8,219 | 6,370 |
| 2016-17 | 534,552 → 535,712 | +0.22% | 19,840 | 15,055 |
| 2019-20 | 584,955 → 585,400 | +0.08% | 15,377 | 12,262 |
| 2024-25 | new | 713,917 kg | – | – |

- Where a quantity changed, the candidate/base ratio has median 1.06 (1st–99th percentile 0.60–3.48).
- Rows shift from the pooled `unit` rung to the item-level rung:

| wave | `unit` rows, base → candidate | `item_unit` rows, base → candidate |
|---|---|---|
| 2004-05 | 30,674 → 30,572 | 85,068 → 85,170 |
| 2019-20 | 4,794 → 3,130 | 22,933 → 24,598 |

  The other waves move the same way, by smaller amounts.
- The ladder labels the 169,995 borrowed rows `survey_kg`. The borrowing is disclosed by the `Derivation` key, not by the factor-source label.
- These per-row counts aren't comparable to the ledger's c964 figures. The ledger compared against a baseline that already contained 2024-25; here the base has no 2024-25 at all.

## 7. Panel-ID regeneration

In a throwaway clone at `621833da3`, `make panel-ids` exited 0. `git status --porcelain` was empty, and both JSON files' sha256 hashes equal the committed blobs.

## 8. Fixes

- **`3c42756cc89c2505d7d963fca7b608fb26cd053c`**
  - Added the Malawi 2024-25 row to `food_recall.csv` and regenerated `recall.yml`. All 20 regenerated files load identical to before; only the counted header comment changed (34 of 58 → 35 of 59).
  - `docs/guide/recall.md`: 55 of 67 → 56 of 68.
  - `test_recall_record.py`: count 67 → 68.
  - `test_gh1190_selected_farm_carrier.py`: names this PR's additions explicitly. Negative controls (an extra assignment, an extra import, or a changed signature) still fail it.
  - No build input or cache hash moves.
- **`c0fb6fdbbe50600f94bbd987f466f535a153dcb4`**: removed the stale Malawi entry from `PARTIALLY_WIRED_SAMPLE`. Test-only.
- **Not fixed, for the author:**
  - The `local_tools.update_id_reserving` docstring cites `tests/test_gh1275_malawi_2024.py`, which doesn't exist; the real test is in `test_malawi_panel_id_guard.py`. Editing `local_tools.py` would move every cache fingerprint again.
  - The CONTENTS contradictions in §1.

## 9. What the PR body must now say

- Drop the "Scientific hold" paragraph and the draft status; the hold was lifted on 2026-11-12.
- Say what merging serves for 2024-25 `sample` weights, and say explicitly whether decision 48 is now ruled.
- Several in-tree comments still say "held" or "pending" and need an author decision: `data_info.yml`, `data_scheme.yml`, the `mapping.py` docstring, the CONTENTS top entry, and `derivations.yml` `validated_against`.
- Replace the c964-era validation with these figures:
  - Base `e5fdd2fa8`; 4 CI-interaction fixes in `3c42756cc` and `c0fb6fdbb`; targeted selection 1,650 passed.
  - The 21 tables equal on old waves.
  - 2024-25 `sample` 14,836 rows, `food_acquired` 331,677, borrowed 169,995.
  - Crosswalk regenerates byte-identical; old-wave kg moves +0.08% to +1.40%.

## 10. Not verified

- I did not re-run the full offline suite on `3c42756cc` or `c0fb6fdbb`. The only full run was GitHub's on `621833da3`. The fixes touch only tests, docs, YAML comments and one CSV row, and every test that reads them passed.
- `pr-data-validation` (the full credentialed suite, over 2 hours) has never run on this PR because it refuses drafts. My Malawi-scoped selection does not stand in for it.
- I did not measure parity under `LSMS_BUILD_BACKEND=make`, or `period=` conversion of 2024-25 beyond the recall tests.
- I did not check which wave the extra `food_prices` warnings belong to. Old-wave `food_acquired` is provably unchanged, so its 2 extra warnings must be 2024-25.
- Data was compared at `621833da3`, not rebuilt at the fix SHAs. The fixes touch no build input, so the frames are identical by construction.
- Only Malawi was built and compared, plus Uganda and Tanzania for the Feature checks. This PR moves every build fingerprint, so a corpus-wide re-warm is still owed after merge.
- There is no external accuracy check of the borrowed IHS5 factors.

## Addendum: follow-ups after the coordinator's reply (2026-11-12)

**New head: `7735489cceb0fd68acf273eff850a5974d71cc58`.** It supersedes `c0fb6fdbb`, and §9's "author decision" items are now done.

- **`4c28e2a961aa8954181c4d24d4e96c76f19ed18f`**: records the ruling (@ligon, 2026-11-12, "Lift hold, rebase, merge").
  - The ruling lifts the decision-48 / whole-wave-promotion hold for this PR's content only, so 2024-25 weights are served as wired.
  - It does NOT settle decision 48's joint-inclusion question, and no other wave's weights change.
  - Files reworded: the 2024-25 `data_info.yml` header, the `data_scheme.yml` `ihps_*` comment, the 2024-25 `mapping.py` docstring, the CONTENTS top entry, and `derivations.yml` `validated_against` (which now names the source-backed jobs).
  - CONTENTS line 45: "5 rounds" becomes 6 waves.
  - The 2026-11-04 entry's "No table of this wave is served" and "not decided here" now carry dated supersession notes; the original text is kept.
  - The `local_tools.update_id_reserving` docstring now cites `tests/test_malawi_panel_id_guard.py::test_empty_reservations_preserve_legacy_values_and_split_counters`.
- **`7735489cceb0fd68acf273eff850a5974d71cc58`**: `tests/test_gh1228_malawi_cowpea_label.py::test_the_label_joins_food_acquired_in_every_wave` pinned 5 Malawi food waves; it is now 6.
  - This defect was latent before c964. CI skips the test (`requires_s3`), and neither my earlier selection nor the PR's validation included it.
  - The broadened re-run (job 41233299, at `4c28e2a96`) found it: 1 failed, 1,583 passed.
- **Job 41234994, at `7735489cc`:**
  - `make panel-ids`: exit 0, `git status --porcelain` empty (byte-identical).
  - 48-file selection: 1,584 passed, 0 failed. It covered `tests/test_*malawi*.py`, `test_recall*`, `test_gh1190*`, `test_build_transform_hash`, `test_derivations`, `test_gh1578_derivation_duplicates`, `test_no_build_input_in_contents_org`, `test_notes`, `test_panel_ids_persistence` and `test_id_walk`.
  - `test_sample -k Malawi`: 16 passed.
  - No in-tree parquets.
- Still not run on the final head: the full offline suite and `pr-data-validation` (§10 stands).
- The CONTENTS line 5458 entry (#1485, development) still says "Malawi weights48 ... stay held". It is dated history and agrees with decision 48's joint-inclusion question staying open, so I left it.
