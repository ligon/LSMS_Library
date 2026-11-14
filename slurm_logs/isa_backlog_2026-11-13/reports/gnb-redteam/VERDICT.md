**Verdict: CHANGES (minor, no blocker).** Candidate `c35c95eaf4dfd4c70474e7ba0c63110aa47d4bcc`, base `f138a3d61c203f892148592675a7ef72e160a322`.

The worker's headline claims hold up in my own cold base-vs-candidate re-runs. That covers the food hold, the weights, the Phase-A frame equalities and the hash census. Three tracked-file edits are needed (F1–F3), plus PR-body must-states and one missing user question (F4).

## Findings

**F1 (should-fix): asset code 26 is served under its raw Portuguese label in 2021-22.**
- `Cortador de relva ou outro artigo de jardinagem` (lawnmower) is in neither the country nor the global `harmonize_assets`.
- 2018-19 has no owned rows of it, so it never surfaced before. In the cold 2021-22 `assets()` it is the one `j` present in 2021-22 and absent from 2018-19 (`out/compare.json` assets21, `out/probe2.json`). I did not count its rows.
- The CONTENTS line "harmonize_assets gains them" covers only codes 25 and 30.
- Fix: one row in `Guinea-Bissau/_/categorical_mapping.org`. Benin maps its twin to `Other`; the global table puts lawn mowers under `Appliances`. Pick one and say which. It costs no extra cache: Guinea-Bissau's hashes already move.

**F2 (should-fix): the weight comment and its precedent.**
- The served weight is right. `hhweight` is the cross-sectional household weight:
  - positive for all 5,351 households, including the 454 non-panel ones (mean 86.3 against 38.5);
  - its sum is 227,784 households, or 1.754M people weighted by `hhsize`, against 205,337 and 1.681M in 2018-19;
  - it equals `ehcvm_welfare.hhweight` ("Ponderation menage") on 5,351 of 5,351;
  - within a grappe it varies only with `panelHH`;
  - no other weight variable exists in any of the 60 files of this wave.
- The source label " Ponderation panel" is misleading. Unlike Benin's comment, the 2021-22 `data_info.yml` comment does not say "Do not read it as a panel weight".
- The comment's "as Togo, Benin and Cote d'Ivoire 2021-22 serve their one weight" is loose: Côte d'Ivoire's file has eleven weight columns. Mali 2021-22 (`hhweight` plus `PanelHH`) is the exact precedent.
- Fix: add the warning sentence, cite Mali and Benin, and must-state that `panel_weight` is not longitudinal.

**F3 (should-fix, CONTENTS prose the data contradict):**
- *Rule fit, "Litro, Unidade and Monte alike."* Ceiling to even fits Unidade and Monte as well as round-half-up does (TVD 0.064 vs 0.063 and 0.098 vs 0.099). Only the Kg and Litro zeros discriminate between them (half-up 0.036 / 0.075, ceiling 0.186 / 0.260). The fit is distributional across seasons and households, not a row-level proof.
- *GPS.* The worker reports 439 joined grappes, all three files cite "2 of 439", and the CONTENTS table's milieu counts total 439. I joined 445, yet the tail counts are identical. The gap is unexplained.
- *GPS reasoning.* The "two displacements" argument assumes both points are displaced. The 2018 point is the community questionnaire's "village or quarter" GPS; the 2021 point is the household-GPS variable, constant within each grappe. Label that reasoning as a hypothesis.
- *Nephew count.* `kinship.yml` and CONTENTS say 6,186 nephew/niece rows. Among served members it is 6,139; the larger figure counts the leavers too.

**F4 (should-fix, PR body and user questions): leavers dropped before #1565 is decided.**
- Sue's own #1565 comment says the decision "should come before Guinea-Bissau's household_roster step".
- Measured: 341 leavers, none of them in `ehcvm_individu`. 9,727 rows with a blank status are kept, and all of them are in `ehcvm_individu`. Served = `ehcvm_individu` = 41,160.
- This follows Benin's de facto drop, with no derivation registered (Benin registers none either). Add it to §7 as a question; today it appears only in the issue table.

**F5 (note): strata registry and scope.**
- Region × milieu is well supported. The file's own `strate` maps one-to-one onto the 17 region × milieu cells, the BID calls region × residence the stratum, and region/milieu hold for all 450 grappes.
- Scope: the ruling names Togo, Benin and Côte d'Ivoire, and the #1575 ruling says "no … 2021 integration". The brief widened it; it needs the user's confirmation (worker Q4).
- The registered `resolve()` reproduces the served 2021-22 value on only 2,597 of 5,351 rows; the rest comes from the `strata` spelling fold. In 2018-19 it reproduces 0 of 5,351, so this predates the PR.
- The Côte d'Ivoire ruling required its own key for a fold. Ask whether that applies to Guinea-Bissau.

**F6 (note): shocks.**
- d/e/f behave as their 2021 labels say. "Not concerned" on `d` is 18.6% among livestock owners against 45.8% among non-owners; `e` and `f` do not depend on ownership. 2018-19 shows the same pattern for `d` (18.8% vs 49.8%), which confirms that wave's stale label.
- All four answer labels match the mapping keys, so no value becomes NA silently.
- My independent recompute of HowCoped0–2 equals the served frame exactly.
- Code 25 (`nonoui`): 34 rows get a constructed rank, and in one of them it fills a missing rank 1.
- 961 rows serve HowCoped0 = `Aucune stratégie`, as the Benin, Togo, Côte d'Ivoire and Niger hooks do.
- 4,417 of 10,842 reported shocks are served. This is in CONTENTS but not in the PR body.

**F7 (note): assets.** 14 owned lines have all four numbers NA (2018-19: 0) and are dropped by the framework's all-NA drop. Per-household counts equal the owned lines that carry a number (29,713). The duplicate index (1,722 rows against 1,511 in 2018-19) is the existing #1710.

**F8 (note): Phase A.**
- Every 2018-19 table is frame-equal to base except:
  - `plot_inputs`: equal after relabelling 4,855 rows;
  - `food_acquired` and the derived food tables: 2,990 rows relabelled from `Aubergine,` to `Jaxatu`;
  - `food_kg_factors`: only the label differs, so the kg pool does not move;
  - `community_prices`: 1,356 Djagatu rows and 696 product-94 rows relabelled, 80,202 rows on both sides. No rows are gained; the 696 product-94 rows now meet household cells.
- The 2018-19 slice of every all-wave call is frame-equal to base, dtypes included, so `MonthsSpent` stays int64.
- `crop_production` goes from 12,173 to 12,188 rows (250 `Aubergine,` become 202 `Jaxatu` and 63 `Aubergine`), and the `GrainCollapseWarning` is gone. `harvest_kg` goes from 730 to 731 rows (+20 kg).
- `nitrogen_kg` matches 293 rows on 188 households, for 1,012.5 kg of nitrogen in total (a figure the worker did not measure).
- `_unit_kg_factors` keeps `quintal` and adds `quilograma` and `grama`. `Feature('plot_inputs')` has a unique index.
- Two choices #1107 left open:
  - The generic NPK share is applied to 46 rows on Togo's precedent, where the issue says "decide".
  - `Tonelada` is undeclared (1 NPK row).

**F9 (note): the shared `kinship.yml` edit.** No other country uses `Sobrinho`, and the title-case lookup works on all 6,139 rows. The census moves nothing outside Guinea-Bissau.

**F10 (note): test re-pins are justified.**
- `crop_production.py` is AST-equal to base once docstrings are stripped, so the `ehcvm_condition_originals.json` hash change is comment-only.
- The re-pinned strata and cluster_features tests change only the wave scope.
- `PARTIALLY_WIRED_SAMPLE` is left stale for integration, per the batch rules, so `test_sample` does not check this wave until then.

## Re-run, and limits
- **Jobs:**
  - 41274016: raw probe;
  - 41274097: food follow-up;
  - 41274147 and 41274349: label and coping probes;
  - 41274076: cold build of every Guinea-Bissau table and mode, plus `Feature`, base and candidate in separate private roots;
  - 41274077: hash census and pytest.
- **Census:** tables 22 of 555 and waves 44 of 2,321 moved, all Guinea-Bissau; `build_transforms_fingerprint` 0 of 42; `framework_imports_fingerprint` 0 of 38. Same as the worker's.
- **Tests:** 31 files, 2,529 passed, 91 skipped (none of them Guinea-Bissau), 4 xfailed, 0 failed.
- **Not verified:**
  - The raw code-31/32 check on `crop_production` crashed, so "11 destroyed plus 4 identical repeats" is unverified.
  - The `Feature('plot_inputs')` relabel errored; I have a symmetric row diff and index uniqueness only.
  - The questionnaires were not read: the venv has no PDF library.
- **Not run:** `scan.py`, warm and fresh-process reads, the strict levers, the wide suite.
- **Not re-counted:** the 4,893 recurring keys, the nitrogen mass split by unit, the 91.3% `depan` figure.
- **My own slips:** two no-op Python invocations on n0035 (`python3 -` on an empty heredoc, and `python3 -c 1`). Neither imported anything. I redacted two exact interview timestamps in `out/raw_probe.json` to year-month; count-1 item cells remain there. `out/` is mode 700.

## Must-states for the PR body
- 4,417 of 10,842 shocks are served.
- `household_characteristics` 2021-22 is now served automatically (5,348 households).
- `panel_weight` = `hhweight` is not longitudinal; the 454 non-panel households carry a positive value.
- Leavers are dropped pending #1565.
- The code-25 constructed rank (34 rows), and `Aucune stratégie` in HowCoped0 on 961 rows.
- 14 owned asset lines are not served.
- The generic NPK share; `Tonelada` undeclared.
- The strata registry relies on a spelling fold.
- `PARTIALLY_WIRED_SAMPLE` and the coverage regrade at integration.
- The nitrogen total, 1,012.5 kg.

## The worker's open questions
1. **Food: the framing is supported.** Lead with these facts:
   - Every positive 2021-22 `s07bq03a` (128,229) and `s07bq07a` (96,262) is an even integer: 0 odd, 0 fractional.
   - The same check on Benin, Togo, Côte d'Ivoire, Senegal, Niger and Mali 2021-22 finds odd and fractional values everywhere. Guinea-Bissau 2018-19 has 70,949 odd and 6,119 fractional of 125,483.
   - 3,945 consumed rows have q3 = 0, against 0 in 2018-19, and none of them has a `depan` in `ehcvm_conso`.
   - 1,760 rows have q7a = 0 with a positive value.
   - The quantities are stored as Stata byte, int8 (2018-19: double).
   - The 23 odd q4/q5 values sit on the same 23 rows, where q3 = q4 + q5.
   - The value `s07bq08` shows no rounding signature (6.5% odd, against 7.3% in 2018-19). The purchase `Expenditure` is derived from the rounded quantities, though, so it is affected anyway.
2. **DAP left raw:** consistent with #1107's own instruction (41 rows).
3. **Seeds:** the framing is fair. `seed_kg` reads nothing on base or candidate; relabelling seed rows `Seed` would be a served-value change.
4. **Strata:** see F5.
5. **GPS:** the facts are confirmed. The two points are different objects (community village GPS against the household variable), and the displacement explanation is unverified.
6. **2018-19 `Purchased`:** confirmed. `s16bq05` / `s16bq07a` are the gift question; `s16bq08` / `s16bq09a` are the purchase.

Re-checks for a new SHA: hash census (still Guinea-Bissau only), the 2021-22 `assets` label set, and `tests/test_gnb_2021_wave.py`.

Scripts are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/gnb-redteam/scripts/`, outputs in `.../gnb-redteam/out/`.
