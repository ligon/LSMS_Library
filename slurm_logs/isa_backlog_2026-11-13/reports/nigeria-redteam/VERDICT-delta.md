**Delta verdict: APPROVE**, with two should-fix items that need a user decision before merge and four notes. Candidate `3d34550d56c13409e12f6cae989b25346a0f1cca`; previous candidate `2ebdad8b372c29f45808e9a7c0bcd281a4e36b4a`; base `f138a3d61c203f892148592675a7ef72e160a322`. The worker's "Changes since `2ebdad8b3`" claims hold: the old-to-new row diff is exactly what was claimed, and the totals reproduce.

## Findings

**D1 — should-fix, needs a ruling. W2 liquids flagged `g_cl = 1` are served about 10x low.** This is the original defect in reverse, and the "solids as grams" ruling does not cover it.
- **How it was found.** I screened every W2 cell against the container volumes the tables' own palm-oil and groundnut-oil rows imply: mudu 1.272 l, milk cup 0.159 l, tiya 2.544 l, paint rubber 3.657 l, derica 0.08 / 0.36 / 0.72 l. The screen is numeric, not by item name.
- **What it found.** Cells flagged "grams" whose `conv` is the container's volume in centilitres, so they are served at 0.08–0.10 kg per litre of container:
  - Milk tinned (unsweetened) / milk cup, `conv` 15: 45 rows (2012Q3) and 34 rows (2013Q1).
  - Fresh milk, bottled water, palm wine, pito, honey and butter/margarine.
- **Size.** About 65 + 42 rows, roughly 11 kg served where the centilitre reading gives about 110 kg.
- **Effect.** W2 per-kg prices for these items are about 10x high.
- **Fix.** Read these items as centilitres under the liquid list, or hold them on the inference. Either needs the user's ruling.

**D2 — should-fix. The jerry-can release is right, but `u = 'l'` misleads.**
- **The release.** On base the 71 rows sat on the `metric` rung at one litre per can (645 kg). They are now `survey_kg` at 10 / 20 / 25 kg per can (14,310 kg). Their value is 210 Naira/kg, against 233 Naira per litre on true-litre lines, so the release is consistent.
- **The label.** `Quantity` counts cans (median 4, maximum 100), while `u` says litres. Naira per unit is 4,200 on these rows against 233 on real litre rows.
- **The library's own audits fire on all 71.** `survey_kg_off_label` flags 54 of 71 as high, and `survey_kg_vs_metric` disagrees on 71 of 71.
- **Only W3 is affected.** W1 and W2 can labels already decode to `Jerry Can`.
- **Fix, with care.** `_canon_unit` is shared beyond the harvest unit: sold units (lines 1258 and 1451), `plot_inputs` through `_canon_input_unit`, and community prices through `_canon_community_unit`. Reordering "jerry" before "litre" there has an unmeasured blast radius. Either override harvest codes 190–192 only, or measure every caller first.

**D3 — note.** The W5 post-planting `correctly-empty` verdict says the roster asks only a leaver's education at departure and the parents' education. It also asks `s1q39c` (a leaver's education "as of now"), non-null on 3,327 rows, every one with `s1q4 = 'NO'` (no longer a member). The closure holds for current members; add `s1q39c` to the evidence text.

**D4 — note / question.** Three W2 post-planting codes (4, 19, 20) have no value label in the .dta and are served as `"4.0"`, `"19.0"` and `"20.0"`, one row each. The canonical note says `Unknown` is the sentinel for unmappable values. At minimum the float spelling is wrong.

**D5 — note.** Beverage powders fit neither reading by container volume, 43 rows and under 1 kg in all. Tea comes out at 0.013 kg/l as grams and 0.13 as centilitres; Milo and coffee at 0.13 against 1.26. They are read as grams, as the ruling says for non-liquids.

**D6 — note.** The `inputs` callables filter on `Derivation == key` exactly. A future compound key (`a+b`) would drop out of every inputs frame. There are 0 compound keys today; split on `+` instead.

## The seven points

**1. Liquid/solid split: correct.**
- **The solids.** Read as grams they sit at plausible densities per litre of container: tomatoes 0.78, onions 0.59, groundnuts 0.63 (unshelled 0.31), puree 0.58, eggs 0.74–1.18, dried fish 0.47, milk powder 0.79, beef and mutton 0.79, mangoes 0.91. Read as centilitres, every one is 3–14 kg/l, which is physically impossible.
- **Item 53, "Other oils and fats".** Centilitre density is 1.00 on tiya, milk cup and cigarette cup and 1.35 on mudu, measured against oils 50 and 52 only. Even if it is a solid fat (about 0.9 kg/l), the litre reading is within about 10%; grams would be 10x low. So it is correctly listed as liquid. It has no cell in the 2013Q1 class, and codes 50, 52 and 53 mean the same items in both rounds.
- **Worker's Q1.** Items 44, 71 and 84 all resolve to grams by container volume (0.63 / 0.58 / 0.74 against 6.3 / 5.8 / 7.4).
- **Which reference to trust.** Container volume refutes the price-implied reference on groundnuts: it implies about 2.6 kg per mudu, roughly 2 kg/l. The inference, W3's table, W4 and container volume all agree with the grams reading.
- **What the user should hear before merge.** This volume evidence, and D1.
- **Worker's Q6, derica.** W2's derica cells are self-consistent at a median 0.82–0.96 kg/l, and the whole grams class sits at a median 0.86 (interquartile 0.63–1.06). The "half of W3" gap is a container-size difference under one label, not a table error.

**2. The new key `w2-cl-class-read-as-grams`.** Its 1,521 rows (884 / 637) join 1:1 and reproduce `Quantity_kg`; all are `g_cl = 0` with kg exactly conv / 1000. The round keys' `g_cl = 0` rows are only the oils. There are 0 compound keys, so the union stamping after the collapse is in place but no real row exercises it.

**3. Jerry cans.** See D2.

**4. #1713 wiring.**
- **Post-planting rows.** W1 adds 17,116 rows and W2 17,403, equal to the non-null `s2q7` / `s2q8` counts. Post-harvest rows are frame-equal to base.
- **`harmonize_education` additions.** Every W1 label total reconciles to its level, so the new spellings join the existing collapse correctly.
- **Person ids.** 17,116 of 17,116 (W1) and 17,402 of 17,403 (W2) match the roster.
- **Verdict checks.** D1 is confirmed from the file listings: W3–W5 ship no post-planting section-2 file. C4 is confirmed from a crude text extraction of the questionnaires (no PDF library is available):
  - W3: the contents page prints "2. EDUCATION … PH ONLY".
  - W4: no education section; parents' education only.
  - W5: parents' and leavers' education only (D3).

**5. `fwimp` moving for Nigeria (1/38).** Acceptable. `union_keys_by_group` is already in the closure of the `@build_transform()` root `_normalize_dataframe_index` (country.py:10391 / 10620), so it is already in every `btf`, and this adds no new re-warm trigger. Avoiding it would mean duplicating the union logic in `nigeria.py`, which is worse. Established by reading the code, not by a probe.

**6. Hash census: confirmed.** `btf` 0/42, `fwimp` 1/38 (Nigeria), tables 24/555 (Nigeria), waves 240/2,321 (Nigeria). The canonical `data_info.yml` edit moves nothing outside Nigeria.

**7. Must-states.** The worker's eight are accurate. Add:
- D1, the liquid cells served 10x low.
- D2: `u = 'l'` counts cans, and the library's own audits fire on all 71 rows.
- The container-volume evidence behind the grams re-read.
- D3, the W5 verdict wording.

## What I re-ran
- **Cold build of the new candidate, plus census** (job 41277138), compared against my round-1 cold base frames.
  - `food_quantities` reproduces exactly: 2012Q3 201,888, 2013Q1 158,307, total 1,659,442.
  - `food_prices` keeps every row; W2 Price changes on 10,886 / 12,709 rows.
  - `harvest_kg` 2016Q1 is 8,887,081 kg, with `survey_kg` 8,507 rows and `metric` 493.
  - Every other table is frame-equal to base.
- **Row-level diff, old candidate to new.** Exactly 1,521 food rows (`Quantity_kg` and `Derivation`), 71 crop rows, 0 `community_prices` rows, and +34,519 education rows.
- **Tests** (job 41277176): 2,259 passed, 15 skipped, 0 failed, including `test_coverage_matrix` and the cached W2 split test.
- **Other jobs:** 41277173, 41278007, 41278163, 41278557.

## Limits
- Base was not rebuilt; I reused the round-1 cold base frames.
- The crop-side `volume_as_mass=False` run was not repeated.
- C4 rests on crude text extraction; the W2 PDFs were not read, and the World Bank's `g_cl` documentation is unverified.

## For Sue
- **Kept data roots to clean on the user's go:** `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/private/nigeria-redteam/job-41274078`, `job-41274079` and `job-41277138`.
- **Private files there:** `frames-{base,cand,cand2}`, `analysis-full.json`, `analysis2-full.json`, `delta-full.json`, `delta2.json`.
- **Report files** (household ids stripped) are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/nigeria-redteam/out/`:
  - `analysis2.json`
  - `cap.json`
  - `w5edu.json`
  - `hashdiff2.json`
  - `pytest-cand2.txt`
- I ran no Python on n0035 this round.
- At integration, `absent_verdicts.csv` may conflict with other streams' rows.
