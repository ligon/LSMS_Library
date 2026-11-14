**Verdict: CHANGES.** Candidate `2ebdad8b372c29f45808e9a7c0bcd281a4e36b4a` (branch `feature/nigeria-tables-20261113`), base `f138a3d61c203f892148592675a7ef72e160a322`.

The W2 food tables serve a whole class of cells at about 10x their weight. Everything else checks out: W3 food, the W3 crop table, the #587 post-harvest rows, Pepper, the hash census, the derivation joins and the test re-pins.

**Idiosyncrasies bearing on the change** (from `Nigeria/_/CONTENTS.org`)
- Two rounds per wave folder, each with its own `t`.
- Item codes collide between quarters (#1144), so items must be decoded per quarter.
- Unit codes collide across waves (#607).
- W3 prints the size in the unit code; W4 asks it in a separate column (#1033).
- #1035 decided that litre containers stay on the inference, and that a container weight fits a fraction of a container but not a count of them.
- "A factor belongs to the quantity block it was asked beside" (#859).
- The base #1518 note said the opt-in helper "does not wire a default table or select a geography". The candidate now does both.

## Findings

**F1 — blocker. The W2 `g_cl = 0` ("centilitres") class is served about 10x too heavy.** The worker attributed W2's rise to "a few vegetable cells". The cause is the whole class flagged `g_cl = 0`, which the code reads as conv / 100 litres = kg.

- **The table contradicts itself.**
  - In both W2 tables (2012Q3 post-planting, 2013Q1 post-harvest) a mudu of palm or groundnut oil is 1.272 l and a milk cup 0.159 l.
  - The same class gives a mudu of tomatoes 10 l, groundnuts 8, onions 7.5, eggs 15, beef 10.
  - It gives a milk cup of groundnuts 1.0 l and tomatoes 0.9.
  - One container cannot hold both. Under a grams reading (conv / 1000) the solids become 0.75–1.5 kg per mudu and 0.1 kg per milk cup, consistent with the oil volumes.
- **Against the inference it replaces.** Cell-median ratio 12.2x (38 cells, 2012Q3) and 11.8x (33 cells, 2013Q1). Under a grams reading: 1.22 / 1.18.
- **Against W3's own World Bank table.** 8.4x on 16 cells (2013Q1). Tomatoes / paint rubber is 30 kg in W2 against 3.45 in W3.
- **Against household price-implied kg.** 5.0x / 3.5x.
- **The `g_cl = 1` (grams) class is fine:** about 1.0 against all three references (118–173 cells).
- **Only the oils look right in the centilitre class.** Tomato puree, milk powder, tea and Milo are about 10x off too; tomato puree / small derica is served 1.05 against W3's 0.095 and the inference's 0.07.
- **Weight of the defect.** The solid-item cells (a name-based split, so a lower bound) are 734 rows in 2012Q3 (4,574 → 63,749 kg) and 488 rows in 2013Q1 (787 → 12,188 kg).
  - That is 83% of the +71,320 kg rise on 2012Q3's tabled rows, and about half of 2013Q1's.
  - 12 of those rows record Quantity ≥ 100 and alone carry 50,565 kg.
  - With the class held, the W2 movement would be roughly +6% and +7.5%, not the +37.5% and +15.0% headline. That counterfactual is approximate, because returning rows to the inference pool moves other factors.
- **Prices move too.** W2 `food_prices` per-kg values for these items are about 10x low.
- **Fix.** Hold the whole W2 `g_cl = 0` class from the `survey_kg` rung under a registered hold that carries this evidence; those rows go back to the inference, as at base. Holding the class needs no item classifier; name-based splitting already misfiles powders. Whether the flag or `conv` is wrong is not established, and re-reading these cells as grams would be the library reinterpreting a World Bank field, so put that to the user as a question.

**F2 — should-fix. Litres are handled two ways in one PR.**
- **Food side.** W2 centilitre rows and W3 liquid containers are served as 1 l = 1 kg in `Quantity_kg`. `volume_as_mass=False` cannot decline them: it changes 50,825 metric-label rows and 0 `survey_kg` rows.
- **Crop side.** The 71 jerry-can rows are held, citing #1035.
- **Fix.** Pick one rule. F1's hold removes the W2 part; the W3 oil rows remain. My W3 liquid count (4,918 rows) is unreliable, because the name regex also caught Watermelon, sachet water and powders.

**F3 — should-fix. Schema prose now contradicts the served crop data.**
- The canonical `data_info.yml` notes say a shipped crop table belongs in the read-time `shipped` layer, and that `Quantity_kg` is "as the survey wrote it down".
- Nigeria's `data_scheme.yml` still says crop_production "Stores REPORTED fields only — no kg conversion", and does not declare `Quantity_kg` (Niger and Mali declare it, optional).
- **Practical cost measured as nil today.** The `shipped` screen would reject 0 of the 8,436 rows; the largest factor is 190.4 kg, under the 250 cap. What is lost is provenance: `KgFactorSource` says `survey_kg` for a World Bank zone-joined table, and only `Derivation` discloses it.
- **Fix.** Declare `Quantity_kg` optional in Nigeria's `data_scheme.yml`, correct its comment, and leave the canonical note to Q6.

**F4 — note. Spillover onto W1 post-planting and W4.** The cited #1452 precedent includes a wave-scoping overlay (`_KG_WAVE_SCOPED_TABLES`) that this change does not extend to Nigeria.
- W1 post-planting: 3,679 `piece` rows, +543 kg. Against price-implied kg the median |log error| goes 1.297 → 1.314 and within-2x goes 29.0% → 28.5%, so not an improvement.
- W4: 521 inferred rows, 392 → 685 kg. No W4 cvn cell shares their (item, unit), so they cannot be scored.
- Small either way.

**F5 — note. #1713 has a third route.** A country-level `_/individual_education.py`, declared by no wave, would grade the post-planting quarters `absent`, as Nigeria `crop_production` already does in `latest.csv`. The worker's statement holds for wave-level declarations, and `food_coping`'s post-harvest quarters grade `dropped` the same way, so the worker's reading matches established practice. Add this as option (c) to Q7.

**F6 — note. Stale or loose prose.**
- CONTENTS line ~215 says W3 food carries a per-row `s10bq2_cvn`; neither W3 CSV has any cvn or conv column.
- The base #1518 paragraph ("does not wire a default table") is left standing above the new subsection.
- The `c8-unrecorded-size-drop` rule text says 13,767 rows while its inputs return 11,170 (the worker already flagged this).
- 28 and 37 W2 cells sit below half of W3's table (mostly derica sizes and milk cup); small kg.
- The W3 zone columns are float32, so served kilograms carry rounding of about 1e-7.

## Verified as the worker claims

| Claim | My result |
|---|---|
| **Point 1: crop 2016Q1, 365,707 → 8,873,416 kg** | Reproduced exactly, and plausible. Against kg implied by the household's own harvest valuation: median 1.075, 76.9% within 2x (6,631 rows, 9 crops). Against sale-implied kg: median 1.01, 63% within 2x (3,356 rows, 4 crops). Against W4 reported factors by (crop, unit): median 1.00, 98.7% of kg within 2x. No collapse by quantity band (≥ 100 band: 1.10). Top 20 rows hold 10% of kg. Only one new Site Q report (a low-tail `Quantity_kg` report, 2016Q1 crop). |
| **Point 3: own-zone column** | W4 cvn equals the own-zone value on 19,047 of 19,047 rows (2018Q3) and 15,373 of 15,388 (2019Q1) where zone and national differ; national on 3 and 8. Purchases agree. No national value sits beside a blank zone cell. |
| **Point 7: derivation inputs** | All four keys join 1:1 to the served rows, reproduce `Quantity_kg` on every row, and keep distinct `t` per round (2012Q3 only, 2013Q1 only, W3 both). The sale key still joins its 8,865 rows, 2,511 of them now compound. |
| **Point 8: post-harvest W3 Kg / l** | Kg matched-cell median 1.000 (91.3% within 2x); l 1.14 (85.8%). Comparable to the served post-planting W3 and post-harvest W4 controls. +3,433 rows, none lost, no other value moves. |
| **Point 9: test re-pins** | 2016Q1 rows by `obs` are 20,313 / 6,273; 2,852 cells priced at both locations, 1,134 differing. These match the re-pinned values. |
| **Point 10: hash census** | My own census: `build_transforms_fingerprint` 0/42, framework imports 0/38, tables 24/555 (Nigeria only), waves 240/2,321 (Nigeria only). |
| **Food headline numbers** | Every `food_quantities` kg figure and `food_prices` median reproduces exactly. `food_prices` keeps 481,191 rows key for key. |
| **#1537 Pepper** | Only codes 76 and 2140 decode to Pepper, and the ruling accepts either. `labels='Aggregate'` now passes nothing through. |
| **Other tables** | 16 base tables plus four non-kg derived frames (`food_expenditures`, `household_characteristics`, `food_prices(units='unitvalue')`, `food_quantities(units='units')`) are frame-equal to base. On the three tables that change, no values or dtypes change beyond what is claimed. |
| **Tests** | 2,175 passed, 15 skipped, 0 failed on the candidate, warm private root. Unlike the worker's run, the five Nigeria #866 cells now run instead of skipping. |

## Must-state items for the PR body
- The W2 +37.5% / +15% is mostly the centilitre class (F1).
- `food_prices` per-kg values for those W2 items move too.
- Every consumer of 2016Q1 harvest kilograms (yield, valuation) moves about 24x; neither the worker nor I measured these.
- On crop rows, `survey_kg` here means a World Bank table at the native key, not a total someone wrote down.
- Food litres-as-kg cannot be declined with `volume_as_mass=False`.
- W1 post-planting and W4 rows move through the pooled estimate.

## The worker's questions
- **Q1 (own-zone):** the evidence supports it.
- **Q2 (volume rows):** the framing is incomplete. Most of W2's "cl" class is solids (F1), and the crop side holds litres while the food side serves them (F2).
- **Q3 (spillover):** the framing is fine, but the user should know the #1452 precedent included the scoping overlay.
- **Q4 (far-off W2 cells):** understated. It is a class identifiable by its flag, not a handful of vegetables.
- **Q5 (9 held crop rows):** not re-run.
- **Q6 (canonical note):** the screening cost is zero today; the issue is documentation and provenance.
- **Q7 (#1713):** add option (c), the country-level route.

## What I ran, and limits
- **Jobs.** 41274078 / 41274079 (cold base and candidate builds of all tables, plus hash census). 41274178 (analysis; failed writing its JSON, rerun as 41274354). 41274413 / 41274520 / 41274598 (targeted sections). 41274768 (NA-aware dtype diff). 41274179 (tests). No jobs left running.
- **Not verified.** The World Bank catalog's `g_cl` documentation (no network), the W2 questionnaires, the crop-side zone check, Q5, and yield / valuation movement. Name-based liquid classification is approximate. The price- and value-implied references cover few crops.
- **Rule slips on n0035.** I ran two trivial `python3` invocations there (`print(1)` and an empty stdin; no data touched) and wrote a `/tmp` state file, since removed.
- **For Sue.** The data roots were kept and need cleanup on the user's go: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/private/nigeria-redteam/job-41274078` (base) and `job-41274079` (candidate). Frames and `analysis-full.json`, which holds household-level rows, are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/private/nigeria-redteam/`. Household ids were stripped from the copy in `out/`.

Files are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/nigeria-redteam/`:
- `out/analysis.json`
- `out/hashdiff.json`
- `out/pytest-cand.txt`
- `out/dtypes.json`
- `scripts/`
