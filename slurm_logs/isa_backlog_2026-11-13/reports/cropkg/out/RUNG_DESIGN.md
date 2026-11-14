# A price-implied inferred rung for crop units in `harvest_kg_factors` (GH #1559)

Design memo only. Nothing here is wired. The user rules on it before anyone builds the rung.

- **Stream:** cropkg, ISA batch of 2026-11-13. Branch `feature/1559-crop-kg-screen-20261113`, base development `f138a3d61`.
- **The ruling** (@ligon 2026-11-12, "GH1559 price-implied kg per unit for crops", "Also build a rung"):

  > (1) a report-only screen flagging survey kg outside 0.1x-10x of the price-implied kg per unit (baseline: kg sales plus sacks at stated mass), and (2) design a price-implied INFERRED rung for crop units in harvest_kg_factors, as its own design, never merged with the food ladder. Measured caveats to carry into the design: product state (shelled/unshelled, ~2.4x price gap; only 2021-22 records it), selection (21-24% of rows from sellers), seasonality (~1.3x, Mali), survey kg 1.1-1.4x the price-implied value at the median.

- **Part (1)** is on the branch: `quantity_audit.audit_survey_kg_prices` (see the PR body).
  - This memo is part (2).
  - The rung below uses **the same estimator** as the screen (`quantity_audit.survey_kg_price_factors`). Every number in this memo comes from that estimator, run on reshaped frames, so the screen and the design are one computation.
- **Measurements:** cold builds of Niger and Mali `crop_production` on the branch, in private node-local roots. Burst jobs **41275992** (at `cd1eebc6a`, the first cut on row text) and **41276357** (at `acd6fccba`, the integer-code rewrite; every section identical to the first), plus **41276727** (follow-up checks at `49aea7436`, `analysis2.py`) and **41279383** (the `s7fq13d` join, `analysis3.py`, at `5a7ee1a7f`). The red team's jobs 41277988-41278567 are cited where used (`reports/cropkg-redteam/`). The scripts are in the report's `scripts/`, and the aggregates in `out/analysis-*.json` and `out/analysis2-*.json`.

## Summary

0. **Read this first: the rung's main use is survey evidence that is not yet wired** (red team F1; checked row by row here, job 41279383).
   - Mali 2017-18's harvest file carries the instrument's own kilograms per unit: `eaci17_s7fp2.s7fq13d`, "Equivalent en Kg", the fourth sub-column of Q7F13.
   - **It joins to every served 2017-18 seasonal row** (24,449 of 24,449, quantity equal on all 23,348 positive rows). It is positive on 23,205 of those 23,348, and exactly 1 on all 1,688 `Kg` rows.
   - It is a factor per unit, so it fits the canonical `KgFactor` slot: the `reported` layer, which outranks any `price_implied` layer.
   - **Wired as `KgFactor`, simulated through the ladder as it stands:**
     - Mali 2017-18 would serve **32.8M kg**, against 1.66M today and 16.4M with the price rung. That splits as `reported` 25.9M and `survey_median` 7.2M (red team, job 41280001). The red team's own re-join, on household, plot and quantity without the crop map, gives 33.1M kg and 2,162 refused rows; the gap is 76 rows that join cannot tell apart.
     - The rows would resolve as: `reported` 21,039, `survey_median` 2,345, `none` 1,535.
     - **The reported-factor screen refuses 2,167 rows, every one above the 250 kg ceiling.** By unit (red team): `Charretée` 1,576, `Autre` 534, `Sac` 31, `Grenier` 21. None is a `Kg` row off 1.
       - **None is left unserved.** Each falls to `survey_median`, the median of the accepted factors for its unit, so 1,576 of the 3,729 cart rows are capped at the median of other carts.
       - They carry about 6.0M of the 32.8M kg. At their own reported factors they would be 82.2M kg.
       - That is the ceiling question, Q5, applied to the survey's own factors.
     - Household-crops with kilograms, all crops, would go from 7.6% to 94.6%.
     - No other wave would change.
   - **So wiring it is a separate ruling, to take before the rung** (Q0). 2014-15's sibling `s3aq08c` mixes factors and totals and is not usable as is (red team F1).
1. **Where the rung would serve.**
   - **Niger: almost nothing.** The rows it can serve are those on `none` today, and 13,240 of them are in 2018-19. That wave has no per-kilogram baseline: 21 `Kg` sales, and sacks that state no size.
     - The rung would serve **161 of them (1.2%)**, in two crops (roselle and fresh tomato).
     - Every cell rests on exactly 5 `Kg` sales, the floor. Roselle's 5 come from 2 households, fresh tomato's from 5, so a 5-household gate leaves 23 of the 161, all of them tomato.
     - Within roselle a `Gros sac` (21 kg, 37 sales) reads lighter than a `Sac Moyen` (28 kg, 6 sales). Whether those masses are wrong is not established (roselle is a light crop). The size inversion says the floor is thin for a rung (Q8).
   - **Mali, without Q0: a lot.** It would serve 9,981 of 21,631 rows in 2017-18 (mostly the unsized `Sac`, at 60-120 kg by crop), adding 14.8M kg to a wave that serves 1.66M today.
     - That is a change of coverage, not of level. Household-crops of millet, maize and groundnut with kilograms go from about 5% to 65-85% (red team F9, on those three crops only; the all-crop figure under the rung was not measured).
     - The three cells that carry it (`Arachide`, `Mil`, `Maïs` per `Sac`, 8,673 rows, 10.9M kg) rest on 5, 5 and 7 `Kg` sales, from 5, 5 and 7 households.
     - With Q0 wired, `reported` outranks the rung on all of those rows. The rung would then serve only what `s7fq13d` leaves on `none`.
     - It would also serve 763 of 1,767 rows in Mali 2018-19 and 218 of 9,206 in 2014-15.
2. **How good the estimator is, against four answer keys.**
   - **The instrument's own `s7fq13d` (Mali 2017-18, the best key):** rung / `s7fq13d` is 1.05 at the median over the 9,981 rung rows, with p10-p90 0.60-1.20 and 94% within 2x.
     - By crop and unit: `Mil` / `Sac` **1.20**, `Maïs` / `Sac` 1.05, `Arachide` / `Sac` **0.60**, `Coton` / `Sac` 1.00, `Sésame` / `Sac` 0.94, `Coton` / `Charretée` 0.86, `Mil` / `Gerbe` 0.50, `Arachide` / `Autre` 0.03 (79 rows).
     - `Sac` is 100 on 95% of millet rows, so `s7fq13d` looks like a nominal conversion, not a weighing.
   - **Stated sacks held out of the baseline:**
     - The 100 kg sack is inferred at 0.80x (Niger and Mali).
     - The 50 kg sack is 1.25x in Niger and 1.00x in Mali.
     - Mali's tonne reads 1.00x, but 222 of its 231 sales are cotton at what looks like one fixed price, so it is not evidence.
   - **The survey's own kilograms (Niger):** 1.13-1.25x the price-implied mass at the median, with 88-92% within 2x.
   - **Mali EAC-I's reported kg per sold unit** (`KgFactor_sold`): 1.00x and 0.95x at the median, 96-98% within 2x. Without cotton: 0.95 (millet 0.83, maize 0.95, sesame 1.00, groundnut 1.50).
   - **The keys disagree in sign for millet:**
     - It is 1.20 against `s7fq13d` but 0.81-0.83 against the held-out sack and `KgFactor_sold`.
     - So the price-implied millet `Sac` (120 kg) sits above the instrument's nominal 100.
     - Relative to `Kg` sales, the 100 kg sack sells as if it held about 80 kg.
   - **Groundnut is off on every key** (0.60, 0.56-0.68 and 1.50). It is 3,777 of the 9,052 served `Sac` rows; its state is not recorded in EAC-I.
   - **So for cereals, sesame and cowpea the estimator is within about ±20% at the median and within 2x on about nine rows in ten. For groundnut and rice it is not.**
     - Its lean depends on what the baseline holds: a lot bigger than the baseline's reads light, a smaller one heavy.
     - That does not explain the survey's 1.1-1.4x offset, which has the other sign (section 7(b)).
3. **Recommendation.** If the rung is built after Q0:
   - Make it its own layer, `price_implied`, placed after `inferred` and before `none`.
   - Key it `(t, j, state, u)` then `(t, j, u)`, plus an optional caller-resolved `region`.
   - Use the screen's gates as a minimum. Q8 asks whether a rung needs stricter ones counted in households (red team F2).
   - Disclose it per row, as the food rungs are.
   - Rule on the open questions in section 10 first, chiefly:
     - Q0, wiring `s7fq13d`;
     - Q1, opt-in or default;
     - Q2, whether reported factors (`s7fq13d`, `KgFactor_sold`) should feed `survey_median` above the rung;
     - Q3, window-mismatched donors (Niger 3E, Mali trees);
     - Q8, a household floor.
4. **Cache cost, measured with no-op probes.**
   - Editing `harvest_kg_factors`, extending `KG_FACTOR_LAYERS`, or adding a helper to `transformations.py` moves **0** of 555 table hashes, 0 of 2,321 wave hashes, 0 of 42 `build_transforms_fingerprint` and 0 of 38 `framework_imports_fingerprint` values.
   - The positive control (a no-op in `food_kg_factors`) moves exactly the GH #1509 set: Ethiopia, GhanaLSS and Uganda, 75 tables, 503 waves, 3 `fwimp`. See section 9.
   - Landing it is free in caches, provided the crop estimator stays outside `food_kg_factors`' closure.
   - Wiring `s7fq13d` (Q0) is different: it edits Mali's wave script and `data_scheme.yml`, so expect one Mali re-warm (not measured).

## 1. The estimator

For a cell (country, wave `t`, crop `j`, [product state], unit `u`), the price-implied kilograms per unit are:

```
kg_per_unit(cell) = median( Value_sold / Quantity_sold  |  sales of the cell in unit u )
                    ---------------------------------------------------------------------
                    median( Value_sold / (Quantity_sold * stated_mass(Unit_sold))
                            |  sales of (t, j[, state]) in a unit that STATES a mass )
```

- **The numerator is in the sale's own unit.** It is `Unit_sold`, never the harvest `u` (GH #824). A harvest row in `u` is served the factor of the cell whose `Unit_sold == u`.
- **Stated mass** is read, never estimated:
  - `KNOWN_METRIC`'s mass tokens: `Kg`, `Gramme`, `Tonne`, `pound`.
  - What `_parse_explicit_metric` reads off a self-describing label: `Sac de 100 kg`, `Sac (50 Kg)`.
  - Volumes are excluded (no litre is a kilogram here), as `_BASELINE_UNIT_CONVERSION` excludes them on the food side.
  - A country's declared `u_kg` is not read. It is a table, not a label; see question Q6.
- **Ratio of medians, not median of ratios.** A household rarely sells one crop in two units. So there is no per-household pair to take a ratio of, which is what the food ladder's step 2 does. The two medians are independent cells.
- **Never pooled across `t`.** Each wave's prices judge only that wave. It is also never pooled across crops: there is no u-pooled rung. The food ladder's `unit` rung pools every item that shares a label. For crops that would price a `Botte` of millet with a `Botte` of onions.
- **Rungs.**
  1. `(t, j, state, u)`, where both the sale (`Condition_sold`) and the harvest (`condition`) record a state.
  2. Then `(t, j, u)`.
  3. Optionally `(t, region, j[, state], u)` first, if the caller resolves `region` onto the frame. This is the `shipped_factors` precedent: `harvest_kg_factors` "never re-enters `sample()` or `cluster_features()`".
  - Measured: on the screen the region rung changes little (155 names against 157). In the rung it matters for Mali 2014-15 (574 rows served against 218, a thin wave of 3.85 households per cluster) and little elsewhere (sections 6 and 7).
- **Implementation:** one function in `transformations.py`, for example `_price_implied_kg_factors(df)`.
  - `quantity_audit.survey_kg_price_factors` would then call it, so the screen and the rung cannot drift apart.
  - It must not go through `_kg_factor_series` / `conversion_to_kgs`, and must not feed `Value_sold` in as an `Expenditure`. That would merge it with the food ladder, which the ruling forbids. It would also bring the u-pooled rung and the food gates' different semantics with it.

## 2. Its evidence rows

- **Donors** are rows with:
  - `Quantity_sold > 0`;
  - `Value_sold > 0`;
  - a usable `Unit_sold`: not `Unknown`, `Manquant`, NA, blank or a currency label.
- **Farm-level rows.** The GH #1457 sale-only rows (Niger 2014-15 / 2021-22, Mali 2017-18 / 2021-22) carry no harvest quantity, so they are donors and never recipients. Other Farm-level rows DO carry a quantity and are recipients: Mali's (the red team counts 244, 159 and 221 by wave, F9) and Niger's 3E rows (33 judged by the screen).
- **Not evidence:**
  - **`Quantity_sold_kg`.** It is the survey's own arithmetic. On a sale in the harvest's unit, sold kg per unit equals harvest kg per unit on 939 of 1,134 Niger 2021-22 sales; see Niger CONTENTS.org. It would make the survey judge itself.
  - **`KgFactor_sold`** (Mali EAC-I's reported kg per SOLD unit) and **`s7fq13d`** (Mali 2017-18's reported kg per HARVEST unit, unwired). Both are survey reports, not prices. `s7fq13d` belongs in the `reported` layer itself (Q0). `KgFactor_sold` could feed a `survey_median`-style borrowing (Q2). Neither belongs in this estimator.
- **Window-mismatched sales.** Some donor rows price a window other than the quantity's:
  - **Niger 2011-12 section 3E (dry season).** `Quantity_sold` is the first cycle and `Value_sold`'s window is unstated. The rows are keyed `dry-season-sale-window`.
  - **Mali EAC-I tree sales.** The quantity is "en moyenne chaque mois" and the value is "le montant total". The rows are keyed `tree-sale-window`; there are 331 of them.

  A price per unit from such a row can be off by the number of cycles or months. Both are already identity derivations stamped on the row, so a data-driven exclusion is available: skip donors whose `Derivation` carries a registered window key. Measured:
  - With 3E in, a net 79 rainy 2011-12 rows lose their implied mass: 103 lost and 24 gained (1,978 rainy rows judged without 3E, 1,899 with; red team F7). **All 103 are one cell, `Oseille` / `Tiya` 2011-12.** A single 3E `Kg` sale moves that cell's baseline from 5 to 6 sales and its max/min spread from 4.0 to 12.0, past the gate. So it is a one-donor veto under the max/min statistic below 10 sales, not a broad difference in 3E prices.
  - On the screen, 2011-12 names 12 rows with 3E in against 9 without. 3 of the 12 are 3E rows themselves; one rainy row is named only with 3E donors, and one only without. **The measurement the ruling was made on excluded 3E both ways** (reports/sold-kg-1560, Part 2).
  - The tree sales do not touch the screen (Mali 2021-22 has no tree rows). They do touch the rung's EAC-I coverage, which was not separated out.

  This is question Q3.

## 3. Gates

These are the food ladder's numbers, as its own constants. The ruling keeps the designs apart, so a food-ladder change must not move crops in silence.

| gate | value | side | statistic |
|---|---|---|---|
| `min_reports` | 5 | sales in the unit | count |
| `min_baseline` | 5 | sales stating a mass | count |
| dispersion | 10 | **both** sides | `p90/p10` at n >= 10, `max/min` below |

- **Both sides are gated.** The food ladder gates only its baseline. The #1559 measurement the ruling was made on gated both, and a unit price whose own sales disagree by more than 10x is not a mass.
- **No tight exception** (the food ladder's 3-4 reports within 1.25x). Crop cells are thin, and the exception buys 0-0.7% coverage on food.
- **A plausibility ceiling is not proposed until ruled** (question Q5). `KG_FACTOR_MAX` (250 kg) is sized for a named container and would refuse carts, granaries and trucks. Mali's `Cart (donkey)` / `Charretée` cells are served here.
- **`survey_kg` outranks it on every row.** So in a wave with survey kilograms the rung serves only rows with none.
- **These gates are enough for a screen and not for a rung.** A screen asks "is this row 10x off?", and a thin baseline only narrows what it can see. A rung SERVES the number. Every Niger 2018-19 cell the rung would serve rests on exactly 5 `Kg` sales, and within one crop it inverts the sack sizes (section 6). Options, none measured:
  - a higher `min_baseline` for the rung (10 sales, or 5 distinct households);
  - a size-order check across one crop's units (`Gros` > `Moyen` > `Petit`);
  - agreement with the crop's stated sacks where the wave has any.

- **The gates count sale ROWS, not households** (red team F2, job 41278567).
  - **Screen:** 4 of its 17,346 judged rows sit on a cell with fewer than 5 households on either side, and none of the 157 named rows does. A 5-household gate names the same 157.
  - **Rung:** Niger 2018-19 roselle's 5 `Kg` sales come from **2 households**. A 5-household gate cuts that wave's servable rows from 161 to 23 (Niger 2011-12: 24 to 23). Mali's 5 / 7 / 5 `Kg` sales behind the 2017-18 cereal and groundnut cells are 5 / 7 / 5 distinct households, so the gate leaves Mali unchanged.
  - **Exact duplicate sale records** count separately today: rows with the same household, crop, unit, quantity and value. Niger has 107 (2011-12) and 126 (2018-19); Mali 58 (2014-15) and 19 (2018-19).
  - A household floor (with exact repeats removed first) would do the work in Niger 2018-19, as measured, and would not touch Mali 2017-18, whose cells sit at the 5-sale floor with 5 households.

  This is question Q8.

## 4. Where it sits in the ladder

Today the ladder is:

```
KG_FACTOR_LAYERS = ('survey_kg', 'reported', 'shipped', 'survey_median', 'metric', 'inferred', 'none')
```

Proposed:

```
KG_FACTOR_LAYERS = ('survey_kg', 'reported', 'shipped', 'survey_median', 'metric', 'inferred',
                    'price_implied', 'none')
```

- **Below every layer that is survey evidence or a stated mass.** Those are `survey_kg`, `reported`, `shipped`, `survey_median` and `metric`.
- **After `inferred`.** On a crop frame `inferred`'s price-ratio branch never runs: `_get_kg_factors` infers only when the frame has an `Expenditure` column, and `crop_production` has `Value_sold`. So `inferred` serves 0 rows on Niger and 0 on Mali (`kg_factor_sources`, cold build). Its non-zero content is the seeds and declarations, which `metric` already labels.
  - Placing the new layer after it changes no served row. It keeps `inferred` meaning "the shared machinery", so a frame that does carry `Expenditure` still resolves as it does today.
  - The alternative is to place it before `inferred`, or to retire `inferred` on crops. That is a relabelling question for the user, not a data change.
- **Never merged with the food ladder.** `FOOD_KG_FACTOR_LAYERS` is untouched, `food_kg_factors` does not call it, and it does not call `food_kg_factors`.

## 5. Disclosure

The food rungs' pattern, which `CLAUDE.md` exempts from `Derivation` by rule: a per-row source column of its own.

- **`KgFactorSource == 'price_implied'`**, in the `kg_factor_sources` partition.
- **Per-row columns:** `kg_price_implied` (what the layer offered, whether or not it won, like `kg_inferred`), `kg_price_rung`, `kg_price_n_unit`, `kg_price_n_kg`.
- **An `attrs['kg_price_implied']` record:**
  - rows served by wave, by rung, and state-blind;
  - cells offered and refused, by gate;
  - the donor exclusions applied (window keys).
- **A kwarg `price_implied=`** on `harvest_kg_factors` / `harvest_kg` / `yield_kg`. Its default is question Q1.
- **No warning.** The food rungs warn on nothing per rung, and the partition is the signal.
- **CONTENTS.org of each country it serves:** a paragraph on the share of kilograms it adds. For Mali 2017-18 that is most of the wave's new kilograms.

## 6. Coverage: where the rung would serve

**All coverage below is against today's ladder, without Q0.** With `s7fq13d` wired, `reported` would outrank the rung on Mali 2017-18's rows (section 7(d)).

Rows now on `KgFactorSource == 'none'` with `Quantity > 0`, on the national rungs. "kg added" is `Quantity x implied`. "kg served now" is what the ladder serves the wave today (`survey_kg` totals plus `Quantity x kg_per_unit`).

| country / wave | `none`, q > 0 | in a unit stating no mass | served | kg added | kg served now | region then national: served |
|---|---|---|---|---|---|---|
| Niger 2011-12 | 86 | 86 | 24 | 25,777 | 2,168,146 | 24 |
| Niger 2014-15 | 87 | 87 | 66 | 9,826 | 2,455,341 | 66 |
| Niger 2018-19 | 13,244 | 13,240 | **161** | 24,320 | 36,695 | 161 |
| Niger 2021-22 | 0 | 0 | 0 | 0 | 2,954,597 | 0 |
| Mali 2014-15 | 9,206 | 9,206 | 218 | 303,631 | 292,888 | 574 |
| Mali 2017-18 | 21,771 | 21,631 | **9,981** | **14,789,672** | **1,656,241** | 10,258 |
| Mali 2018-19 | 1,767 | 1,767 | 763 | 1,846,259 | 14,881,091 | 763 |
| Mali 2021-22 | 571 | 571 | 12 | 3,667 | 7,062,291 | 12 |

The masses it would serve, per unit (median kg per unit over served rows):

| country / wave | unit: median kg per unit (rows) |
|---|---|
| Niger 2011-12 | `Planche` 36.7 (14), `Botte` 26.3 (3), `Panier` 16.4 (2), `Unité` 7.5 (4), `Tiya` 2.4 (1) |
| Niger 2014-15 | `Botte` 10.2 (34), `Tiya` 2.5 (30), `Panier` 20.0 (2) |
| Niger 2018-19 | `Oseille`: `Tiya` 1.33 (67), `Gros sac` **21.0** (52), `Sac Moyen` **27.8** (19); `Tomate fraîche`: `Gros sac` 45.1 (11), `Autre` 22.6 (12). Every cell on exactly 5 `Kg` sales |
| Mali 2014-15 | `Sac` 100.0 (164), `Charretée` **255.3** (54) |
| Mali 2017-18 | `Sac` 105.0 (9,052; max 120), `Charretée` 214.2 (330), `Gerbe` 12.5 (520), `Autre` 10.0 (79) |
| Mali 2018-19 | `Cart (donkey)` **317.3** (570; 309 rows over 250), `Gerbe` 15.4 (128), `Baignoire` 20.0 (65) |
| Mali 2021-22 | `Baignoire` 33.3 (12) |

Notes on the tables:

- **Mali 2017-18 is the rung's main use: a ninefold change in the wave's total, but a change of COVERAGE, not of level.** The wave serves 1.66M kg today, on 1,761 `metric` rows of 24,921, and the rung would add 14.8M kg. The share of millet, maize and groundnut household-crops with positive kilograms goes from about 5% to 65-85%, and per-household medians land in the range of 2018-19 and 2021-22 (red team F9).
  - **But most of it is waiting survey evidence** (red team F1; joined to served rows here, job 41279383).
    - `s7fq13d` ("Equivalent en Kg") is on the same s7f harvest line as every served 2017-18 seasonal row: 24,449 joined of 24,449, and positive on 23,205 of the 23,348 positive-quantity rows.
    - It covers all 9,981 rung rows.
    - Wired as `KgFactor`, the wave serves 32.8M kg (`reported` 21,039 rows and 25.9M kg, `survey_median` 2,345 rows and 7.2M kg, `none` 1,535), against the rung's 16.4M. The `survey_median` rows include the 2,167 the reported-factor screen refuses, all above 250 kg and mostly carts: re-served at the median of the accepted factors for their unit, about 6.0M kg, where their own factors would give 82.2M (red team; Q5).
    - Median kilograms per household-crop would be: `Mil` 1,500, `Maïs` 1,000, `Sorgho` 1,000, `Riz` 1,260, `Arachide` 500, `Coton` 1,125.
    - The rung would then serve only rows `s7fq13d` leaves on `none`, at most 1,535 in this wave (not simulated).
  - That is nearly all the unsized `Sac`:
    - `Mil`: 2,453 rows at 120 kg, 5.0M kg (59 `Sac` sales, 5 `Kg` sales behind it).
    - `Maïs`: 2,443 rows at 105 kg, 4.3M kg (39 and 7).
    - `Arachide`: 3,777 rows at 60 kg, 1.6M kg (75 and 5).
    - `Coton`: 97 rows at 100 kg, 1.8M kg (11 and 101).
    - `Sésame`: 282 rows.
  - **Its baselines sit at the floor.** Each cereal and groundnut cell rests on 5-7 `Kg` sales, the same thinness as Niger 2018-19 below, because EAC-I's sold sacks state no size. Only cotton has a deep baseline (101 sales).
  - **It is concentrated.** The top 10 rows carry 16.8% of the 14.8M kg, and the top 1% (99 rows) 27.3%.
  - **One row Site Q already names as an implausible `Quantity`** (16 named in the wave) would be served 857k kg, 5.8% of the addition. Whether a rung should serve kilograms to such a row is Q9.
  - The cross-check is good where it can be made. EAC-I field sales report a kg per sold unit (`KgFactor_sold`), and against the price-implied mass, without cotton, they read 0.95 at the median: `Maïs` 0.95, `Mil` 0.83, `Sésame` 1.00 (section 7(c)). The exception is groundnut, which reads 1.50, where state confounds a state-blind wave.
  - It is still a change of that size in a published wave, which is why the default (Q1) matters.
- **Niger 2018-19, the rows #1559 was about, stays almost entirely unserved.**
  - Of its 13,240 rows, 5,666 are `Botte`, 4,100 `Tiya`, 1,454 `Gros sac`, 968 `Autre` and 866 `Sac Moyen`.
  - The wave has 21 `Kg` sales of 3,226 and its sacks state no size, so no `(t, j)` per-kilogram baseline reaches 5 sales in most crops. This confirms, by simulation, the prior report's inference from sale counts.
  - **The 161 rows it would serve are two crops, every cell on a baseline of exactly 5 `Kg` sales: roselle's from 2 households, fresh tomato's from 5.** A 5-household gate keeps the 23 tomato rows.
    - Within roselle (`Oseille`), a `Gros sac` reads 21.0 kg (37 sales in the unit) and a `Sac Moyen` 27.8 kg (6 sales): a big sack lighter than a medium one.
    - Its `Tiya` reads 1.33 kg (25 sales).
    - Fresh tomato's `Gros sac` reads 45 kg.
  - Whether those masses are wrong is not established: roselle is a light crop. **The size inversion on a floor-thin baseline is the finding:** the screen's gates admit a baseline thinner than a rung may want to serve from (Q8).
  - A stricter baseline floor (for example 10 sales, or 5 distinct households) or a size-order check across a crop's units are options. Neither was measured. This is question Q8.
  - Cross-wave borrowing (2021-22's estimate per Tiya applied to 2018-19) would serve more of the wave; no rung does it here (Q4).
- **Carts exceed `KG_FACTOR_MAX`** (250 kg): Mali 2014-15 `Charretée` 255 kg (54 rows) and 2018-19 `Cart (donkey)` 317 kg (309 of 570 rows). A ceiling sized for named containers would refuse them (Q5).
- **Selection.** The households that sell a crop are the minority whose prices set the factor.
  - Survey-kg rows from sellers: Niger 23.6% / 22.8% / 21.4%, Mali 21.6%. This reproduces the ruling's 21-24%.
  - Of the rows the rung would serve, the share in a household-crop that sold anything is: Niger 66.7% / 7.6% / 56.5%, and Mali 23.9% / 3.8% / 15.2% / 16.7%.
  - Within (wave, crop, unit), sellers' survey kg per unit matched non-sellers' in the prior measurement: ratio of medians p10 0.82, p50 1.00, p90 1.09 over 54 Niger cells (job 41249490). That was not re-measured.

## 7. Agreement, against four answer keys

**(a) Stated-mass sales held out of the baseline.**

- Method: each stated sack is relabelled mass-less, removed from the baseline, and inferred from prices. Then implied / stated is compared with the label.
- Recipients carry their own sale's state, so the state rung applies where it can.

| country | held out | stated | sales inferred | median implied/stated | p10-p90 | within 2x (rows) | cells: median, within 2x |
|---|---|---|---|---|---|---|---|
| Niger | `Sac de 100 kg` | 100 | 682 of 885 | **0.80** | 0.60-1.00 | 96.0% | 0.83, 14 of 14 |
| Niger | `Sac de 50 kg` | 50 | 302 of 507 | **1.25** | 1.03-1.53 | 96.4% | 1.09, 11 of 11 |
| Mali | `Sac (100 Kg)` | 100 | 1,447 of 1,518 | **0.80** | 0.51-1.05 | 96.3% | 0.83, 27 of 29 |
| Mali | `Sac (50 Kg)` | 50 | 139 of 233 | **1.00** | 0.80-1.44 | 100% | 1.03, 12 of 12 |
| Mali | `Sac (25 Kg)` | 25 | 12 of 32 | 1.94 | 1.04-2.84 | 50% | 1.04, 2 of 3 |
| Mali | `Tonne` | 1,000 | 231 of 244 | 1.00 | 1.00-1.02 | 100% | 1.00, 5 of 5 |

- **The tonne is not evidence.** 222 of the 231 inferred tonne sales are cotton, which sells at what looks like one fixed price (p10-p90 of implied over stated 1.00-1.02), so a tonne priced against cotton `Kg` sales returns its own label. Without cotton: 9 sales, 0.78.
- **By crop** (100 kg sack): Niger groundnut 0.68, cowpea 0.86, millet 0.91, sesame 0.99, onion 1.00. Mali groundnut 0.56, rice 0.60, maize 0.80, millet 0.81, sesame 0.90, sorghum 1.04, onion 1.15. Groundnut and rice read lightest; for groundnut, that fits the state (in-shell against shelled) that the sale and the baseline need not share.

- **The sign follows lot size.**
  - With the 100 kg sack held out, the baseline is `Kg` plus 50 kg sacks, and the big sack reads light.
  - With the 50 kg sack held out, the baseline is dominated by 100 kg sacks, and it reads heavy.
- That is what a per-kilogram price falling with lot size produces: a bulk discount, or a small-lot premium in `Kg` sales.
- The 100 kg sack's 0.80 also fits a sack holding less than its label: the survey's own kilograms read a 100 kg sack lighter (Niger CONTENTS.org, GH #1436). The 50 kg sack's 1.25 does not, so the lot-size effect is real; how much of the 0.80 each explains cannot be separated here.
- Either way the estimator's median error on a known container is about ±20-25%, with 96-100% of rows within 2x where cells are not thin.

**(b) The survey's own kilograms**, where both exist. This is the screen's comparison: survey / implied, on rows in units stating no mass.

| wave | judged rows | median | p10-p90 | within 1.25x | within 2x | < 0.1x | > 10x |
|---|---|---|---|---|---|---|---|
| Niger 2011-12 | 1,932 | 1.14 | 0.75-1.50 | 64.9% | 91.7% | 6 | 6 |
| Niger 2014-15 | 6,394 | 1.13 | 0.77-1.73 | 60.4% | 88.0% | 26 | 89 |
| Niger 2021-22 | 9,020 | 1.25 | 0.88-1.85 | 53.1% | 89.0% | 2 | 28 |
| Mali 2021-22 | 1 | 0.30 | | | | 0 | 0 |

- The ruling's "survey kg 1.1-1.4x the price-implied value" holds: 1.13-1.25 at the median, every wave above 1.
- **The offset is not explained by section 7(a).** A bulk discount would make a unit smaller than the baseline's lots (a Tiya against sacks) read HEAVY, so survey / implied would fall below 1. It sits above 1. So either the survey's conversions run heavy (2.5 kg per Tiya, an estimate), or the Tiya and Botte sales are themselves bulk lots. Not established.
- Niger 2021-22's survey kilograms are an estimate: "Estimation", 2.5 per Tiya on 65% of rows, keyed `survey-kg-estimate`. So (b) is two estimates, not an estimate against a weighing.

**(c) Mali EAC-I's reported kg per sold unit, `KgFactor_sold`** (s3aq23c / s7gq21d, field sales).

- This is an answer key the instrument supplies, in exactly the waves and units where the rung would serve most.

| wave | sales with a factor | in a unit stating no mass | inferred | median reported/implied | p10-p90 | within 2x |
|---|---|---|---|---|---|---|
| 2014-15 | 620 | 534 | 48 | **1.00** | 0.80-1.18 | 97.9% |
| 2017-18 | 661 | 506 | 264 | **0.95** | 0.83-1.67 | 95.8% |

By unit:

| unit | 2014-15 (n 28) | 2017-18 (n 212) |
|---|---|---|
| `Sac` | 1.00 | 0.95 |
| `Charretée` | 0.98 | 1.17 |

- **Counts.** 48 and 264 count sale rows with a factor and a positive sold quantity. Requiring a positive value as well (a priced sale), as the red team did, gives 39 and 261 (red team F9). The medians are the same.
- **Without cotton** (72 of the 312 sales; cotton sells at what looks like one fixed price): median 0.95, n 240, 97.5% within 2x.
- **By crop, both waves:**
  - `Maïs` 0.95 (39 sales);
  - `Mil` 0.83 (64);
  - `Sésame` 1.00 (41);
  - `Coton` 1.00 (72);
  - `Arachide` **1.50** (83).
- **Groundnut is the exception**, and it is the state again: EAC-I records no state, so a state-blind cell mixes in-shell and shelled sales.

**(d) The instrument's own kg per harvest unit, `s7fq13d` (Mali 2017-18, unwired): the best key** (red team F1; joined row by row here, job 41279383).
- **Join.** Keys rebuilt as the wave script builds them: `eaci17_i(grappe, exploitation)` (2017-18's `updated_ids` map is empty), `plot_id = s7fq01_s7fq02`, crop through `harmonize_food`.
  - 24,449 of 24,449 served seasonal rows join.
  - Quantity is equal to `s7fq13a` on all 23,348 positive rows.
  - `s7fq13d` is positive on 23,205 of them and is 1 on all 1,688 `Kg` rows.
- **Against the rung, on the rung's 9,981 rows:** rung / `s7fq13d` median 1.05, p10-p90 0.60-1.20, 94.2% within 2x.

| crop / unit | rows | rung kg per unit | `s7fq13d` | rung / `s7fq13d` |
|---|---|---|---|---|
| `Arachide` / `Sac` | 3,777 | 60 | 100 | **0.60** |
| `Mil` / `Sac` | 2,453 | 120 | 100 | **1.20** |
| `Maïs` / `Sac` | 2,443 | 105 | 100 | 1.05 |
| `Mil` / `Gerbe` | 520 | 12.5 | 25 | 0.50 |
| `Coton` / `Charretée` | 330 | 214 | 250 | 0.86 |
| `Sésame` / `Sac` | 282 | 94 | 100 | 0.94 |
| `Coton` / `Sac` | 97 | 100 | 100 | 1.00 |
| `Arachide` / `Autre` | 79 | 10 | 300 | 0.03 |

- **Wired, not all of it is served at its own value** (red team, job 41280001). 2,167 rows carry a factor above `KG_FACTOR_MAX` (`Charretée` 1,576, `Autre` 534, `Sac` 31, `Grenier` 21). The reported-factor screen refuses them, and `survey_median` re-serves each at the median of the accepted factors for its unit: about 6.0M kg, against 82.2M at their own factors. So 1,576 of the 3,729 cart rows would be capped at the median of other carts. The red team's re-join, without the crop map, counts 2,162 such rows and 33.1M kg in all; the gap is 76 rows ambiguous on household, plot and quantity.
- **It is a conversion, not a weighing.** `Sac` is 100 on 95% of millet rows (red team). So (d) tests the rung against the instrument's nominal masses, as (a) tests it against labels.
- **The keys disagree in sign for millet.**
  - The price-implied millet `Sac` is 1.20x `s7fq13d`.
  - The held-out 100 kg sack is 0.81x its label, and `KgFactor_sold` reads 0.83 for millet.
  - These are not contradictory measurements of one mass. Relative to `Kg` sales, prices value a labelled 100 kg sack at about 80 kg, and an unsized `Sac` at about 120. A bigger unsized sack, or a quantity-price gradient, would do that. Neither is established.
- **2014-15's sibling `s3aq08c` is not usable as is** (red team F1). It is positive on 9,502 of 9,571 rows but mixes factors and totals: it equals the quantity on 10% of `Kg` rows, and on those 511 rows quantity x factor sums to 45.5M kg.

## 8. Measured caveats

- **Product state.**
  - The price gap is real:
    - Niger 2021-22 groundnut is 2.35x per Tiya (1,000 against 425 FCFA, n 62/61, prior job 41249490).
    - It is 4.3x per kilogram in `Kg` sales (n 6/6, thin; this job).
    - Mali 2021-22 groundnut is 1.48x per kg (n 35/14); cotton is 1.00-1.02x.
  - Where a state is recorded on both sides, the state rung uses it.
    - On the screen it removes 2 low names in Niger 2021-22 (state-blind: 4).
    - It judges 4,187 of 9,020 rows on the state rung.
  - **Which waves record a state.** The ruling's "only 2021-22 records it" is exact for the waves with survey kilograms.
    - The EHCVM 2018-19 waves record a state as well: Niger 3,226 sales with a state and 13,276 harvest rows; Mali 2,102 and 7,385.
    - So the rung's state rung reaches Niger 2018-19 (22 of its 161 rows) and Mali 2018-19 (45 of 763).
    - No ECVMA or EAC-I wave records a state: Niger 2011-12, 2014-15 and Mali 2014-15, 2017-18. So **Mali 2017-18, the rung's main use, is entirely state-blind.** Mali 2017-18 does record a state (`s7fq13b`), but it is unread: its codes invert the EHCVM table (Mali CONTENTS.org).
  - The survey's own kilograms do not see the state (Niger 2021-22: 2.5 per Tiya and 100 per 100 kg sack in both states). So where both sides record a state, the price-implied mass can separate states and the survey's kilograms cannot.
- **Selection:** see section 6. The prices are the sellers'. Price-implied factors therefore describe the units sellers use, applied to everyone. The prior's seller vs non-seller check found no difference in survey kg per unit.
- **Seasonality:** not re-measured.
  - Mali per-kilogram prices in `Kg` sales vary across interview months within (wave, crop) by max/min 1.0-2.8, median about 1.3, over 12 cells (prior job 41249490).
  - Seasonality biases the ratio only where unit sales and `Kg` sales differ in timing, and the cells are too thin to separate that.
  - A month rung would cut most cells below 5 sales.
- **Survey 1.1-1.4x price-implied:** reproduced in section 7(b): 1.13-1.25 at the median. Its cause is not established, and section 7(a)'s lot-size lean has the wrong sign to explain it.
- **The stated-mass baseline inherits the sacks' under-weight.** The survey reads a 100 kg sack at a median of about 87 kg in 3E, and a p10 of 30 kg for unshelled sacks in 2021-22. Reading sacks at nominal mass lowers the baseline price per kilogram and so raises every implied mass. Section 7(a)'s 0.80 for a held-out 100 kg sack is consistent with that under-weight as well as with a bulk discount; the two are not separated here.

## 9. Cache cost of landing it (measured, not assumed)

- **Method:** no-op probe edits in a job-local clone of the branch head, then the batch hash census (`tools/hash_census.py`, 38 countries), compared with the head's own census. Burst jobs 41275939 (probes 1-3 on `4aefeec25`) and 41276345 (probes 1-4 on `7d27171ab`).

| probe | edit | `btf` (42) | `fwimp` (38) | tables (555) | waves (2,321) |
|---|---|---|---|---|---|
| P1 | a statement inside `harvest_kg_factors` | 0 | 0 | 0 | 0 |
| P2 | `'price_implied'` added to `KG_FACTOR_LAYERS` | 0 | 0 | 0 | 0 |
| P3 | a new, unreferenced helper in `transformations.py` | 0 | 0 | 0 | 0 |
| P4 | POSITIVE CONTROL: a statement inside `food_kg_factors` | 0 | **3** (Ethiopia, GhanaLSS, Uganda) | **75** (25 / 22 / 28) | **503** (125 / 154 / 224) |

- No country or wave script imports `harvest_kg`, `harvest_kg_factors` or `yield_kg`. No tagged build transform reaches them, and the derived food tables' importers (Ethiopia, GhanaLSS and Uganda via `food_quantities_from_acquired`, GH #1509) do not reach them either.
- **The condition that keeps it free:** the crop estimator must not be called from, or share a helper with, `food_kg_factors` / `food_quantities_from_acquired`. P4 shows that closure is live. A shared helper there would make every rung edit a re-warm of Ethiopia, GhanaLSS and Uganda.
- The screen's move of the estimator into `transformations.py` (section 1) is covered by P3 and P1.

The P4 row reproduces exactly the cost `CLAUDE.md` records for a kg-factor edit since GH #1509 ("75 of 555 `_table_cache_hash` ... 503 of 2,321 `Wave._input_hash` ... 3 of 38 `framework_imports_fingerprint`"). So the probe sees a live closure, and P1-P3's zeros are measurements, not a blind probe.

## 10. For the user to rule on

**A separate ruling, to take first.**

0. **Q0, wire Mali 2017-18's `s7fq13d` as `KgFactor`?** It is the instrument's "Equivalent en Kg" per harvest unit. It joins to every served seasonal row, is positive on 23,205 of 23,348, and is 1 on every `Kg` row.
   - Through today's ladder it would serve Mali 2017-18 32.8M kg (`reported` 25.9M, `survey_median` 7.2M), against 1.66M now. Household-crops with kilograms, all crops, would go from 7.6% to 94.6%.
   - **The 2,167 rows the reported-factor screen refuses are not left unserved.** All are above the 250 kg ceiling (`Charretée` 1,576, `Autre` 534, `Sac` 31, `Grenier` 21). `survey_median` re-serves each at the median of the accepted factors for its unit: about 6.0M kg, against 82.2M at their own factors. Whether the ceiling should apply to the survey's own factor is Q5.
   - It looks like a nominal conversion (`Sac` = 100 on 95% of millet rows), as Niger 2021-22's survey kilograms are. It is still the instrument's answer on the row, which is what `KgFactor` holds (as UNPS's `a5?q6d` does for Uganda). Whether it also carries a `Derivation` key is part of the ruling.
   - **Not ruled anywhere.** The `CONTENTS.org` lines on 2017-18's state bridge do not cover it.
   - 2014-15's `s3aq08c` mixes factors and totals and would need its own rule.
   - Expect one Mali re-warm (not measured).
   - Q1, Q2 and Q8 below read differently depending on Q0.

**On the rung.**

1. **Q1, default.** Should `price_implied` be on by default in `harvest_kg` / `yield_kg`, or opt-in?
   - **Without Q0**, on by default moves Mali 2017-18 by 14.8M kg, 10.9M of it from cells resting on 5-7 `Kg` sales.
   - **With Q0**, `reported` outranks the rung on those rows, and the rung serves at most the 1,535 rows `s7fq13d` leaves on `none` in that wave, plus Mali 2014-15 / 2018-19 / 2021-22 (218 / 763 / 12) and Niger (24 / 66 / 161).
   - Opt-in keeps every published number where it is.
2. **Q2, reported factors as a `survey_median` source.**
   - Today `survey_median` borrows the median reported `KgFactor` of the same `(u, condition)` in the wave. With Q0 that already fills `s7fq13d`'s gaps in 2017-18: 2,345 rows in the simulation, 2,167 of them the rows the reported-factor screen refuses (Q5).
   - The remaining question is the sold side. EAC-I field sales report kg per SOLD unit (`KgFactor_sold`; 48 / 264 sale rows with a factor, 39 / 261 priced). Should their median also fill harvest rows in the same `(t, j, u)`, above `price_implied`? They agree with prices at 0.95 without cotton.
   - Outside this ruling's words.
3. **Q3, window-mismatched donors.** Exclude sales whose `Derivation` carries a registered window key (`dry-season-sale-window`, `tree-sale-window`) from the estimator? On the screen, Niger 3E's donors veto one 2011-12 cell (section 2).
4. **Q4, cross-wave borrowing.** Niger 2018-19 stays unserved without it. Never pooling across `t` is the screen's and Site Q's rule, and the reason is currency and price levels. A mass per unit is not a price, but the estimator's inputs are.
5. **Q5, a ceiling.** `KG_FACTOR_MAX` (250 kg) would refuse carts and granaries, for the rung and, under Q0, for the survey's own `s7fq13d`. There it refuses 2,167 rows (`Charretée` 1,576, `Autre` 534, `Sac` 31, `Grenier` 21), and `survey_median` re-serves them at about 6.0M kg, where their own factors say 82.2M. A unit-class ceiling (carts, granaries, trucks), none, or as now?
6. **Q6, declared masses in the baseline.** `u_kg` declarations are not read today (labels only, per "sacks at stated mass"). Read them?
7. **Q7, region.** Accept a caller-resolved `region` level as the first rung?
8. **Q8, a floor counted in households.** The screen's gates count sale ROWS.
   - Niger 2018-19's roselle cells rest on 5 `Kg` sales from **2 households**, and within roselle a `Gros sac` (21 kg) reads lighter than a `Sac Moyen` (28 kg).
   - A 5-household gate leaves 23 of that wave's 161 rows (measured, red team F2). It leaves Mali unchanged, whose 5 / 7 / 5 sales are 5 / 7 / 5 households.
   - Exact duplicate sale records count separately today: Niger 107 + 126, Mali 58 + 19.
   - For the rung, should the floor count distinct households, after removing exact repeats? And at what number? A floor of 10 would refuse Mali 2017-18's main cells as well, which matters only without Q0.
   - Other options, not measured: a size-order check across a crop's units, or agreement with the wave's stated sacks.
9. **Q9, rows Site Q names.** In Mali 2017-18, one row whose `Quantity` Site Q already names as implausible would be served 857k kg, 5.8% of the wave's addition. Should the rung refuse a row Site Q names? That would make a read-path report decide a served value. Or should it serve the row and say so in its record?
10. **Q10, groundnut.** In the state-blind EAC-I waves the price-implied groundnut mass is off on every key: 0.60 against `s7fq13d`, 1.50 against `KgFactor_sold`, 0.56-0.68 held out. Should a crop whose state the wave does not record be refused the rung?
