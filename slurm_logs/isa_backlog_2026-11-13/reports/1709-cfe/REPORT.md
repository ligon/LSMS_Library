# GH #1709 report: is CFE's w = −log λ robust to cross-wave item response? (Togo 2018 vs 2021-22)

**Short answer.** Within a market-period cell, w barely moves when the producer's factor is inverted, but only on a basis where the factor is a clean multiplier. The default basis (purchased, which for Togo is the same as `basis='total'`) is not such a basis.

w does not give a usable regional poverty change. Its cross-wave level moves about +30 points nationally in every arm, which is far outside a placebo noise floor. It moves +6 to +16 points in Senegal and Niger too, where there is no factor and the official change is near zero. And as soon as levels are read from a_jtm net of unit values, the factor comes straight back.

Stream 1709-cfe, read-only research worker. Base development `f138a3d61c203f892148592675a7ef72e160a322`. No commits, no tracked-file edits, no sub-agents, no GitHub writes.

## 1. Commits, jobs, rule breaches, private data
- **Commits:** none, so there is no branch head and no hash census (nothing changed). No tests were run; this is analysis only.
- **Jobs:** all on co_carleton `savio4_htc`, 4–8 cores, ≤64 GB. Nothing ran on fc_jevons. Every job identity-asserted f138a3d61 and a private root.
  - 41274087: arm B extraction and timing fit (25 s, 2.1 GB).
  - 41274247: arm A extraction (resumed), stage 1b, 112 fits.
  - 41274269: item counts in the other EHCVM countries.
  - 41274366: stage 3 analysis.
  - 41274424: level separations.
  - 41274489: diagnostics.
  - 41274521: missing foods.
  - 41274800: 24 control fits.
  - 41274919 / 41274920: Senegal and Niger negative controls.
  - Failed and fixed: 41274086 (import bug, after its frames were saved), 41274216 (parquet write of mixed-type labels), 41274356 (pandas groupby ambiguity).
- **Rule breach to report:** three stray system `python3` calls ran on n0035, inside shell commands: `python3 -` with an empty heredoc, `python3 -c "print(1)"` and `python3 -V`. None imported anything or read data, but COMMON-RULES forbids them.
- **Private data, needs your go-ahead to clean** (~286 MB, all under `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/private/1709-cfe/`):
  - four `KEEP_DATA` data roots: `job-41274086`, `job-41274087`, `job-41274216`, `job-41274247`;
  - household-level frames in `A/`, `B/` and `other/` (Senegal and Niger);
  - the fits, with household w, in `fits/`.
- **Where things are:** scripts in `reports/1709-cfe/scripts/`, logs in `reports/1709-cfe/logs/`, aggregates only in `reports/1709-cfe/out/stage{1,1b,3,3b,3c,3d,3e,4,5}/`.

## 2. Togo idiosyncrasies that bear on this (from CONTENTS.org and derivations.yml)
- **The factor.** Only `s07bq03a` carries it: f = 1.115 × {Centrale 1.5, Savanes 1.2, Plateaux 1.1, Kara 0.9, Maritime 0.9, Grand Lomé 0.82}. A Grand Lomé row with own production or a gift carries 1.2 instead.
  - It is divided out under three keys (`-regional-factor-divided-out`, `-floored-count-recovered`, `-floored-count-withheld`) plus a deletion key, `-purchase-removed-by-division` (25,234 rows).
  - **Confirmed here:** filed ÷ recovered on the divided key's inputs is exactly f by region, and exactly 1.2 on Lomé own/gift rows.
- **Counts (`unité`).** They were floored after scaling. Outside Lomé they are recovered exactly. In Lomé they are withheld: 3,906 NA purchased rows, 607 of them filed as 0.
- **Lomé label.** Region code 6 is `Lomé commune` in 2018 and `Grand Lomé` in 2021-22, on the same 98 grappes. CFE cells are (t, m), so this does not matter there; I merged the two for the regional comparisons.
- **No panel link.** 2021-22 ids carry an `E2_` prefix, so the waves are two cross-sections. A vs B compares the same 2021-22 households.
- **No value on produced or in-kind rows.** Produced and in-kind `Expenditure` is NA (the gift value `depan(Don)` is an imputation and is not served). So **`basis='total'` equals `basis='purchased'` for Togo**, which I measured (identical index and values; 0 non-purchased rows carry a value). From `ehcvm.py`, the same should hold for every EHCVM country, but I measured only Togo.
- **Unit values.** `Expenditure` uses the last purchase's unit value, filled only within 30 days. `Expenditure` is NA on 17,536 of 83,275 purchased rows in A and 38,092 of 107,841 in B.
- **Goods.** 2021-22 food labels were split or added ("a cross-wave demand system on j must bundle these itself"). I tested this with `alltm=True`.

## 3. Method
- **Arm A:** f138a3d61 as served.
- **Arm B:** the same tree with Togo 2021-22's `food_acquired` hook swapped for the pre-#1697 `_shared` hook (i.e. the filed `s07bq03a`). This was done in-process, in its own private root.
  - Gates passed: 2021-22 purchased rows / Expenditure are 107,841 / 34,464,072 for B and 83,275 / 29,435,020.05 for A, as CONTENTS records.
  - 2018 frames and household characteristics are identical across arms.
- **B rebuilt through `derivation_inputs`** (the 7-day key's inputs as filed, plus the registered `recover_s07bq03a` / `derive_ehcvm_purchase_value`).
  - My reconstruction of A matches served A on all 64,719 2021-22 (t, i, j) cells exactly.
  - The fit on the derivation_inputs-built B (Bdi) gives w identical to the B fit (correlation 1.0, difference 0).
- **Bases:**
  - *purchased* (equal to *total*);
  - *valued*: `basis='total', valuation=('own_price','median_price')`;
  - *gross*: `s07bq03a` × the household's own last-purchase unit value, from `derivation_inputs`. On this basis the factor is an exact multiplier, apart from the counts and the Lomé split.
- **Pipeline:** `food_expenditures(market='Region', …)` and `household_characteristics(market='Region')`, then `prepare_demand_inputs`, then `cfe.Regression(alltm=False)`. Each fit takes 10–43 s and at most 2.8 GB.
- **Drop rates** come from source rows with q3 > 0 per household and match the doc search's T-TGO-15 exactly: Centrale 21.52 → 16.32 (p = 0.242), Plateaux 0.102, Savanes 0.078, Maritime 0.023, Kara 0.005, Lomé 0.
- **Seeds:**
  - C1–10 and D1–10 (random drops): `default_rng(seed)`.
  - Cm1–5 and Dm1–5 (each household's smallest items dropped, k ~ Binomial(n, p)): `default_rng(seed)`.
  - P1–5 (pseudo-wave placebo): `default_rng(1000 + seed)`.
  - Grappe bootstrap: seed 1, 500 draws, w held fixed.
- **Controls:**
  - E: Centrale 2021-22 multiplied by exactly 1.6725 on every good.
  - G1 / G2: a genuine β-proportional change, y_j += 0.5·β_j, on Centrale 2021-22 / on every 2021-22 cell.
  - Br: B restricted to the (i, t, m, j) that A also has.
  - P: 2018 split at random into two pseudo-waves, so a 2018-only noise floor.

**Poverty line.** In each fit, the person-weighted (sample weight × household size) quantile of 2018 w at the official 2018 national rate, held fixed for 2021-22.
- I reproduced the official rate myself from `ehcvm_welfare_tgo2018` (Data1) and `_tgo2021`, with pcexp < zref: national 45.53 → 43.78, **Centrale 47.41 → 50.32 = +2.91**.
- That matches the World Bank Togo Poverty & Equity Brief: "the Central region … increase of almost 3 percentage points, reaching 50.3 percent in 2021".

**Price and level separation.**
- **S1 (CFE's own).** `get_w` fits Mpi(y − γ̂d). Anything common to every good of a (t, m) cell goes to pi, so the cross-cell level of w comes only from the β-direction of the cell × good means. Relative prices go to Ar. No unit values are used.
- **S3.** G_tmj − c·ℓ_tmj = α_j + β_j·W_tm, with no free uniform term. ℓ is the log median last-purchase unit value on (j, u) present in both waves. Variants: c = 1 (S3u), c = 1 − β (S3f), c = 0 (S3n).

**Benchmark.** The same line rule applied to log food expenditure per person, with 2021-22 divided by 1.115. That is CPI-sized; Togo's own zref ratio is 1.079 and was not used. The benchmark is food-only with no spatial deflator, so read only its contrasts.

## 4. Is w invariant between A and B? (2021-22 cells unless stated)

| basis | B/A per (i, j) item | w correlation by region | mean B − A, in 2018 sd of w | S1 Centrale change, A vs B |
|---|---|---|---|---|
| gross | 83–85% exactly at f (Lomé: 95% at 0.9143) | 0.990–0.998 (Lomé 0.964) | −0.02 to +0.06 (Lomé 2018 +0.16, a spillover) | +35.3 vs +31.7 |
| valued | at f on only 52–67% of items, at 1 on 30–40% | Centrale 0.936 | Centrale +0.62, Savanes +0.35 | +37.1 vs +5.4 |
| purchased (= total) | 82–84% at f | 0.79–0.94 | up to ±0.97 | +29.7 vs −35.1 |

- **Gross:** close to invariant. The regional S1 changes under A and B differ by at most 3.6 points, inside the placebo's regional sd of 2–7. Within-region quintile agreement is 0.88–0.94, and 0.75 in Lomé.
- **Valued:** not a multiplicative shift. Own-produced and gifted components are valued at the same price in both arms, so the factor reaches only the purchased part.
- **Purchased:** the B fit breaks.
  - β correlates 0.14 with A, and B's top eigenvalue is 11.1 against 4.0.
  - Removing the 3,872 B-only rows (Br) restores β correlation 0.9997, |Δw̄| ≤ 0.05, and S1 Centrale +30.2 against A's +29.7.
  - Br removes both the residual slivers and the withheld Lomé counts. Gross B keeps the Lomé counts and is stable. So the slivers being the cause is an inference.
  - **Why purchased is not a clean shift:** the purchased quantity is the residual f·q3 − q4 − q5, not f·(q3 − q4 − q5). The B/A ratio is therefore at least f, without bound as A's residual goes to 0. Where all consumption was own production or gifts, the residual makes a purchase out of nothing. In Kara and Maritime (f = 1.0035) these are 0.35% slivers (685 B-only items in Kara, 0.02% of value) whose logs are extreme low outliers.

**Where the factor lands.**
- E control: Δa(Centrale 2021-22) = 0.47 of log 1.6725 = 0.514. Every other cell moves −0.03 to −0.045, a leak through γ̂'s pooled constant. Δw̄ ≤ 0.017 and w correlation ≥ 0.9993. A uniform region-wave scalar is absorbed entirely.
- G1: Δw̄(Centrale 2021-22) = +0.467, others −0.03. G2: 2021-22 cells +0.24, 2018 cells −0.24. A genuine β-proportional change is recovered (about 0.47–0.48 of 0.5) and moves the S1 headcount; G2 brings the national change from +29.8 to +2.8.
- So a_jtm absorbs exactly the part of any change that is common to all goods. β's uniform share is 0.89–0.90, which bears on precision: it does not mean 89% of a genuine change is lost.

## 5. Item drops (C pooled; D = 2018 alone, placebo)
- **Households:** w stays highly correlated with the reference fit, 0.94–0.99 (lowest in Centrale).
- **2018 levels under C, same households:**
  - purchased: 2018 w is +0.16 to +0.33 sd higher in every region, including Lomé, which had no drop;
  - gross: +0.03 to +0.15 sd;
  - valued: ≤ 0.05 sd.
  - So the wave that loses items looks richer. MCAR item loss therefore does not produce S1's rise: equalising item response raises the national S1 change (purchased +29.4 → +36.7; gross +29.8 → +33.0 ± 3.6; valued +34.9 → +36.0). Centrale: purchased +43.7 ± 2.5 against +29.7; gross +38.6 ± 5.5 against +35.3; valued +36.6 against +37.1.
- **The sum is mechanically sensitive, as expected.** C − A national −3.7 to −5.5; Centrale −10.9 (gross), −11.7 (purchased), −17.7 (valued). With smallest-items drops (Cm), Centrale moves only −2 to −3.
- **D placebo (gross):** regional S1 shifts of Centrale −4.1 ± 2.6, Kara −5.3, Lomé +5.7, Savanes −7.2; the national figure is fixed by construction. For Centrale this is CFE's own sample selection (households retained 724 → 649; the same households move only +0.03 sd).
- **P placebo noise floor (gross S1):** national +1.3 ± 2.7, regional sd 1.8–6.9, maximum |change| 10.4.
- **What actually went missing** (a between-foods test; it cannot see which of a household's own items went missing):
  - On common foods, Centrale reports 27.7% fewer items and loses 31.8% of 2018 value; Plateaux 14.0% / 15.3%; Savanes 15.6% / 15.4%.
  - That is closer to the random-drop arm than to the smallest-items arm.
  - The correlation of the reporting fall with β is −0.15 to −0.28 on 37–39 foods, which is not distinguishable from zero.

## 6. Regional poverty change from 2018 (points; gross basis unless stated; bootstrap SE about 4–5 for Centrale)

| | Centrale | Kara | Lomé | Maritime | Plateaux | Savanes | National |
|---|---|---|---|---|---|---|---|
| Official (ehcvm_welfare) | **+2.9** | −2.7 | −2.2 | −3.0 | −2.5 | −1.2 | −1.75 |
| S1 A | +35.3 | +19.9 | +28.1 | +40.0 | +21.1 | +33.4 | +29.8 |
| S1 B | +31.7 | +18.8 | +30.8 | +40.1 | +21.0 | +30.0 | +29.7 |
| S3u A / B | +30.2 / −18.3 | | | | | | +19.7 / +13.2 |
| Sum A / B | +19.0 / −0.3 | −0.7 / −0.7 | +17.6 / +16.2 | −0.4 / −0.4 | +2.0 / −8.2 | −6.2 / −15.0 | +6.1 / +0.5 |

- **S1 is invariant to the factor but not a usable level.**
  - It is +27 to +35 nationally on every basis and arm: E +30.0, alltm +28.9, valued +34.9 (valued retains 85–98% of households, so selection is not driving it).
  - The unit-value adjustment as built (S2) did not reduce it (+43). Its ℓ includes (j, u) cells present in one wave only, and its national slope differs between arms, which looks like composition. So relative prices are not ruled out.
  - Negative controls with no factor and rising item counts: Senegal S1 +11.9 (purchased) / +15.8 (valued) against official −0.3; Niger +6.3 / +13.5 against +0.4. **The cause of the cross-wave S1 shift is not established.** I do not net those countries off Togo: one fit each, no gross basis, no placebo.
- **S3 (unit values) brings the factor back.** A − B is about 48 points in Centrale under S3u, S3f and S3n, and E reproduces B's shift. This is hypothesis 3: separating a_jtm into prices and levels re-imports whatever scaled all goods.
- **The sum benchmark's level is unreliable** (Senegal −8 / −10, Niger −18 / −19 against an official change near 0). Use only its contrasts: A − B in Centrale ≈ 19 points (27 on the CFE-retained households), and C − A as above. The rows above do not show "B ≈ official" or "A ≈ the doc search's +20.5".

## 7. Caveats you asked about
- **Floored Lomé counts:** 916 of 958 retained Lomé households have at least one withheld count row (3,308 of 23,060 consumed rows). That is why Lomé 2021-22 is the weakest A vs B cell (w correlation 0.964, quintile agreement 0.75).
- **The 1.2 / 0.82 split:** a household's |Δw| correlates 0.25 with its share of 1.2-split rows and 0.07 with its share of withheld rows (gross). The split is the larger non-uniformity.
- **Does a_jtm absorb genuine level change?** A uniform change: entirely. A β-proportional one: recovered. In practice the identifying β-direction is noisy (P sd up to 7 points regionally), and something in the data moves it +6 to +30 points across waves in all three countries tested.

## 8. Other EHCVM-2 countries
- No other EHCVM-2 country carries the factor (2026-11-12 live check), so the A vs B comparison has nothing to invert there.
- Items per household (source rows, unweighted, label lists changed between waves) rose nationally in Benin, Burkina Faso, Côte d'Ivoire, Niger and Senegal, and fell 1.7% in Mali. Read each region against its own national ratio.
- Regional falls of at least 14%: Mali Gao −22%, Kidal −19%, Kayes −16%; Côte d'Ivoire Bounkani and Folon −17%, Agnéby-Tiassa and Moronou −16%, Gontougo −14%; Senegal Kédougou −14%.
- Guinea-Bissau 2021-22 `food_acquired` is not wired.
- **Recommendation:** not worth running as a poverty comparison until S1's cross-wave shift is explained. A cheap next step is S1 with the P and G gates across all EHCVM-2 countries.

## 9. Issue status, residuals, questions
- **#1709: partial.** The test was run and answers the invariance and item-drop questions for Togo. The level question is open.
- **Drafted residual (Tier C):** "CFE w's cross-wave level shifts +6 to +30 headcount points from EHCVM 2018-19 to 2021-22 (Togo, Senegal, Niger) with no producer factor; separate prices from levels before any poverty use." Body: §§5–6 here, with the P and G controls as acceptance gates.
- **Drafted residual (Tier C, docs):** "`demand-estimation` skill: for EHCVM countries `basis='total'` equals `'purchased'` (no produced or in-kind value is recorded); a total needs `valuation=`. Also, CFE's `min_prop_items` keeps only 45% of Savanes 2018 households on the purchased basis."
- **Questions for you:**
  1. Which level separation should a CFE poverty measure use? S1 is factor-invariant but shifts in every country tested; unit-value separations re-import the factor.
  2. Is Togo's 2021 fall in reported foods to be read as reporting error or as a genuine extensive-margin response? CFE ignores the extensive margin either way.
  3. Should the S1 negative control be run across all EHCVM-2 countries?
