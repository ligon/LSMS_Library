Title: Transforms umbrella: livestock_income, rcsi, HDDS/FCS, fertiliser rate, gross crop revenue, crop diversity
Labels: enhancement

Six small transforms EPAR builds routinely from items we already store
have no equivalent in `transformations.py` today (`grep -c` for each name
is 0). None needs new microdata -- they are reductions over rosters and
item tables we already carry -- but two of them (`livestock_income`,
`rcsi`) have a real design decision or a real bug to avoid copying.

## What is missing, and why each one is not a copy-paste

**`livestock_income`** -- EPAR imputes it from `number_sold` and
`number_slaughtered` times a laddered price. We have no such function.
Caveat: our own `ValuePerAnimal` column is explicitly a **reported
hypothetical**, not a transaction price --
`lsms_library/data_info.yml:419-440` documents two instrument variants,
both per head: a RESERVATION price ("If you would sell one of the
[ANIMAL] today, how much would you receive?", reported whether or not
anything was sold -- Nigeria `s11iq3`, Malawi `ag_r04`, Uganda 2009-10/
2010-11 `a6aq6`) and a REALISED average (defined only where the household
sold -- Uganda 2011-12 onward). Porting EPAR's construct against
`ValuePerAnimal` without saying which variant is live in a given
country/wave would silently change what the resulting number means.

**`rcsi`** (reduced Coping Strategies Index) -- for Malawi, EPAR's formula is
`rcsi = s1 + s2 + s3 + 3*s4 + 2*s5` over five strategy counts
(`Malawi IHS/Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:4525`,
`gen rcsi=strategy1 + strategy2 + strategy3 + 3*strategy4 + 2*strategy5`,
over `hh_h02a..hh_h02e` renamed at `:4520-4524`). That maps one-to-one onto
our `food_coping` table, which Malawi declares at
`Malawi/_/data_scheme.yml:156-168` -- "Module H question H02 (hh_h02a..hh_h02e)
... how many of the past 7 days the HH used each of the 5 standard
reduced-Coping-Strategies-Index strategies", index `(t, i, Strategy)`.
Two things the implementation must not inherit.

First, **the formula is not universal**: EPAR's Tanzania W5 rcsi
(`Tanzania NPS/Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do:2617`) is an
EIGHT-term variant with different weights --
`hh_h02a + hh_h02b + hh_h02c + hh_h02d + 3*hh_h02e + hh_h02f*2 + hh_h02g*4 +
hh_h02h*4`. A single hard-coded five-term expression would silently produce
a Malawi-shaped number for a Tanzania questionnaire.

Second, **a defect in EPAR's phase cutoffs**: `W1.do:4531` is
`gen rcsi_phase3 = (rcsi > 19 & rcsi <= 42)` while phase 2 at `:4530` ends at
`rcsi <= 18` and the phase-3 label at `:4535` says "(19 - 42)". An `rcsi` of
exactly 19 falls in no phase. Ours must use intervals that partition, so
every integer score lands in exactly one phase.

**HDDS / FCS** (Household Dietary Diversity Score / Food Consumption
Score) -- food-group counts over diet recall. EPAR's group mapping is
`Tanzania NPS/Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do:2540-2552`
(banner at `:2532-2534`, source file at `:2537`, the two collapses that turn
item rows into a group count at `:2556` and `:2560`). It is a starting point
to re-derive against, not to copy: it groups on Tanzania's own `itemcode`
ranges, and we must map to our `harmonize_food` taxonomy instead. Note the
recode is not a partition -- item 704 appears in both "FRUITS" (`:2543`) and
"SWEETS" (`:2550`), and 1003 in both "OILS AND FATS" (`:2549`) and "SPICES,
CONDIMENTS, BEVERAGES" (`:2551`), so Stata's first-match wins and the
grouping depends on statement order.

**Fertiliser application rate, gross crop revenue, Shannon crop
diversity** -- reductions over `plot_inputs` / `crop_production` /
`plot_features` items we already hold (kg fertiliser per hectare cultivated;
sum of `Value_sold` across crops; Shannon entropy over crop-share of planted
area). `transformations.py` already carries `nitrogen_kg` (`:2469` onward,
with the per-product N shares at `:2448-2462`), which is the numerator half
of the fertiliser rate; what is missing is the per-hectare denominator and
the reduction. The other two have no partial. No EPAR-specific gotcha is
recorded for these three.

## Evidence

**Ours** -- case-insensitive `grep -c` for each name in
`lsms_library/transformations.py`: `livestock_income` = 0, `rcsi` = 0,
`hdds` = 0, `fcs` = 0, `dietary_diversity` = 0, `food_consumption_score` = 0,
`gross_crop_revenue` = 0, `shannon` = 0. `fertili` = 24, all of them
`nitrogen_kg`'s product-to-N-share machinery (`:2421-2462`, `:2469-2531`);
none is an application rate.

**EPAR side** (commit `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`LSMS-Agricultural-Indicators-Code/`)
- `Malawi IHS/Malawi IHS Wave 1/EPAR_UW_Malawi_IHS_W1.do:3339-3346` -- the
  price ladder (EA -> TA -> district -> region -> country median, each gated
  at `obs_* >= 10`, `:3339-3343`) and its two products,
  `gen value_lvstck_sold = price_per_animal * number_sold` (`:3345`) and
  `gen value_slaughtered = price_per_animal * number_slaughtered` (`:3346`).
  The ladder is applied to FLOWS, never to held stock.
- `:5959` -- the full `livestock_income` expression, which is broader than
  those two terms: slaughtered + sold - purchases + (milk + eggs + other
  produced + manure sales) - (hired labour + fodder + vaccines). Anything we
  build must say which of those terms it includes.
- `:4525` (rcsi formula), `:4529-4532` (phase cutoffs 3 / 18 / 42),
  `:4531` and `:4535` (the `== 19` gap).
- `Tanzania NPS/Tanzania NPS Wave 5/EPAR_UW_Tanzania_NPS_W5.do:2540-2552`
  (HDDS recode) and `:2617` (the eight-term Tanzania rcsi, on the file opened
  at `:2614`). EPAR builds no rcsi at all for Uganda W5:
  `Uganda UNPS/Uganda UNPS Wave 5/EPAR_UW_Uganda_UNPS_W5.do:2868` is "*No
  available data to construct rCSI module.", and that wave's `readme.md:284`
  says "The Reduced Coping Strategies Index (rCSI) was not calculated for the
  Uganda field survey, as the available data did not allow for a consistent
  construction of the indicator." Coverage is a per-wave fact.

## What a fix would touch

- New functions in `transformations.py`: `livestock_income`, `rcsi`
  (plus a phase-cutoff helper with closed intervals, kept separate from
  the raw score), `hdds` / `fcs` over our own
  `harmonize_food` groups, a fertiliser-rate reduction, a
  gross-crop-revenue reduction, and a Shannon-diversity reduction.
- Per-country wiring only where the source table (`food_coping`,
  `livestock`, `plot_inputs`, `crop_production`) already exists. Six
  countries declare `food_coping` (Malawi, Ethiopia, Tanzania, Nigeria, Mali,
  Burkina Faso); Malawi is the natural first, since its five strategies map
  onto EPAR's five-term formula exactly.

## What it must NOT do

- Never treat `ValuePerAnimal` as a transaction price for
  `livestock_income` without a design note stating which instrument
  variant (reservation vs. realised) is live for the country/wave being
  computed.
- Never reproduce EPAR's `rcsi == 19` phase gap -- use intervals that
  partition, so every integer score has exactly one phase.
- Never hard-code the five-term rcsi weights as if they were the definition;
  the strategy set and its weights are a property of the questionnaire, and
  EPAR's own Malawi and Tanzania formulas differ in both.
- Never copy EPAR's HDDS food-group recode ranges directly -- re-derive
  the grouping against our own `harmonize_food` table.
- Never assume a transform applies uniformly across every country/wave
  just because one wave has the source table -- EPAR's own Tanzania/
  Uganda rcsi asymmetry is a reminder that coverage is a per-wave fact,
  not a corpus-wide default.

## Cross-references

`dm_gender` is filed
separately, as part of the `plot_cultivation` schema proposal filed alongside
this one, since it depends on that table existing first.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org (L8)
