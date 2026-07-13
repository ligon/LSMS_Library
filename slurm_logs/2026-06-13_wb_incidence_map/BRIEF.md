# WB LSMS-ISA harmonised panel × our features — incidence-map brief

Shared artifact for the `wb-incidence-map` workflow. Author: Sue (scrum master), 2026-06-13.

## Goal

Build a country × feature-area **incidence map / scorecard** comparing OUR
features (LSMS_Library) with the variables the **World Bank LSMS-ISA
harmonised panel v2.0** constructs, so we can (1) see coverage at a glance,
(2) sanity-check overlaps, and (3) drive a parity loop that closes gaps.

## Sources

- THEIRS (read-only): `/global/scratch/fsa/fc_jevons/ligon/reference/lsms-isa-harmonised/Reproduction_v2/`
  - `Code/programs.do` — `define_labels` (lines ~562-680) is their FINAL
    variable dictionary; valuation programs reveal median-price methodology.
  - `Code/Cleaning_code/{CC}_*.do` — per-country-wave construction.
  - `Code/Cleaning_code/Append_{CC}.do` — what each country's final file carries.
- OURS: `lsms_library/countries/{Country}/_/data_scheme.yml` (+ runtime-derived
  features: household_characteristics, food_expenditures/quantities/prices).

## Overlap countries (the scorecard columns)

Ethiopia (their ESS = our `Ethiopia`), Mali (EACI), Malawi (IHPS), Niger
(ECVMA), Nigeria (GHS), Tanzania (NPS), Uganda (UNPS). (EthiopiaRHS and our
EHCVM-only countries like Burkina_Faso are NOT in their panel; note as our-only.)

## THE CAUTION (first-class)

Their panel is **fundamentally aggregated**: ~1,231 `collapse` + ~954
aggregating `egen`. Many "variables" are HH/plot/holder-level sums, ratios,
or constructed indices (`harvest_kg`, `total_labor_days`, `totcons`,
`ag_asset_index`, `hh_asset_index`, `cons_quint`, `hh_dependency_ratio`,
the `*_value_{LCU,USD}` median-price valuations). Our philosophy: data stays
**item-level**; aggregation lives in `transformations.py`, not in parquets.

**Every their-variable in the map MUST be tagged `agg` (aggregate/index/ratio/
imputed-valuation) or `item` (item/row-level, directly parity-able).** For an
`agg` THEIRS-ONLY cell, the recommendation is "expose underlying item-level
feature + add a `transformations` function", NEVER "build an aggregated feature".

## SECOND CAUTION: tasteful decomposition (do NOT mirror their wide table)

Stata holds only ONE dataframe in RAM, which forces their harmonised output
into very WIDE tables — everything (plot area + crops + inputs + labor +
valuations) collapsed and merged onto a single grain/row. We are NOT
constrained this way and must not emulate it. Gap recommendations must propose
a **tasteful SET of feature dataframes at their NATURAL grains** (each may be
narrow — even single-column — or wide, as fits), not a wide mirror. Example:
their plot-grain wide block decomposes into distinct features —
plot characteristics `(t,i,plot)`, crop production `(t,i,plot,crop)`, input
use `(t,i,plot,input)`, plot labor `(t,i,plot,...)` — each its own dataframe.
Apply this TOGETHER with the item-level + aggregation-in-transformations rules.

## Bridging feature-area taxonomy (the scorecard ROWS) — shared by all agents

Map both sides onto these areas. (Our-feature crosswalk is a guide, not exhaustive.)

| # | Feature-area | Their variables (examples) | Our feature(s) |
|---|---|---|---|
| 1 | Panel IDs | *_id_obs/_merge, ea_id, season | panel_ids |
| 2 | Sampling/geography | strataid, urban, admin_*, geocoords, pw | sample, cluster_features |
| 3 | HH composition | hh_size, hh_dependency_ratio | household_characteristics, household_roster |
| 4 | Individual roster | age, female, married, indiv_id | household_roster |
| 5 | Education | education, formal_education, *_education_* | individual_education |
| 6 | Anthropometry/nutrition | weight, height, haz06 | nutrition |
| 7 | Labor/time-use | total_labor_days, farm_work, wage_work, farm_hrs | employment, people_last7days |
| 8 | Plots/parcels | plot_area, irrigated, fallow, owned, certificate | plot_features |
| 9 | Crop production/harvest | harvest_kg, yield_kg, main_crop, harvest_sold_kg | (likely OURS-thin) |
| 10 | Ag inputs | seed_kg, fertilizer, herbicide/pesticide, improved | (likely OURS-thin) |
| 11 | Ag valuations | yield_value, harvest_value, seed_value (median-price) | food_prices (logic only; mostly transforms) |
| 12 | Livestock | livestock | livestock (bespoke today) |
| 13 | Assets | ag_asset_index, hh_asset_index | assets (item-level; they INDEX) |
| 14 | Consumption/welfare | totcons_{LCU,USD}, cons_quint | food_acquired/expenditures, nonfood_expenditures |
| 15 | Housing/utilities | hh_electricity_access | housing |
| 16 | Shocks | hh_shock, crop/drought/rain/pests/flood_shock | shocks |
| 17 | Subjective wellbeing | (none expected on their side) | subjective_well_being, life_satisfaction |
| 18 | Food security | (none expected) | food_security, food_coping, months_food_inadequate, food_security_hfias |

## Cell value per (country, feature-area)

One of: `BOTH`, `OURS-ONLY`, `THEIRS-ONLY`, `NEITHER`. Plus, when theirs is
present, the dominant `agg`/`item` tag for that area, and (for THEIRS-ONLY /
partial) a pointer to the specific `.do` file+construct to consult.

## Outputs (the synthesis agent assembles)

1. `INCIDENCE_MAP.org` — the 18-area × 7-country matrix (cells as above),
   with an aggregation legend.
2. Per-area detail table: our feature(s) ↔ their variable group ↔ grain ↔
   agg/item ↔ `.do` source ↔ recommendation.
3. `GAP_RANKING.org` — THEIRS-ONLY (and OURS-partial) areas ranked by
   parity value. For each gap propose a **tasteful dataframe decomposition**
   (feature name(s) + natural grain — NOT a wide mirror of their table),
   tagged build-as-item-feature vs add-transformations-fn, with the `.do`
   construct to consult. This seeds the parity loop.

## Workflow role split

- **Extract** (parallel, 1 agent/overlap-country, READ-ONLY): from that
  country's `{CC}_*.do` + `Append_{CC}.do`, report per feature-area which of
  their variables that country constructs, the grain, and the agg/item tag,
  with `.do`:line evidence.
- **OurCoverage** (1 agent): authoritative per-country feature coverage from
  `data_scheme.yml` + the runtime-derived set, mapped to the 18 areas.
- **Synthesize** (1 agent): assemble the three outputs above.
- **Adversary** (1 agent): verify THEIRS-ONLY gaps are real (not just naming
  differences), every agg/item tag is correct, and no aggregated variable is
  recommended as a data-layer feature. Returns corrections.
- **Final review**: Sue.
