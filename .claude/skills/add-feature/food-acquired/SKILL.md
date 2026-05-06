---
name: food-acquired
description: Use this skill to add the food_acquired feature to an LSMS-ISA country. This skill should be used when a user wants to add household food acquisition data (quantities, values, units) with metric unit conversions. This is the most complex feature — it requires food item harmonization, unit harmonization, and unit-to-kg conversion factors.
---

# Add Food Acquired Feature to LSMS Country

This skill guides adding `food_acquired` — the foundational food data feature from which `food_expenditures`, `food_prices`, and `food_quantities` are all derived. Getting the units right is critical.

## Why food_acquired matters

Raw survey data records food quantities in local units (heaps, tins, bunches, cups). These are not comparable across households, regions, or countries. The `food_acquired` pipeline:

1. Harmonizes food item names across waves
2. Harmonizes unit labels across waves
3. Converts all quantities to a common metric (kg or liters)
4. Computes unit values (price per local unit)

The downstream features (`food_expenditures`, `food_prices`, `food_quantities`) all depend on this.

## Target schema

```yaml
food_acquired:
    index: (t, v, i, j, u, s)
    Quantity: float
    Expenditure: float
```

Index: wave (`t`) × cluster (`v`) × household (`i`) × food item (`j`) × unit (`u`) × **acquisition source (`s`)**.

The `s` axis carries acquisition source as canonical values: `purchased`, `produced`, `inkind`, `other` (see `lsms_library.transformations.S_VALUES`).  The full design rationale lives in `slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org` and GH #169.

Each input row from a survey questionnaire typically becomes **multiple long-form rows** — one per (item, unit, source) tuple where the household has data.  E.g., a household that purchased 5 kg of rice and produced 2 kg of rice emits two rows: one with `s='purchased'`, `Quantity=5`; another with `s='produced'`, `Quantity=2`.

`Price` is **api-derived** (computed in `Country._finalize_result`) — it is *not* stored in the wave-level parquet.  Semantics by source:

| `s`         | Price means                        | Where it comes from                              |
|-------------|------------------------------------|--------------------------------------------------|
| `purchased` | market price                       | `Expenditure / Quantity` (back-calculated)       |
| `produced`  | farmgate price                     | survey-reported where available, else NaN        |
| `inkind`    | imputed value of in-kind receipt   | survey-reported where available, else NaN        |
| `other`     | NaN by default                     | only filled if the country provides a valuation  |

If a country's source data records farmgate prices natively (e.g., Uganda's `farmgate` column), pass them through under `Price` for the `s='produced'` rows.

### Visit / round handling

Some surveys record multiple visits per wave (EHCVM `vague`, pp/ph countries' planting / harvest rounds).  **Fold these into `t` rather than carrying a separate `visit` index level**, e.g., `2018-19_p1` / `2018-19_p2` for EHCVM passages, `2018Q3` / `2019Q1` for Nigeria pp/ph.  See `add-feature/pp-ph` for the script-path pattern.

## The unit conversion pipeline

### Step 1: Harmonize food item names

Each country needs a `#+NAME: harmonize_food` table in `categorical_mapping.org` (or a standalone `food_items.org`) that maps variant spellings across waves to canonical labels.

Access from code:
```python
from lsms_library.local_tools import get_categorical_mapping
food_labels = get_categorical_mapping(tablename='harmonize_food',
                                      idxvars={'j': 'Original Label'},
                                      **{'Label': 'Preferred Label'})
```

The `get_categorical_mapping()` function searches for the named org table in `categorical_mapping.org`, reads it via `df_from_orgfile()`, and returns a dict.

Reference: `lsms_library/countries/Uganda/_/food_items.org`, `lsms_library/countries/Malawi/_/categorical_mapping.org`

### Step 2: Harmonize unit labels

Each country needs a `#+NAME: unit` table in `categorical_mapping.org` (or a standalone `units.org`) mapping numeric unit codes to canonical unit names across waves.

Access from code:
```python
unit_labels = get_categorical_mapping(tablename='unit')
```

The legacy approach uses a separate `unitlabels.csv` — the modern approach puts everything in `categorical_mapping.org`.

Reference: `lsms_library/countries/Uganda/_/units.org`, `lsms_library/countries/Mali/_/categorical_mapping.org` (has `#+NAME: unit`)

### Step 3: Convert units to metric (THE CRITICAL STEP)

There are two approaches, depending on what the survey provides:

#### Approach A: Price-ratio inference (Uganda pattern)

**This is the clever part.** When some households report in kg and others report in local units, the *ratio of unit values* reveals the conversion factor:

```
price_per_kg = value / quantity_in_kg        (from kg-reporting households)
price_per_local = value / quantity_in_local   (from local-unit households)
kg_per_local = price_per_local / price_per_kg
```

The key code is in `lsms_library/countries/Uganda/_/kg_per_other_units.py`:

```python
# Price per kg (from households reporting in known metric units)
pkg = v[prices].divide(v.Kgs, axis=0)
pkg = pkg.groupby(['t','m','i']).median().median(axis=1)

# Price per other unit
po = v[prices].groupby(['t','m','i','u']).median().median(axis=1)

# Ratio = kg per local unit
kgper = (po/pkg).dropna()
kgper = kgper.groupby('u').median()
```

This produces a `kgs_per_other_units.json` that maps every local unit to its kg equivalent, inferred purely from the data.

For units whose names already encode the conversion (e.g., "Sack (120 kgs)" → 120), hand-coded values in `conversion_to_kgs.json` take priority.

**Key files:**
- `{Country}/_/conversion_to_kgs.json` — hand-coded conversions (from unit label names)
- `{Country}/_/kg_per_other_units.py` — infers remaining conversions from price ratios
- `{Country}/_/kgs_per_other_units.json` — output of inference (merged with hand-coded)

#### Approach B: Survey-provided conversion tables (Malawi pattern)

Some surveys (Malawi IHS, Ethiopia ESS) include measured conversion factors as part of the survey data. These are item × unit × region specific — e.g., a "Pail (Small)" of maize weighs 1.93 kg in the North region.

**Key files:**
- `{Country}/{wave}/_/ihs3_conversions.csv` (Malawi) — pre-built from survey documentation
- `{Country}/{wave}/Data/Food_CF_WaveN.dta` (Ethiopia) — survey-provided conversion factors
- `{Country}/{wave}/Data/caloric_conversionfactor.dta` (Malawi 2019-20)
- `{Country}/{wave}/Data/ihs_foodconversion_factor_*.dta` (Malawi 2019-20)

The code joins these factors onto the food data by item × unit × region.

Reference: `lsms_library/countries/Malawi/2010-11/_/food_acquired.py`

### Which approach to use

- If the survey provides conversion factors (`.dta` or `.csv` files with item × unit → kg mappings): **use Approach B**
- If not: **use Approach A** (price-ratio inference)
- Many countries benefit from a **combination**: hand-coded conversions for units with metric amounts in their names, survey-provided factors where available, and price-ratio inference for the rest

## Country-specific notes

| Country | Approach | Conversion source | Status |
|---------|----------|-------------------|--------|
| Uganda | A (price-ratio) | `conversion_to_kgs.json` + `kg_per_other_units.py` | Complete |
| Malawi | B (survey tables) | `ihs3_conversions.csv`, IHS food conversion factors | Partial (legacy .py scripts) |
| Tanzania | A (price-ratio) | `conversion_to_kgs.json` | Complete (legacy) |
| Ethiopia | B (survey tables) | `Food_CF_WaveN.dta` | Partial (legacy) |
| Mali | Mixed | Has `categorical_mapping.org` for food items | Partial |
| Nigeria | ? | Check for conversion factor files | Not started |
| Niger | ? | Check for ECVMA/EHCVM conversion files | Not started |
| Burkina Faso | ? | Check for EHCVM conversion files | Not started |

## Check existing documentation first

Before starting implementation, **read the `.org` files** in the country's `_/` directory. These are literate documents that often contain hard-won insights about data quirks, unit conversion decisions, and food item harmonization choices. Key files to check:

- `{Country}/_/CONTENTS.org` — overview of data issues and decisions
- `{Country}/_/units.org` — unit code mapping rationale
- `{Country}/_/food_items.org` — food item harmonization table (also used as a `categorical_mapping` reference)
- `{Country}/_/demands.org` — may contain analysis that reveals data structure
- `{Country}/_/nutrition.org` — may contain conversion factor derivations
- `{Country}/{wave}/_/*.org` — wave-specific notes

These documents may explain *why* certain choices were made (e.g., why a particular unit was dropped, or why a conversion factor differs from the survey documentation).

## Implementation workflow

1. **Examine Uganda's implementation** as the reference:
   - `Uganda/_/uganda.py` → `food_acquired()` function
   - `Uganda/_/food_items.org` → food item harmonization
   - `Uganda/_/units.org` → unit label harmonization
   - `Uganda/_/conversion_to_kgs.json` → hand-coded metric conversions
   - `Uganda/_/kg_per_other_units.py` → price-ratio inference

2. **Find the food consumption module** for the target country:
   - Usually Section G/J/K (household consumption)
   - WB reference code: check `global items`, `global harvest_rwdta` in the country's `.do` files

3. **Build the harmonization tables**:
   - `food_items.org` — inspect all waves' food item labels, create preferred mappings
   - Unit labels — inspect unit codes/labels across waves
   - This is the most labor-intensive step

4. **Build or obtain conversion factors**:
   - Check if the survey provides conversion factor files
   - If not, bootstrap from the data using price-ratio inference
   - Hand-code conversions for units with metric amounts in their names

5. **Write the extraction code** (`.py` script or `data_info.yml`)

6. **Verify** with `is_this_feature_sane`

## Common pitfalls

- **Unit codes change across waves** — the same physical unit (e.g., "Pail Small") may have code 4 in one wave and code 4A in another
- **Food item names vary wildly** — "Maize ufa mgaiwa (normal flour)" vs "Maize Ufa Mgaiwa (Normal F" (truncated) vs "MAIZE UFA MGAIWA (NORMAL FLOUR)"
- **Regional variation in local units** — a "heap" of tomatoes may be 0.5 kg in one region and 2 kg in another. Survey-provided conversion factors are region-specific for this reason.
- **Missing conversion factors** — some unit × item combinations may lack conversion factors. The price-ratio method can fill these gaps.
- **Multiple acquisition sources** — surveys typically ask about purchased, own-produced, received as gift. Each may have different units.
