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

`Price` is an **optional column** on the wave-level parquet — it is populated only when the survey records a unit price directly.  Semantics by source:

| `s`         | Price means                        | Where it comes from                              |
|-------------|------------------------------------|--------------------------------------------------|
| `purchased` | market price                       | survey-reported where available, else NaN        |
| `produced`  | farmgate price                     | survey-reported where available, else NaN        |
| `inkind`    | imputed value of in-kind receipt   | survey-reported where available, else NaN        |
| `other`     | NaN by default                     | only filled if the country provides a valuation  |

If a country's source data records prices natively (e.g., Uganda's `market` column for purchased and `farmgate` column for produced), pass them through under `Price` for the corresponding `s` value.  Consumers access these via `food_prices(units='unitprice')` (per native `u`) or `food_prices(units='kgprice')` (converted to per-kg).

Wave scripts that omit `Price` are fine — `food_prices(units='kgvalue')` (the default) and `food_prices(units='unitvalue')` derive Price from `Expenditure / Quantity_kg` and `Expenditure / Quantity` respectively, so the framework still produces useful per-kg or per-unit prices.  See *Downstream API* below.

### LCU-only goods: `u='Value'`

Some goods have no natural physical unit — the survey records only expenditure in local currency.  Examples: "Meals in restaurants", GhanaLSS 1987-88 / 1988-89 (money-only food data).  Encode these as a synthetic `u='Value'` with `Quantity = Expenditure` and `Expenditure` populated.

These rows flow correctly through:

- `food_expenditures` (sum across `u`, irrespective of physical-unit conversion).
- `food_prices(units='unitvalue')` — gives 1 (Kwacha per Kwacha; mathematically tautological, sentinel-clear).
- `food_quantities(units='kgs')` — carried through with `u='Value'` and the LCU quantity preserved (per the carry rule); consumers wanting purely-kg do `df.xs('kg', level='u')`.

They naturally drop out of:

- `food_prices(units='kgvalue' | 'kgprice')` — no kg factor for `u='Value'`, so NaN.
- `food_prices(units='unitprice')` — no reported `Price` column for value-only goods.

No special-casing required; the canonical schema accommodates `u='Value'` directly.

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

**Caveat (fixed in `2ab51106`, GH #222).** `df_data_grabber` -- which `get_categorical_mapping` calls under the hood -- runs every `idxvars` value through `format_id`, which historically did `s.split('.')[0]` on string input to strip Stata's `"123.0"` -> `"123"`.  For food labels ending in `"etc."` or any internal period, that quietly truncated the dict key.  The fix narrows the strip to "both sides of the dot are digits", so calls like `get_categorical_mapping(idxvars={'j': 'Original Label'})` are now safe.  If you're working on a country that hasn't been rebuilt since the fix, force a rebuild after pulling.

**Alternative for case / encoding handling.** If your wave script applies `df['i'].str.capitalize()` (or hits encoding mojibake -- `\x96`, `�`, `ï¿½` for en-dashes), the dict-key lookup will silently miss anything where the org-column entry differs in case from the post-`.capitalize()` data.  Malawi works around this with `apply_harmonize_food()` and `normalize_food_label()` helpers in `Malawi/_/malawi.py` that read `categorical_mapping.org` directly and case-fold dict keys to match the data path.  Worth copying that pattern when adding a new country whose source `.dta` carries case drift or mojibake.

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

#### Approach C: Framework-level conversion (default since GH #231)

As of GH #231, the framework's `food_quantities_from_acquired` and `food_prices_from_acquired` (in `lsms_library/transformations.py`) do *most* of the kg conversion work without per-country setup, given a canonical `food_acquired` table with a `u` index level.  The pipeline `_get_kg_factors()` runs three layers in order:

1. **Hand-coded `KNOWN_METRIC` lookup** — covers `kg`, `kilogram`, `kilogramme`, `g`, `gram`, `gramm`, `l`, `litre`, `liter`, `ml`, `cl`, `pound`, `lbs`, all case-insensitive.  Most country surveys are case-mojibake of these (`KILOGRAMME`, `Kg`, `LITRE`) and resolve here for free.
2. **Explicit-metric label parser `_parse_explicit_metric()`** — extracts a kg factor from labels that name their own metric content: `"50 kg Bag"`, `"500 g packet"`, `"1L Carton"`, `"500 ml Bottle"`, `"2 lbs sack"`.  Mass patterns always match; volume patterns (l/ml/cl) are gated on the `volume_as_mass` kwarg.
3. **Price-ratio inference (Approach A above)** — fills in remaining unit factors from the data.

Empirically (see `SkunkWorks/malawi_unit_resolution_diagnostic_2026-05-07.org`): on Malawi, this resolves 99.9% of rows to `u='kg'` with no country-specific code at all — 732 distinct input u-values collapse to 86.  The 0.1% residual is dominated by unmeasured container labels (`Basin/Lichero`, `THUNGWA`, `NKOKO`).

**`volume_as_mass` kwarg.**  By default the framework treats `1 litre = 1 kg` (specific-gravity-1 approximation).  This is roughly right for water-based foods (juice, milk, soup) and moderately wrong for cooking oil (~0.92) and alcohol.  Pass `volume_as_mass=False` to drop fluid units from the hand-coded factor map and from the parser; the inference path then back-fills whatever the data implies.  Note: this is a *disable-the-shortcut* flag, not a *correct-fluid-handling* flag — `volume_as_mass=False` may not improve oil estimates if the inference's median factor across all foods is also water-dragged.

**When you still need country-specific Approaches A or B.**  Approach C handles the easy cases (case-variants, explicit-metric labels, items where kg-and-local-unit observations co-exist).  You'll still want the legacy paths when:
  - the survey ships authoritative conversion tables item × unit × region (Malawi IHS, Ethiopia ESS) — Approach B remains better than inference for these;
  - unit labels encode hand-coded factors that the regex parser doesn't catch (e.g. `"Pail (Small)"`, `"No.10 Plate (Heaped)"`, `"Sack of 120 kgs"` with non-standard punctuation) — populate `conversion_to_kgs.json` per Approach A;
  - per-item specific-gravity matters (cooking oil precision) — neither Approach C nor the existing inference handles per-item factors today; per-item refinement is a noted future option.

### Which approach to use

- **Default**: Approach C does the work; minimum viable wave script is just the canonical `food_acquired` reshape.  Verify with the diagnostic recipe in `SkunkWorks/malawi_unit_resolution_diagnostic_2026-05-07.org` that >99% of rows resolve to `u='kg'`.
- If the survey provides conversion factors (`.dta` or `.csv` files with item × unit → kg mappings): **layer Approach B** on top of C — Approach B applies first at the wave script level, leaving Approach C with cleaner residuals.
- If neither C nor B closes the gap and the long tail is large (>1% of rows or contains nutritionally important items): **add hand-coded factors** via Approach A or via the categorical_mapping `u` table.

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

## Downstream API: `food_prices(units=...)` and `food_quantities(units=...)`

Phase 4 (PR #224) added a `units=` kwarg to the derived food tables.  When implementing a country's `food_acquired`, anticipate which modes will be useful:

**`food_prices(units=...)` — four modes:**

| Mode          | Formula                                   | Output unit         | When useful                                                  |
|---------------|-------------------------------------------|---------------------|--------------------------------------------------------------|
| `'kgvalue'` *(default)* | `Expenditure / Quantity_kg`     | currency / kg       | Cross-country comparisons; demand systems; backward compat.   |
| `'unitvalue'` | `Expenditure / Quantity` (native)         | currency / native u | Native-unit prices; preserves info `kgvalue` discards.       |
| `'kgprice'`   | reported `Price` × kg_factor              | currency / kg       | When wave script populates `Price`; gives per-kg prices for produced/inkind rows where Expenditure is NaN. |
| `'unitprice'` | reported `Price` (native u)               | currency / native u | Surfaces farmgate / market / imputed prices the survey recorded directly. |

**`food_quantities(units=...)` — two modes:**

| Mode      | Formula                              | Output                                                |
|-----------|--------------------------------------|-------------------------------------------------------|
| `'kgs'` *(default)* | kg where convertible; native `Quantity` carried with native `u` tag where it isn't | mixed-physical-unit; `u` tag distinguishes |
| `'units'` | sum of native `Quantity` per `(t, v, i, j, u, s)` | preserves native units |

The default for both is the pre-Phase-4 behavior (kg-denominated).  No silent fallback between modes — `unitprice` returns NaN where `Price` is missing, doesn't fall back to `unitvalue`.

Full design rationale: `slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org`.

**Naming caveat**: `kgvalue` is what the demand-systems literature usually calls "unit value" (Deaton 1988, 1997 — `Expenditure / Quantity` standardized to kg).  Our `unitvalue` mode deliberately departs from that convention to mean per-native-`u`.  Document this distinction in any analysis-facing docstring.

## Designing the `Aggregate Label` and `Aggregate (short)` columns

After `harmonize_food` is in place and `food_expenditures()` returns
clean per-PL data, downstream demand estimation (CFE) usually wants a
*coarser* grouping than the full Preferred-Label list — both for
estimation power (sparse PLs that fail `cfe.regression.prepare_data`'s
`min_obs=30` floor want a home) and for downstream interpretability.
This is the role of the `Aggregate Label` column on `harmonize_food`.

### When to add an `Aggregate Label` column

When any of:

- The country has > ~30 Preferred Labels and the analyst plans to run
  CFE — a smaller bucketing avoids the dense-many-items regression
  becoming noisy.
- A meaningful number of PLs are observed too rarely (per-wave-market)
  to clear `prepare_data`'s thresholds; bundling sparse items with
  type-similar bigger items rescues them.
- The analyst wants a `j` index of ~20–40 buckets for plots / tables.

Trigger the API by calling
`Country.food_expenditures(labels='Aggregate', reaggregate=True)` (or
the same on `food_quantities`). The framework reads the `Aggregate
Label` column from `harmonize_food` and renames the `j` level
accordingly, summing expenditures within each bucket.

### Bucket-design rules (Malawi worked example: 65 buckets)

The principled criterion is the **CFE β-spread test** at low
confidence (~ 50% CI):

> For every multi-PL bucket, the within-bucket β span must satisfy
> `max(β) - min(β) < 1.348 × σ_min`, where σ_min is the smallest
> standard error among the bucket's well-estimated constituents.
> Buckets failing this introduce a w-correlated residual: the
> aggregate β becomes a weighted average that drifts from each
> constituent's individual β by more than 50% CI, and the resulting
> aggregate-labelled regression no longer estimates the same latent
> MUE as the Preferred-labelled one.

In practice the curator iterates:

1. Run CFE on Preferred Labels → get `(β, σ)` for each PL.
2. Cluster PLs by β within type-coherent groups (don't lump
   `Eggs Boiled (Vendor)` β=0.51 with `Maize Ufa Refined (Fine Flour)`
   β=0.51 just because the βs match — they're different food types).
3. Assign sparse (no-β) PLs to whichever bucket they conceptually
   match (use category, common-sense substitutability).
4. Re-test the bucket scheme against the β-spread rule; split any
   failing bucket along its dominant β cleavage line.

A typical end state: ~30–60 buckets, with 5–10 well-estimated
singletons (sugar, salt, rice, …), ~20–30 small clusters (tropical
fruits, leafy greens, fish-by-preservation, …), and a residual
"Other" bucket.

### Carve-outs for downstream experiments

Sometimes a downstream analysis (an experimental sub-survey, a
specific paper) requires that certain Preferred Labels remain their
own Aggregate buckets — verbatim string match. In Malawi the
MalawiMUEs experiment reserves 17 such PLs. The convention: set
`Aggregate Label = Preferred Label` for those rows, and design the
remaining buckets around them. Other PLs in the same conceptual
category that *aren't* on the carve-out list can still fold into the
carve-out's bucket (e.g. `Honey` and `Jam, Jelly, Honey` join the
`Sugar` carve-out bucket because Sugar is the carve-out anchor).

### `Aggregate (short)` column for graphs / tables

A second optional column gives short forms (≤ 11 characters) of each
Aggregate Label, suitable for axis ticks, panel titles, table
headers. Keep the two columns separate — the long form is the
analytical handle, the short form is the display label.

Design heuristics:

- Drop generic qualifiers when uniqueness preserved (`Cooking Oil`
  → `Oil`, `Sugar Cane` → `Cane`).
- Use local terms where they help legibility (Malawi: Mgaiwa,
  Madeya, Nkwani, Tanaposi, Therere, Nandolo, Maheu, Thobwa, Bica,
  Chambo).
- Pluralisation suffix `+` for "this term and similar" when the
  bucket is a list (`Mango+`, `Beans+`).
- Brand-as-genre when the brand is the bucket
  (`Coffee`, `Soda`, `Mandazi`).

### Tooling

`lsms_library/util/orgtbl.py` adds or updates a derived column in any
named Org table.  Used like:

```bash
python -m lsms_library.util.orgtbl \
    lsms_library/countries/{Country}/_/categorical_mapping.org \
    --table harmonize_food \
    --column 'Aggregate (short)' \
    --mapping lsms_library/countries/{Country}/_/aggregate_short.yml \
    --source-column 'Aggregate Label' \
    --insert-after 'Aggregate Label'
```

The mapping YAML is a flat dict from source-column value to
target-column value. Idempotent on re-run; the same invocation can
update an existing column. Same tool handles inserting the
`Aggregate Label` column itself — supply a YAML keyed on
`Preferred Label`.

For Malawi see `lsms_library/countries/Malawi/_/aggregate_short.yml`
as a reference checked-in mapping.

### Empirical sanity (after re-running CFE on the Aggregate scheme)

Once buckets are designed and you have estimates `w_p` (Preferred)
and `w_a` (Aggregate), the empirical comparisons that matter:

1. Predictive validity against held-out non-food expenditure (your
   `lambda_predicts_nonfood.py` pattern). Whichever w predicts
   non-food better is closer to true MUE.
2. Stability under perturbation (drop one wave, sub-sample HHs).
3. Correlation `corr(w_p, w_a)` and the regression
   `w_p ~ α + δ·w_a` — useful as **diagnostics**: agreement
   reassures, disagreement signals a bucket-design problem the
   β-spread test missed (e.g., separability violation, demographic
   heterogeneity within bucket). Disagreement does not by itself
   adjudicate which is correct — for that, fall back on (1).

GH #226 tracks the principled auto-aggregation script that would
codify the β-spread test plus a type-coherence judgement plus
greedy clustering.

## Common pitfalls

- **Unit codes change across waves** — the same physical unit (e.g., "Pail Small") may have code 4 in one wave and code 4A in another
- **Food item names vary wildly** — "Maize ufa mgaiwa (normal flour)" vs "Maize Ufa Mgaiwa (Normal F" (truncated) vs "MAIZE UFA MGAIWA (NORMAL FLOUR)"
- **Regional variation in local units** — a "heap" of tomatoes may be 0.5 kg in one region and 2 kg in another. Survey-provided conversion factors are region-specific for this reason.
- **Missing conversion factors** — some unit × item combinations may lack conversion factors. The price-ratio method can fill these gaps.
- **Multiple acquisition sources** — surveys typically ask about purchased, own-produced, received as gift. Each may have different units.
- **Case-sensitive `u='kg'` lookups in downstream code** — the framework's `_apply_kg_conversion` lowercases unit lookups against `KNOWN_METRIC`, and Phase 4's `food_quantities(units='kgs')` tags converted rows with lowercase `'kg'`.  If a country's source data uses capital `'Kg'`, downstream code that does `df.xs('Kg', level='u')` will silently break against framework-derived input (Uganda nutrition.py / unitvalues.py hit this — fixed in PR #224 with case-insensitive `mask = u.str.lower() == 'kg'`).
