---
name: add-feature
description: Use this skill to add a missing feature (table) to an LSMS-ISA country in the LSMS Library. This skill should be used when a user wants to extend a country's data coverage by creating the YAML configuration and/or Python scripts needed to extract and harmonize a new dataset from raw survey microdata.
---

# Add Feature to LSMS Country

This skill guides the process of adding a new feature (table) to a country in the LSMS Library. The library harmonizes the *interface* across countries — each feature should produce a DataFrame with the same index structure and column names regardless of which country it comes from.

## Feature-Specific Sub-Skills

For domain-specific guidance on particular features, load the relevant sub-skill:

- `add-feature/shocks` — Household shocks (natural disasters, economic shocks, coping strategies). Covers module identification across countries, effect variable mapping (Decrease→True), combined-effect splitting, French/English label handling, and the 26-binary-coping-indicator pattern in EHCVM surveys.
- `add-feature/assets` — Durable goods ownership (item-level, no aggregation). Covers the Module L/M distinction across survey instruments and the design principle of passing item-level data without summing to household totals.
- `add-feature/panel-ids` — Panel household ID linkage across waves. Covers ID stability patterns, composite IDs, household splits, cross-survey-program limitations, and the World Bank harmonised panel as a reference.
- `add-feature/food-acquired` — Food acquisition data with unit conversions. The most complex feature — covers two approaches to unit-to-kg conversion: price-ratio inference from the data itself, and survey-provided conversion factor tables.
- `add-feature/sample` — Sampling design (cluster/PSU assignment, household weights, strata, urban/rural). Covers weight variable discovery across countries, single-file vs multi-file YAML patterns (EHCVM ponderation files), and strata label harmonization.
- `add-feature/pp-ph` — Post-planting / post-harvest countries (Nigeria, Ethiopia, GhanaSPS, Tanzania `2008-15/`). Covers the canonical duplicate-index bug, distinct-`t`-value script pattern, and attrition handling for people who appear in only one round.
- `add-feature/housing` — Dwelling material characteristics (roof, floor). Covers module-letter instability across surveys, case normalization of Stata labels, and the Cross_Sectional + Panel merge pattern.

## World Bank reference code

The World Bank's LSMS-ISA Harmonised Panel project has Stata code mapping raw files and variables for all 8 countries. Saved at `/var/tmp/lsms-isa-harmonised/reproduction/Reproduction_v2/Code/Cleaning_code/`. GitHub: `lsms-worldbank/LSMS-ISA-harmonised-dataset-on-agricultural-productivity-and-welfare` (release v2.0).

Their goal (one huge merged .dta) differs from ours (uniform API, preserve detail), but their per-wave do files reveal file names, variable names, and ID linkage logic that are useful reference. Treat with skepticism — verify against actual data.

## Read existing documentation FIRST

Before writing any code, **read the `.org` files** in the country's `_/` directory. These contain hard-won insights about data structure, variable quirks, and harmonization decisions:

- **`{Country}/_/CONTENTS.org`** — overview of available data, known issues, and design decisions. Most LSMS-ISA countries have one. **Start here.**
- **`{Country}/_/food_items.org`** — food item harmonization table
- **`{Country}/_/units.org`** — unit code harmonization table
- **`{Country}/_/demands.org`** — analysis notes that may reveal data structure
- **`{Country}/_/nutrition.org`** — may contain conversion factor derivations
- **`{Country}/_/categorical_mapping.org`** — centralized cross-wave category harmonization

Also check wave-level org files: `{Country}/{wave}/_/*.org`

## Dependencies — do NOT use `lsms.tools`

The upstream `lsms` package (`lsms >=0.4.13,<0.5.0`) has been **retired** as a dependency. Do not write `from lsms.tools import get_food_prices, get_food_expenditures, get_household_roster` or any other `lsms.tools` import in new code. Use `get_dataframe` and `df_data_grabber` from `lsms_library.local_tools` instead. ~15 legacy country-level files still carry dead `lsms.tools` imports; ignore them as templates.

## Prerequisites

- The reference implementation (usually Uganda) already has the feature
- The target country has raw survey data (`.dta` files tracked via DVC) containing the needed variables
- The target country directory exists under `lsms_library/countries/{Country}/`

## Workflow

### Step 1: Define the target schema

Read the reference country's `data_scheme.yml` to learn the feature's schema:

```
lsms_library/countries/Uganda/_/data_scheme.yml
```

Note:
- **Index levels** — e.g., `(i, pid, t)` for individual-level or `(i, t)` for household-level
- **Column names and types** — e.g., `Sex: str`, `Age: int`, `Relation: str`
- The feature name itself (e.g., `household_roster`, `earnings`, `assets`)

Also read 1-2 of the reference country's `data_info.yml` files to see how the raw variable mappings work:
```
lsms_library/countries/Uganda/{wave}/_/data_info.yml
```

If the reference feature is marked `!make` (no YAML schema), read the `.py` scripts and inspect the output parquet to determine the schema empirically.

### Step 2: Inventory the target country

For the target country, determine:

1. **Available waves** — list subdirectories (e.g., `2019-20`, `2020-21`)
2. **Existing features** — check `data_scheme.yml` (if it exists) and/or `Makefile`
3. **Existing `data_info.yml` files** — check each wave's `_/` directory
4. **Available raw data** — list `.dta.dvc` files in each wave's `Data/` directory

If no `data_scheme.yml` exists, create one. Include existing Makefile features with the `!make` tag so they keep working:

```yaml
Country: Tanzania

Data Scheme:
  new_feature:
    index: (i, pid, t)
    Col1: str
    Col2: int

  existing_makefile_feature: !make
```

### Step 3: Find raw source files

Identify which `.dta` file in each wave contains the needed variables.

**Primary method: World Bank data dictionary.** Each wave has a `Documentation/SOURCE.org` file with a URL like `https://microdata.worldbank.org/index.php/catalog/XXXX/get-microdata`. Replace `get-microdata` with `data-dictionary` to browse the variable catalog online. This is the authoritative source for which module contains which variables.

**IMPORTANT:** Module letters are NOT consistent across surveys. For example, shocks data lives in Module U in Malawi 2010+ but Module AB in Malawi 2004-05, and Module S in Uganda. Always verify via the data dictionary — never assume.

**Secondary methods** (in order of reliability):
1. **Existing `.py` scripts** in the same wave — they've been tested and reveal variable names
2. **XML documentation** in `{wave}/Documentation/*.xml` — grep for variable labels
3. **Questionnaire PDFs** in `{wave}/Documentation/`
4. **Naming convention inference** — within a wave, variables follow patterns (e.g., `hh_b02`, `hh_b03`, `hh_b04`)

**Always pull the data and inspect it directly:**
```python
from ligonlibrary.dataframes import from_dta
df = from_dta('/path/to/file.dta')
print(df.columns.tolist())
print(df.shape)
for c in df.columns:
    print(f'  {c}: {df[c].dropna().value_counts().head(3).to_dict()}')
```

### Step 4: Map variable names per wave

Variable names differ across waves AND across survey instruments. Build a complete mapping table by inspecting the actual data:

| Wave | File | i (hhid) | Index vars | Target col 1 | Target col 2 | ... |
|------|------|----------|------------|--------------|--------------|-----|

**Key patterns:**
- Household ID changes per wave (e.g., `case_id`, `y2_hhid`, `y3_hhid`, `y4_hhid` in Malawi)
- Check existing `data_info.yml` entries for other features in the same wave to learn the correct `i` variable
- Value labels may differ in capitalization across waves (e.g., "Decrease" vs "DECREASE" vs "Did Not Change" vs "Did not change") — mappings must handle all variants
- Subdirectory structure varies: some waves have flat `Data/`, others have `Data/Cross_Sectional/` and `Data/Panel/`

### Step 5: Decide YAML vs Python script

**Use `data_info.yml`** (strongly preferred) when:
- Column extraction from one or two files
- Value transformations can be expressed as dictionary mappings
- Standard index variables available directly

The YAML `mapping` feature is powerful — it supports:

**Simple column extraction:**
```yaml
HowCoped0: hh_u04a
```

**Value mapping (recode categories):**
```yaml
AffectedIncome:
    - hh_u03a
    - mapping:
        Decrease: True
        DECREASE: True
        Did not change: False
        Did Not Change: False
        DID NOT CHANGE: False
        Increase: True
        INCREASE: True
```

**Dual-column mapping (derive two target columns from one source):**
Reference the same source variable twice with different mappings:
```yaml
AffectedIncome:
    - ab04
    - mapping:
        Income loss: True
        Asset loss: False
        Loss of both: True
AffectedAssets:
    - ab04
    - mapping:
        Income loss: False
        Asset loss: True
        Loss of both: True
```

**Multiple source files (cross-sectional + panel):**
```yaml
file:
    - Cross_Sectional/HH_MOD_B.dta
    - Panel/hh_mod_b_19.dta:
        i: y4_hhid
        pid: id_code
```

**Function-based index transforms:**
```yaml
idxvars:
    i:
        - case_id
        - function: cs_i
```

**Categorical mapping tables** (`categorical_mapping.org`) for cross-wave harmonization:

When the same concept has different labels across waves (e.g., food item names, district spellings, shock types), use an org-mode table in `{Country}/_/categorical_mapping.org` rather than repeating mappings in every `data_info.yml`:

```org
#+NAME: harmonize_food
| Preferred Label | 2004-05              | 2010-11                    | 2019-20                    |
|-----------------+----------------------+----------------------------+----------------------------|
| Maize Flour     | Maize ufa mgaiwa ... | Maize ufa mgaiwa (normal f | Maize ufa mgaiwa (normal f |
| Rice            | Rice                 | Rice                       | Rice                       |
```

Reference from `data_info.yml` using the `mapping` key with a list argument:
```yaml
j:
    - raw_variable_name
    - mapping: ['harmonize_food', 'Original Label', 'Preferred Label']
```

This tells the library: "look up the org table named `harmonize_food`, use the `Original Label` column as keys and `Preferred Label` column as values."

**Resolved bugs (fixed 2026-03-19):**
- Both `mapping:` and `mappings:` (plural) are now accepted in `data_info.yml`. Mali uses the plural form for categorical table references in `idxvars`.
- Categorical mapping table lookups (e.g., `['harmonize_food', 'Original Label', 'Preferred Label']`) now work correctly for both `idxvars` and `myvars`. The `.loc` bug that treated column names as row labels has been fixed.

**When to use which:**
- `mapping:` (inline dict) — for value transforms like Decrease→True, or simple recoding. Works for both `idxvars` and `myvars`.
- `mapping:` or `mappings:` (categorical table reference) — for harmonizing labels across waves via `categorical_mapping.org`. Works for both `idxvars` and `myvars`. Mali uses this pattern for food items and units.

**Use a `.py` script** only when:
- Multi-round files that need splitting by a `round` column
- Complex joins across multiple files
- Derived calculations (e.g., onset timing from dates)

**Avoid aggregation in YAML or scripts.** The library's principle is to pass data in full detail with a uniform interface. Aggregation (groupby, sum, etc.) is the analyst's decision. For example, assets should be item-level `(i, t, j)` with quantity and value per item — not summed to a household total.

### Step 6: Write the configuration

1. Add the feature entry to `data_scheme.yml`
2. For each wave, add a `shocks:` (or similar) block to `data_info.yml`
3. Columns that don't exist in some waves will naturally be NaN — this is fine

**When a wave uses a different module/structure:** Write a separate mapping for that wave. For example, if 2004-05 uses Module AB while all other waves use Module U, each wave's `data_info.yml` simply points to the correct file with the correct variable names.

### Step 7: Verify

**Run the built-in sanity checker:**
```python
from lsms_library.diagnostics import is_this_feature_sane
import lsms_library as ll

c = ll.Country('{Country}')
df = c.{feature_name}()

report = is_this_feature_sane(df, country='{Country}', feature='{feature_name}')
report.summarize()
assert report.ok  # True if no checks failed (warnings allowed)
```

This runs 13 checks: non-empty, index levels match `data_scheme.yml`, no null indices, time/household indices present, reasonable size, no all-null or constant columns, declared columns present, dtype consistency, duplicate rate, and household ID overlap with the spine.

**The feature is not done until `report.ok` is True.**

Additionally, spot-check the data:
```python
# Coverage
print(sorted(df.index.get_level_values('t').unique()))  # All waves?
print(df.groupby('t').size())  # Rows per wave

# Values
has_data = df.dropna(subset=['{key_column}'])
print(has_data['{key_column}'].value_counts())  # Sensible?
print(has_data.head(10))
```

Run existing tests: `pytest tests/test_table_structure.py`

### Step 8: Review for consolidation

After the feature works, check for repeated inline `mapping:` dicts that could be consolidated into a `categorical_mapping.org` table. Common candidates:
- **Index labels** that vary across waves (shock types, food item names, asset names) — these benefit most from a centralized harmonization table
- **Value mappings** repeated identically across waves (e.g., Decrease→True in every wave's effect columns) — less urgent, but a shared table reduces maintenance burden

If you see the same mapping dict copy-pasted across 3+ wave `data_info.yml` files, consider creating a named table in `{Country}/_/categorical_mapping.org` and referencing it with `- mapping: ['table_name', 'Original Label', 'Preferred Label']`.

**Note:** Categorical mapping table references now work for both `myvars` and `idxvars` (fixed 2026-03-19). When multiple source files have different column structures (e.g., some files lack quantity/unit columns), the library fills missing columns with NaN automatically when `missing_ok` is enabled (triggered when the YAML `file:` lists multiple files).

## food_acquired: YAML reference pattern

The EHCVM surveys (Mali, Burkina Faso, Niger, Senegal) share a common food consumption module with standard variable names. Use this as a template for new EHCVM countries:

```yaml
food_acquired:
    file: s07b_me_{country}{year}.dta
    idxvars:
        v: grappe
        visit: vague
        i:
            - grappe
            - menage
        j:
            - s07bq01
            - mappings: ['harmonize_food', 'Original Label', 'Preferred Label']
        u:
            - s07bq03b
            - mappings: ['unit', 'Original Label', 'Preferred Label']
    myvars:
        Quantity: s07bq03a
        Expenditure: s07bq08
        Produced: s07bq04
```

The corresponding `data_scheme.yml` entry:
```yaml
  food_acquired:
    index: (t, v, visit, i, j, u)
    Quantity: float
    Expenditure: float
    Produced: float
```

**Key lessons from Burkina Faso 2014:** Older (pre-EHCVM) waves often use completely different variable names and may have different column availability across survey passages. When listing multiple files (passages), columns absent from some files are automatically filled with NaN --- use this to include all available data even when some passages lack quantity detail.

**Countries with legacy Python scripts** (e.g., Uganda, Malawi) use `!make` in `data_scheme.yml` to bypass schema normalization. Prefer the YAML approach for new work.

## Common pitfalls

- **Wrong module:** Module letters change across survey instruments (U=shocks in one country, U=livestock in another). Always check the World Bank data dictionary.
- **Wrong hhid variable:** Each wave uses a different household ID. Check existing `data_info.yml` entries for the same wave to find the correct `i` variable.
- **Case-sensitive value labels:** "Decrease" vs "DECREASE" vs "Did not change" vs "DID NOT CHANGE". Include all variants in mappings.
- **File path case sensitivity:** `HH_MOD_U.dta` vs `hh_mod_u.dta`. Match the actual filename.
- **Subdirectory structure:** Some waves use `Data/Cross_Sectional/` and `Data/Panel/` subdirectories; others have files directly in `Data/`.
- **Multi-round files:** A single `.dta` containing multiple survey rounds (e.g., Tanzania 2008-15) cannot be handled by `data_info.yml` alone — use a `.py` script.
- **Missing columns across waves:** Earlier survey instruments may not include all variables. Columns absent from a wave's `data_info.yml` entry will be NaN in the output — this is expected.
- **DVC-tracked files:** Pull data with `dvc pull {path}.dvc` before building. Run from the DVC root (`lsms_library/countries/`).
- **Pre-ISA vs ISA waves:** Earlier waves (e.g., Malawi 2004-05 "IHS2") predate the LSMS-ISA standardization and often use completely different module letters and variable naming conventions. Module L might be "non-food expenditures" in 2004-05 but "durable goods" in 2010+. Always verify via the World Bank data dictionary — never assume module letters are stable across survey instruments.
