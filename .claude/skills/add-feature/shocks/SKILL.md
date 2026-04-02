---
name: shocks
description: Use this skill to add the shocks feature to an LSMS-ISA country. This skill should be used when a user wants to add household shock data (natural disasters, economic shocks, coping strategies) to a country that doesn't yet have a shocks table. Requires the add-feature skill for general workflow.
---

# Add Shocks Feature to LSMS Country

This skill provides domain-specific guidance for adding the `shocks` table to an LSMS-ISA country. Use in conjunction with the general `add-feature` skill.

## Target schema

```yaml
shocks:
    index: (t, i, Shock)
    EffectedIncome: bool
    EffectedAssets: bool
    EffectedProduction: bool
    EffectedConsumption: bool
    HowCoped0: str
    HowCoped1: str
    HowCoped2: str
```

- **Index:** household (`i`) × wave (`t`) × shock type (`Shock`)
- **Effect columns:** boolean — did the shock affect income/assets/production/consumption?
- **Coping columns:** categorical — up to 3 coping strategies per shock

Reference: Uganda's shocks implementation in `lsms_library/countries/Uganda/` (uses `.py` scripts with `!make`).

## Finding the shocks module

**The module letter for shocks varies by country and even by wave within a country.** Do NOT assume it is always the same letter.

| Country | Module | File pattern | Notes |
|---------|--------|-------------|-------|
| Uganda | Section 16 | `GSEC16.dta` | |
| Malawi 2010+ | Module U | `hh_mod_u.dta` / `HH_MOD_U.dta` | |
| Malawi 2004-05 | Module AB | `sec_ab.dta` | Earlier survey instrument |
| Tanzania | Section S | `HH_SEC_S.dta` / `hh_sec_s.dta` | |
| Ethiopia | Module 8 | `sect8_hh_*.dta` | |

**Always verify** by checking the World Bank data dictionary for the specific catalog number. Each wave's `Documentation/SOURCE.org` has the catalog URL — append `/data-dictionary` to browse modules online.

## Variable mapping patterns

### Standard pattern (Malawi 2010+, most countries)

Most LSMS-ISA shocks modules follow this structure:

| Target | Typical variable | Description |
|--------|-----------------|-------------|
| Shock (index) | `hh_u0a` | Shock type (categorical) |
| — (filter) | `hh_u01` | Whether experienced (Yes/No) |
| EffectedIncome | `hh_u03a` | Effect on income |
| EffectedAssets | `hh_u03b` | Effect on assets |
| EffectedProduction | `hh_u03c` | Effect on food production |
| EffectedConsumption | `hh_u03d` | Effect on food consumption |
| HowCoped0 | `hh_u04a` | First coping strategy |
| HowCoped1 | `hh_u04b` | Second coping strategy |
| HowCoped2 | `hh_u04c` | Third coping strategy |

The effect variables typically have three values that must be mapped to boolean:

```yaml
EffectedIncome:
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

**Include all capitalization variants** — they differ across waves even within the same country.

### Combined-effect pattern (older survey instruments)

Some earlier surveys (e.g., Malawi 2004-05) combine income and asset effects into a single variable:

| Value | EffectedIncome | EffectedAssets |
|-------|---------------|---------------|
| "Income loss" | True | False |
| "Asset loss" | False | True |
| "Loss of both" | True | True |

Handle by referencing the same source column twice with different mappings:

```yaml
EffectedIncome:
    - ab04
    - mapping:
        Income loss: True
        Asset loss: False
        Loss of both: True
EffectedAssets:
    - ab04
    - mapping:
        Income loss: False
        Asset loss: True
        Loss of both: True
```

Columns not available in the older instrument (e.g., `EffectedProduction`, `EffectedConsumption`) will naturally be NaN — this is expected and correct.

### Uganda pattern (derived fields)

Uganda's shocks script computes additional derived columns not typically available from raw data:
- `Year` — shock year, derived from onset month and interview date
- `Onset` — months between shock start and interview
- `Duration` — from raw data

These require complex date arithmetic and joins with interview date files. When adding shocks to a new country, start with the core columns (effects + coping) and add derived timing fields later if the raw data supports it.

## Harmonizing shock type labels with categorical_mapping.org

Shock type labels vary across waves within a country and across countries. Use `categorical_mapping.org` to harmonize the `Shock` index values to canonical labels. This is the same mechanism used for food item names.

Create a `#+NAME: harmonize_shocks` table in `{Country}/_/categorical_mapping.org`:

```org
#+NAME: harmonize_shocks
| Preferred Label                | 2010-11                          | 2019-20                          |
|--------------------------------+----------------------------------+----------------------------------|
| Drought                        | Drought                          | Drought                          |
| Floods                         | Floods                           | Floods                           |
| High Food Prices               | Unusually High Prices for Food   | Unusually High Prices for Food   |
| Crop Pests or Disease          | Unusually High Level of Crop ... | Unusually High Level of Crop ... |
```

Reference from `data_info.yml`:
```yaml
shocks:
    file: HH_MOD_U.dta
    idxvars:
        i: case_id
        Shock:
            - hh_u0a
            - mappings: ['harmonize_shocks', 'Original Label', 'Preferred Label']
```

This is especially valuable for:
- Cross-country comparisons where the same shock has different labels
- French/English label harmonization (e.g., "Sécheresse" → "Drought")
- Aggregating fine-grained shock categories to broader ones

The inline `mapping:` dict (Decrease→True) is still the right tool for the boolean effect columns, since that's a value transform rather than label harmonization.

## Typical shock types across countries

Shock type labels vary but cluster around these categories:
- **Agricultural:** Drought, floods, crop pests/disease, livestock disease, irregular rains
- **Economic:** High food prices, high input prices, low crop prices, business failure
- **Health:** Illness/accident of household member, death of income earner
- **Crime/conflict:** Theft, conflict/violence
- **Other:** Dwelling damage, household breakup, loss of employment, end of aid/remittances

## Worked example: Malawi

Malawi has 5 waves. The shocks module changes between the first and subsequent waves:

| Wave | File | i | Shock | Effects | Coping |
|------|------|---|-------|---------|--------|
| 2004-05 | `sec_ab.dta` | `case_id` | `ab02` | `ab04` (combined) | `ab07a/b/c` |
| 2010-11 | `Full_Sample/Household/hh_mod_u.dta` | `case_id` | `hh_u0a` | `hh_u03a-d` (separate) | `hh_u04a/b/c` |
| 2013-14 | `HH_MOD_U_13.dta` | `y2_hhid` | `hh_u0a` | `hh_u03a-d` | `hh_u04a/b/c` |
| 2016-17 | `Cross_Sectional/hh_mod_u.dta` | `case_id` | `hh_u0a` | `hh_u03a-d` | `hh_u04a/b/c` |
| 2019-20 | `Cross_Sectional/HH_MOD_U.dta` | `case_id` | `hh_u0a` | `hh_u03a-d` | `hh_u04a/b/c` |

### Reviewing agent output

When agents add shocks to multiple countries, watch for:
- Repeated inline `mapping:` dicts for Decrease/Increase→boolean across waves — these can be consolidated into a `categorical_mapping.org` table once the idxvars bug is fixed
- Shock type labels that vary across waves (capitalization, truncation, typos) — prime candidates for a `harmonize_shocks` table in `categorical_mapping.org`
- French/English label mixing in West African countries (Niger, Mali, Burkina Faso) — a harmonization table can map both to canonical English labels for cross-country analysis

Key lessons from this implementation:
1. The 2004-05 wave uses a completely different module (AB vs U) and variable structure
2. Household ID changes: `case_id` in most waves, `y2_hhid` in 2013-14
3. File paths change: flat in early waves, `Cross_Sectional/` subdirectory in later waves
4. All 5 waves handled purely via `data_info.yml` — no Python scripts needed
5. The raw data includes rows for all possible shock types per household (experienced or not); rows where the shock was not experienced have NaN effect values

## EHCVM Cope1-26 binary → HowCoped0-2 text conversion

EHCVM surveys (Benin, Togo, Guinea-Bissau, Senegal, Burkina Faso, Mali, Niger) store coping strategies as **26 binary indicator columns** (`s14q05__1` through `s14q05__26`) rather than the 3 text labels (HowCoped0/1/2) used by other LSMS-ISA countries.

**This cannot be handled by YAML alone.** A formatting function `shocks(df)` is required in the wave's Python file (e.g., `2018-19.py`) to:
1. Iterate over Cope1-Cope26 columns for each row
2. Collect the first 3 strategies where the value is ≥ 1
3. Map strategy numbers to French text labels via a `COPING_LABELS` dict
4. Assign to HowCoped0, HowCoped1, HowCoped2 columns
5. Drop the original Cope columns

Reference implementations: `Senegal/2018-19/_/2018-19.py`, `Benin/2018-19/_/2018-19.py`, `Togo/2018/_/2018.py`, `Burkina_Faso/2018-19/_/2018-19.py`.

The `data_info.yml` declares all 26 Cope columns as myvars; the formatting function transforms them post-extraction. The `data_scheme.yml` declares only `HowCoped0/1/2: str` (not the 26 Cope columns).

**Note on Affected* columns**: EHCVM surveys DO have effect data (s14q04a-d with French labels: Diminué, Augmenté, Inchangé, Non-concerné). The mapping dicts must use French labels. If these columns appear all-null, the most likely cause is stale cached parquets — purge the cache and rebuild.
