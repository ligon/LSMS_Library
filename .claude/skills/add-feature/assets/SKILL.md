---
name: assets
description: Use this skill to add the assets (durable goods) feature to an LSMS-ISA country. This skill should be used when a user wants to add household asset/durable goods ownership data to a country that doesn't yet have an assets table. Requires the add-feature skill for general workflow.
---

# Add Assets Feature to LSMS Country

This skill provides domain-specific guidance for adding the `assets` table to an LSMS-ISA country. Use in conjunction with the general `add-feature` skill.

## Target schema

```yaml
assets:
    index: (t, i, j)
    Quantity: float
    Age: float
    Value: float
    Purchased Recently: str
    Purchase Price: float
```

- **Index:** wave (`t`) × household (`i`) × item (`j`, the asset type name)
- **Quantity:** number of items owned
- **Age:** age of item in years
- **Value:** current estimated value in local currency
- **Purchased Recently:** whether purchased in the last 12 months (YES/NO)
- **Purchase Price:** price paid if recently purchased (sparse — only for recent purchases)

**Design principle:** Pass item-level data without aggregation. Do NOT sum to household totals — that is the analyst's decision. The library provides the detailed data with a uniform interface.

## Finding the assets module

The durable goods module letter varies by country and survey instrument:

| Country | ISA waves | Pre-ISA waves | Notes |
|---------|-----------|---------------|-------|
| Uganda | Section 14 (`gsec14`) | — | |
| Malawi | Module L (`hh_mod_l`) | Module M (`sec_m1`) | L=expenditures in 2004-05! |
| Tanzania | Section N (`HH_SEC_N`) | — | |
| Ethiopia | Section 10 (`sect10_hh`) | — | |
| Nigeria | Section 5 (`sect5_plantingwN`) | — | Verify per wave |

**IMPORTANT:** Module letters are NOT stable across survey instruments. Malawi's Module L is "non-food expenditures" in 2004-05 but "durable goods" in 2010+. Always verify via the World Bank data dictionary.

## Variable mapping patterns

### Standard ISA pattern (Malawi 2010+, most countries)

Most LSMS-ISA asset modules follow this structure:

| Target | Typical variable | Description |
|--------|-----------------|-------------|
| j (index) | `hh_l02` | Asset item name (categorical) |
| — (filter indicator) | `hh_l01` | Does HH own this item? (Yes/No) |
| Quantity | `hh_l03` | Number owned |
| Age | `hh_l04` | Age in years |
| Value | `hh_l05` | Current estimated value |
| Purchased Recently | `hh_l06` | Purchased in last 12 months? |
| Purchase Price | `hh_l07` | Amount paid (if recently purchased) |

### Pre-ISA pattern (Malawi 2004-05)

Earlier surveys use different variable prefixes and may split across multiple files:

| Target | Variable | Notes |
|--------|----------|-------|
| j (index) | `m0a` | Asset item name |
| — (filter) | `m01a` | Owns? (Yes/No) |
| Quantity | `m03a` | Number owned |
| Age | `m04a` | Age in years |
| Value | `m05a` | Current estimated value |
| Purchased Recently | `m06a` | Purchased recently? |
| Purchase Price | — | `m07a` is year of purchase, not price |

## Handling non-owned items

The raw data includes one row per asset type per household, regardless of ownership. Non-owned items have `hh_l01 = 'No'` and all data columns (Quantity, Age, Value) are NaN.

**Do NOT filter these out in the YAML config.** Include all rows — the analyst can use `df.dropna(subset=['Quantity'])` to get only owned items. This preserves the principle of passing data without making analytical decisions.

However, do NOT include the ownership indicator (`hh_l01`) as a column — it adds no information beyond what the NaN pattern already conveys.

## Columns that vary across waves

Not all waves have the same columns:
- **Purchased Recently** and **Purchase Price** may be absent in some waves (e.g., Malawi 2013-14). These will naturally be NaN for those waves.
- **Item names** may vary slightly across waves (e.g., "Radio ('wireless')" vs "Radio (wireless)"). This is raw data variation — do not attempt to harmonize item names.

## Worked example: Malawi

| Wave | File | i | j (item) | Quantity | Age | Value | Purchased Recently | Purchase Price |
|------|------|---|----------|----------|-----|-------|-------------------|---------------|
| 2004-05 | `sec_m1.dta` | `case_id` | `m0a` | `m03a` | `m04a` | `m05a` | `m06a` | — |
| 2010-11 | `Full_Sample/Household/hh_mod_l.dta` | `case_id` | `hh_l02` | `hh_l03` | `hh_l04` | `hh_l05` | `hh_l06` | `hh_l07` |
| 2013-14 | `HH_MOD_L_13.dta` | `y2_hhid` | `hh_l02` | `hh_l03` | `hh_l04` | `hh_l05` | — | — |
| 2016-17 | `Cross_Sectional/hh_mod_l.dta` | `case_id` | `hh_l02` | `hh_l03` | `hh_l04` | `hh_l05` | `hh_l06` | `hh_l07` |
| 2019-20 | `Cross_Sectional/HH_MOD_L.dta` | `case_id` | `hh_l02` | `hh_l03` | `hh_l04` | `hh_l05` | `hh_l06` | `hh_l07` |

Key lessons:
1. 2004-05 uses Module M (`sec_m1.dta`), not Module L — Module L is expenditures in that instrument
2. All 5 waves handled via pure `data_info.yml`, no Python scripts needed
3. No aggregation — item-level data passed through directly
4. 2013-14 lacks purchase columns; those are NaN for that wave
5. ~11% of rows represent owned items; the rest are all-NaN "No" responses
