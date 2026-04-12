---
name: housing
description: Use this skill to add the housing feature to an LSMS-ISA country. This skill should be used when a user wants to add dwelling material data (roof, floor) to a country that doesn't yet have a housing table. Requires the add-feature skill for general workflow.
---

# Add Housing Feature to LSMS Country

This skill provides domain-specific guidance for adding the `housing` table to an LSMS-ISA country. Use in conjunction with the general `add-feature` skill.

## Target schema

```yaml
housing:
    index: (t, i)
    Roof: str
    Floor: str
```

- **Index:** household (`i`) x wave (`t`)
- **Roof:** categorical string — predominant roof material (e.g. Grass, Iron Sheets, Clay Tiles)
- **Floor:** categorical string — predominant floor material (e.g. Smoothed Mud, Smooth Cement, Sand)

Reference: Uganda (`lsms_library/countries/Uganda/`) and Malawi (`lsms_library/countries/Malawi/`).

## Design decisions

- **Categorical strings, not binary.** Material names are preserved as-is (Title Case). Consumers wanting binary indicators derive trivially: `df['Roof'] == 'Grass'`.
- **Walls excluded.** Data is typically available (same module, adjacent variable) but excluded from the current schema. Can be added later by extending `data_scheme.yml`.
- **Labels normalized to Title Case** across waves via inline `mapping:` dicts in `data_info.yml`. Raw Stata labels vary in case across survey waves (Title Case, ALL CAPS, mixed).

## Finding the housing module

**The housing module letter is NOT stable across surveys or even across waves within one country.**

| Country | Module | Variables |
|---------|--------|-----------|
| Uganda  | GSEC9 / GSEC11 | h9q04 (Roof), h9q06 (Floor) |
| Malawi IHS2 (2004-05) | Section G | g09 (Roof), g10 (Floor) |
| Malawi IHS3+ (2010--) | Module F | hh_f08 (Roof), hh_f09 (Floor) |

**Always check the World Bank data dictionary** for each wave. Replace `get-microdata` with `data-dictionary` in the catalog URL from `Documentation/SOURCE.org`. Search file descriptions for "Housing" or "Dwelling".

Common traps:
- Module K in Malawi is non-food expenditures, not housing
- Module F in some countries is land/agriculture, not housing
- The same letter can mean different things across survey instruments (IHS2 vs IHS3)

## Variable patterns

Housing materials are typically 3 adjacent questions in the dwelling characteristics module:

1. **Outer walls** — "What material are the outer walls predominantly made of?"
2. **Roof** — "What material is the roof predominantly made of?"
3. **Floor** — "What material is the floor predominantly made of?"

Each has an `_os` or `_oth` companion for "Other (specify)" free text. We ignore those.

## Case normalization

Labels vary in case across waves:
- Title Case: `Iron sheets`, `Smoothed mud` (common in older waves)
- ALL CAPS: `IRON SHEETS`, `SMOOTHED MUD` (common in IHPS/panel waves)
- Mixed: `grass`, `IRON SHEETS`, `concrete` (IHS4/IHS5 cross-sectional)
- Parenthetical local names: `MUD BRICK(UNFIRED)`, `COMPACTED EARTH(YAMDINDO)` (IHS4+)

Normalize all to Title Case with inline `mapping:` dicts per wave. Could consolidate into `categorical_mapping.org` tables named `Roof` and `Floor` if the mapping grows unwieldy across many countries.

## Cross_Sectional + Panel merge

Some countries (Malawi IHS4/IHS5) have separate Cross_Sectional and Panel data files. Follow the `household_roster` pattern for that wave:

```yaml
housing:
    file:
        - Cross_Sectional/hh_mod_f.dta
        - Panel/hh_mod_f_16.dta:
            i: y3_hhid
    idxvars:
        i:
            - case_id
            - mapping: cs_i
    myvars:
        Roof:
            - hh_f08
            - mapping:
                GRASS: Grass
                ...
```

## Verification

```python
from lsms_library.diagnostics import is_this_feature_sane
import lsms_library as ll

c = ll.Country('{Country}')
df = c.housing()
report = is_this_feature_sane(df, country='{Country}', feature='housing')
report.summarize()
assert report.ok
```

The `index_levels_match_scheme` check may warn about an extra `v` level — this is expected (auto-joined from `sample` by `_join_v_from_sample()`).
