---
name: add-feature/pp-ph
description: Use this skill when adding or debugging a feature for a country whose wave has both a post-planting (pp) and post-harvest (ph) round in a single wave directory. Covers distinct-t-value construction, the canonical duplicate-index bug, and the attrition-drop pattern.
---

# Post-Planting / Post-Harvest (PP/PH) Countries

Several LSMS-ISA countries collect data in two rounds per wave: a
**post-planting** visit followed by a **post-harvest** visit to the
same household. This is a cross-cutting structural concern and the
**single most common source of duplicate-index bugs** in these
countries.

## Which countries have dual-round structure

| Country | Program | Waves affected | File naming pattern | Notes |
|---------|---------|----------------|---------------------|-------|
| **Nigeria** | GHS/LSMS-ISA | All waves | `sect*_plantingw*.dta` / `sect*_harvestw*.dta` | Wave dirs like `2018-19/` contain both pp and ph data |
| **Ethiopia** | ESS | All 5 waves | `sect*_pp_w*.dta` / `sect*_ph_w*.dta` | Heavy `!make` usage — most features need scripts |
| **Tanzania** | NPS | `2008-15/` only | Single file with `round` column covering rounds 1–4 | Later waves (`2019-20`, `2020-21`) are single-round; see also `.claude/skills/multi-round-waves.md` |
| **GhanaSPS** | SPS | Some waves | Planting/harvest questionnaires | Less structured than Nigeria/Ethiopia |

## Why YAML cannot express this

The YAML path (`data_info.yml`) assumes **one directory = one `t`
value**. In pp/ph countries, a single wave directory (e.g.
`Nigeria/2018-19/`) contains two source files that need **different
`t` values** (e.g. `2018Q3` post-planting, `2019Q1` post-harvest).
The YAML path has no mechanism to:

1. Load two different source files from the same directory
2. Assign a different `t` value to each
3. Concatenate the results and deduplicate

This is why pp/ph features **must use `materialize: make`** (or
`!make`) with a Python script.

## How pp/ph affects index construction

Each round must receive a **distinct `t` value** so the same
household appearing in both rounds does not create duplicate index
entries. The standard patterns:

- **Nigeria**: quarter-based `t` values — `2018Q3` (pp) and
  `2019Q1` (ph).
- **Ethiopia**: wave-label reuse (e.g. `2018-19`) since most
  features only use one round's data.
- **Tanzania `2008-15/`**: the script reads a `round` column and
  maps values to wave labels (`2008-09`, `2010-11`, `2012-13`,
  `2014-15`). See `.claude/skills/multi-round-waves.md`.

## The duplicate-index bug

This is the most common bug in pp/ph countries. It occurs when both
pp and ph data are loaded but assigned the **same `t` value**:

```python
# BUG: Both rounds get t='2018-19' → household appears twice
pp['t'] = '2018-19'
ph['t'] = '2018-19'
df = pd.concat([pp, ph])   # → 50–87 % duplicate indices
```

Symptoms:

- `df.index.duplicated().mean()` returns 0.50–0.87
- `is_this_feature_sane()` reports massive duplicate rates
- Household counts are roughly double the expected number

## How to fix: the script pattern

Assign distinct `t` values to each round, concatenate, and
deduplicate:

```python
from lsms_library.local_tools import df_data_grabber, to_parquet

# Post-planting: assign t='2018Q3'
idxvars_pp = dict(i='hhid', t=('hhid', lambda x: '2018Q3'), v='ea', pid='indiv')
myvars_pp  = dict(Sex=('s1q2', extract_string),
                  Age='s1q6',
                  Relationship=('s1q3', extract_string))
pp = df_data_grabber('../Data/sect1_plantingw4.dta', idxvars_pp, **myvars_pp)

# Post-harvest: assign t='2019Q1'
idxvars_ph = dict(i='hhid', t=('hhid', lambda x: '2019Q1'), v='ea', pid='indiv')
myvars_ph  = dict(Sex=('s1q2', extract_string),
                  Age='s1q4',
                  Relationship=('s1q3', extract_string))
ph = df_data_grabber('../Data/sect1_harvestw4.dta', idxvars_ph, **myvars_ph)

# Concatenate and drop people who left between rounds
df = pd.concat([pp, ph])
df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, 'household_roster.parquet')
```

Key details:

- The `t` lambda assigns a constant string to every row from that
  file.
- Variable names may differ between pp and ph files (e.g. Nigeria's
  `s1q6` vs `s1q4` for Age).
- `dropna(how='all')` removes individuals who appeared in one round
  but have no data in the other (attrition between rounds).
- The `data_scheme.yml` must use `materialize: make` for these
  features.

## Reference implementations

- **Nigeria `household_roster`**:
  `Nigeria/2018-19/_/household_roster.py` — canonical pp/ph pattern
  with distinct `t` values and attrition handling.
- **Nigeria `food_acquired`**: `Nigeria/2018-19/_/food_acquired.py`
  — pp/ph with food item and unit harmonization across rounds.
- **Tanzania `2008-15/`**: multi-round single-file pattern with
  `round` column mapping. See `.claude/skills/multi-round-waves.md`.
