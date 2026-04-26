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

## Variable-name differences between rounds

Source variable names often differ between pp and ph files within
the same wave.  Always check both files before defining `myvars`:

```python
import pyreadstat
_, meta_pp = pyreadstat.read_dta('../Data/sect1_plantingw4.dta', metadataonly=True)
_, meta_ph = pyreadstat.read_dta('../Data/sect1_harvestw4.dta', metadataonly=True)
print('PP columns:', meta_pp.column_names)
print('PH columns:', meta_ph.column_names)
```

Concrete example (Nigeria 2018-19 `household_roster`): the Age
variable is `s1q6` in the planting file but `s1q4` in the harvest
file — same concept, different name.  Define separate `myvars`
dicts per round whenever names diverge.

## Script template

Skeleton for a pp/ph feature script.  Replace the bracketed
placeholders with the country / feature / wave specifics:

```python
#!/usr/bin/env python
"""Extract {feature_name} for {Country} {wave}.

Source files:
  - {pp_file}  (post-planting, t={pp_t})
  - {ph_file}  (post-harvest,  t={ph_t})
"""
import sys
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

# --- Post-planting ---
idxvars_pp = dict(
    i='hhid',
    t=('hhid', lambda x: '{pp_t}'),
    v='ea',
    # pid='indiv',  # if person-level
)
myvars_pp = dict(
    # Column mappings for planting file
)
pp = df_data_grabber('../Data/{pp_file}', idxvars_pp, **myvars_pp)

# --- Post-harvest ---
idxvars_ph = dict(
    i='hhid',
    t=('hhid', lambda x: '{ph_t}'),
    v='ea',
    # pid='indiv',
)
myvars_ph = dict(
    # Column mappings for harvest file (may differ from pp)
)
ph = df_data_grabber('../Data/{ph_file}', idxvars_ph, **myvars_ph)

# --- Combine ---
df = pd.concat([pp, ph])
df = df.replace('', pd.NA).sort_index().dropna(how='all')

to_parquet(df, '{feature_name}.parquet')
```

## Reference implementations

- **Nigeria `household_roster`**:
  `Nigeria/2018-19/_/household_roster.py` — canonical pp/ph pattern
  with distinct `t` values and attrition handling.
- **Nigeria `food_acquired`**: `Nigeria/2018-19/_/food_acquired.py`
  — pp/ph with food item and unit harmonization across rounds.
- **Tanzania `2008-15/`**: multi-round single-file pattern with
  `round` column mapping. See `.claude/skills/multi-round-waves.md`.

## Relationship to `multi-round-waves`

Some pp/ph countries also use the `wave_folder_map` mechanism (see
`.claude/skills/multi-round-waves.md`).  Tanzania `2008-15/` is the
canonical case: it is *both* a multi-round folder (rounds 1–4 in
one directory) and uses a single-file pattern with a `round`
column rather than separate pp/ph files.  These are distinct
mechanisms that can co-occur:

- **pp/ph** = two source files per wave, each needing a different
  `t` → solved by script-level `t` assignment.
- **multi-round-waves** = multiple logical waves sharing one
  physical directory → solved by `Wave(year=..., wave_folder=...)`
  and filtering by `t`.

Both require `materialize: make`.  A single script can handle both
patterns (e.g., Tanzania `2008-15/_/food_acquired.py`).

## Common bugs and how to avoid them

| Bug | Cause | Fix |
|-----|-------|-----|
| 50–87% duplicate indices | Both rounds assigned the same `t` value | Assign distinct `t` values (e.g. `2018Q3` / `2019Q1`) |
| Missing half the data | Loaded only pp or only ph | Load both files and concatenate |
| Wrong variable names | Used pp variable names for ph file (or vice versa) | Check column names in both files separately |
| Ghost rows after concat | Individual enumerated in pp but absent in ph | `dropna(how='all')` after concat |
| Double-counted households | Concatenated without distinct `t` on a household-level feature | Assign distinct `t` values; for hh-level features, also confirm data is truly from both rounds |
| `data_info.yml` used for pp/ph feature | YAML cannot express two-file + distinct-`t` pattern | Switch to `materialize: make` with a `.py` script |

## Checklist for adding a pp/ph feature

1. Confirm the country has dual-round structure (file naming in `Data/`).
2. Identify which source files contain the needed variables for each round.
3. Check variable names in **both** pp and ph files (they may differ).
4. Write a `.py` script that:
   - Loads pp file with distinct `t` value
   - Loads ph file with different `t` value
   - Concatenates and deduplicates
   - Uses `df_data_grabber()` for data access
   - Uses `to_parquet()` for output
5. Mark the feature as `materialize: make` in `data_scheme.yml`.
6. Verify with `is_this_feature_sane()` — duplicate rate should be small.
7. Confirm both rounds appear in `df.index.get_level_values('t').unique()`.

## Relationship to the `add-feature` parent skill

This skill extends the general `add-feature` workflow.  When the
target country is identified as a pp/ph country:

- **YAML vs Script step**: always **script** for features that use
  both rounds' data.
- **Configuration step**: use the script template above; the
  `data_scheme.yml` entry must include `materialize: make`.
- **Verify step**: pay special attention to duplicate rates and
  `t` value coverage in diagnostics.

All other steps in the `add-feature` workflow apply unchanged.
