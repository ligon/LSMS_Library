---
name: panel-ids
description: Use this skill to add panel household ID linkage to an LSMS-ISA country. This skill should be used when a user wants to enable tracking of the same households across survey waves, allowing panel data analysis. Requires the add-feature skill for general workflow.
---

# Add Panel IDs to LSMS Country

This skill provides guidance for adding the `panel_ids` feature, which links household identifiers across survey waves so the library can track the same physical household over time.

## What panel_ids does

Each LSMS-ISA wave assigns households a wave-specific identifier (e.g., `case_id` in wave 1, `y2_hhid` in wave 2). The `panel_ids` feature creates a crosswalk mapping each wave's IDs back to the baseline wave's IDs. The library's `id_walk()` function then uses this mapping to give every household a single canonical ID across all waves.

**Output:** `panel_ids.json` and `updated_ids.json` in the country's `_/` directory.

## Key concepts

### Not all waves are panel-linked

Many LSMS-ISA countries have waves from **different survey programs** that cannot be linked:

| Country | Panel waves | Non-panel waves | Why |
|---------|------------|----------------|-----|
| Malawi | IHS3 2010-11 → IHPS 2013-14 → IHS4 2016-17 → IHS5 2019-20 | IHS2 2004-05 | Different survey instrument |
| Ethiopia | ESS W1-W3 (2011-12 → 2015-16) | ESS W4 (2018-19) | New sample drawn |
| Nigeria | GHS W1-W3 (2010-11 → 2015-16) | NLSS 2018-19 | Different survey (cross-sectional) |
| Niger | ECVMA 2011-12 → 2014-15 | EHCVM 2018-19 | Different program |
| Mali | — | EACI 2014-15, 2017-18 | Not longitudinal |
| Mali | EHCVM 2018-19 → 2021-22 | — | Only within EHCVM |
| Burkina Faso | EHCVM 2018-19 → 2021-22 | EMC 2014 | Different program |

**Always check the World Bank catalog** for each wave to determine if it's part of a panel.

### ID stability patterns

Three patterns for how household IDs work across waves:

1. **Stable IDs** (simplest): The same `hhid` persists across waves. Nigeria GHS-Panel works this way. Panel_ids is an identity mapping.

2. **New IDs with backward link** (most common): Each wave assigns new IDs but includes a variable linking to the previous wave's ID. Examples:
   - Malawi: `y4_hhid` (current) → `y3_hhid` (previous) in the cover sheet
   - Uganda: `hhid` (current) → `hhidold` (previous) in GSEC1
   - Ethiopia: `household_id2` (current) → `household_id` (W1 baseline)

3. **Composite IDs** (West Africa): Household ID is constructed from `grappe` (cluster) + `menage` (household number). The same household keeps the same grappe+menage in the panel wave, but the construction method or variable names may change.

### Disjoint sub-panels

Some countries split their sample into sub-panels tracked in different
subsequent waves.  Tanzania is the key example:

- **2014-15** drew both an "Extended Panel" (~20% of the 2008-13
  cohort) and a "Refresh Panel" (entirely new sample).
- **2019-20** followed the Extended Panel only.
- **2020-21** followed the Refresh Panel only, plus an urban booster.

These sub-panels have **zero household overlap by design**.
`panel_attrition()` correctly shows 0 in the cross-cell.  The
`y4_hhid` in the 2020-21 data uses refresh panel numbering
(e.g. `1000-001`) which matches `r_hhid` values first appearing in
2014-15 --- not the extended panel `r_hhid` values (`0001-001`).

Check the World Bank's Basic Information Document for each wave to
determine if sub-panel splits exist.

### Household splits

Some surveys track household splits --- when one household divides
into two.  The split-off household gets a new ID but its `previous_i`
points to the original household.  The library's `update_id()`
function handles this by appending `_1`, `_2` suffixes.

Niger uses an `extension` variable: `0` = same household same
location, `1` = same household moved, `2` = split-off.

Tanzania 2008-15 uses **suffix-based detection**: within the
multi-round UPD4 data, multiple UPHIs can share the same `r_hhid` in
early rounds and diverge later.  Round 2 (16-digit r_hhid): suffix
`01` = primary, `02`+ = split-off.  Rounds 3-4 (NNNN-NNN): suffix
`001` = primary, others = split-off.  `map_08_15()` only links
primaries backward; split-offs start as new households.

## Implementation patterns

### Pattern 1: YAML-driven (preferred for simple cases)

Add `panel_ids:` to each wave's `data_info.yml`:

```yaml
panel_ids:
    file: Panel/hh_mod_a_filt_19.dta
    idxvars:
        i: y4_hhid
    myvars:
        previous_i: y3_hhid
```

And to `data_scheme.yml`:
```yaml
panel_ids:
    index: (t, i)
    previous_i: str
```

Works when: the cover sheet has a simple current_id → previous_id mapping.

### Pattern 2: Legacy Waves dict + script

For complex cases, define a `Waves` dict in `{country}.py` and a `panel_ids.py` script:

```python
# In ethiopia.py:
Waves = {
    '2011-12': (),  # baseline — no previous
    '2013-14': ('sect_cover_hh_w2.dta', 'household_id2', 'household_id'),
    '2015-16': ('sect_cover_hh_w3.dta', 'household_id2', 'household_id'),
    '2018-19': (),  # new sample — no linkage
}
```

Each tuple: `(cover_file, current_wave_id, previous_wave_id)`. Empty tuple = no linkage (baseline or new sample).

```python
# panel_ids.py:
from ethiopia import Waves
from lsms_library.local_tools import panel_ids
D, updated_ids = panel_ids(Waves)
# ... write to JSON
```

Add `panel_ids: !make` to `data_scheme.yml` and a Makefile target.

### Pattern 3: Custom function (for complex ID construction)

Tanzania's 2008-15 multi-round file and Niger's composite IDs need custom Python:

```python
# Tanzania: map_08_15() groups by UPHI (universal panel ID) and shifts r_hhid
# Niger: parse hhid="grappe-menage-extension", zero-pad, handle splits
```

## Detective work

### Step 1: Determine panel design

Check the World Bank catalog for each wave (`Documentation/SOURCE.org` → catalog URL):
- Is this wave part of a panel?
- What previous wave does it follow?
- Is it a full panel or a subsample (e.g., Malawi IHPS vs IHS)?
- Was a new sample drawn (e.g., Ethiopia W4)?

### Step 2: Find cover sheet / linkage file

The household ID linkage is typically in:
- **Section A / cover sheet**: `hh_mod_a_filt.dta`, `GSEC1.dta`, `HH_SEC_A.dta`, `sect_cover_hh_wN.dta`
- **Tracking files**: `HHTrack.dta` (Nigeria)
- **Section 0**: `s00_me_{country}{year}.dta` (EHCVM countries)

Pull and inspect: look for both a current-wave ID and a previous-wave ID variable.

### Step 3: Verify ID construction consistency

For composite ID countries (grappe+menage), check that the ID is constructed the same way in all features. Compare with existing `.py` scripts in the wave directory.

### Step 4: Cross-reference with World Bank harmonised panel

The World Bank published Stata code for panel merging across all LSMS-ISA countries:
- GitHub: `lsms-worldbank/LSMS-ISA-harmonised-dataset-on-agricultural-productivity-and-welfare`
- The `Append_{COUNTRY}.do` files contain the panel merge logic
- Saved locally at `/var/tmp/lsms-isa-harmonised/reproduction/Reproduction_v2/Code/Cleaning_code/`

**Treat this code as reference for the logic, not as gospel.** It's a "huge ungainly Stata mess" (user's words) but the ID linkage decisions are useful to verify against.

## Verification

```python
from lsms_library.diagnostics import check_panel_consistency
import lsms_library as ll

c = ll.Country('{Country}')
report = check_panel_consistency(c)
report.summarize()
assert report.ok
```

Expected warnings:
- "No ID mappings for wave X" — expected for baseline waves and new-sample waves
- Panel attrition checks may be skipped if `other_features` is not cached

## Common pitfalls

- **Mixing survey programs**: ECVMA ≠ EHCVM, IHS ≠ IHPS, GHS ≠ NLSS. Check the catalog.
- **Composite ID padding**: `str(1) + str(5)` = `"15"` but `str(1) + str(05)` = `"15"` too — zero-padding matters for matching.
- **Extension/split-off handling**: Split-off households need distinct IDs (e.g., append `_1`), not the same ID as the parent.
- **Cross-sectional waves in panel countries**: Some waves are cross-sectional expansions alongside the panel subsample. Only the panel subsample has linkage.
- **data_scheme.yml gate**: `panel_ids` must be listed in `data_scheme.yml` for the library to load it. Without it, `Country.panel_ids` returns `None` silently.
