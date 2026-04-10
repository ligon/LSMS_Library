---
name: sample
description: Use this skill to add the sample (sampling design) feature to an LSMS-ISA country. This skill should be used when a user wants to add household-level cluster assignment, sampling weights, strata, and urban/rural classification. Requires the add-feature skill for general workflow.
---

# Add Sample Feature to LSMS Country

This skill provides domain-specific guidance for adding the `sample` table to an LSMS-ISA country. Use in conjunction with the general `add-feature` skill.

## Purpose

The `sample` table encodes the survey's sampling design at the household level: which cluster (PSU) each household was drawn from, the household's sampling weight, the stratum it belongs to, and its urban/rural classification. Other features use `sample` to join cluster identity (`v`) onto household-level data, replacing the fragile pattern of joining `v` from `household_roster`.

## Target schema

```yaml
sample:
    index: (i, t)
    v: str
    weight: float
    strata: str
    Rural: str
```

- **Index:** household (`i`) x wave (`t`) --- one row per household per wave
- **`v`:** cluster/PSU identifier (EA code, parish code, or parish name depending on wave)
- **`weight`:** household sampling weight (cross-sectional weight preferred; NaN if unavailable)
- **`strata`:** stratification domain (sub-region label, harmonized via `categorical_mapping.org`)
- **`Rural`:** urban/rural classification

## Finding the source variables

All variables typically live in the **cover page / Section 1 / household identification** file --- the same file used for `cluster_features`. Check the existing `cluster_features` block in each wave's `data_info.yml` to find the file path.

### Weight variable names by country

Weight variable names change across waves and countries. There is no standard.

| Country | Weight variable pattern | File | Notes |
|---------|----------------------|------|-------|
| **Uganda** | `wgt09`, `wgt10`, `mult`, `wgt`, `hwgt_W5` | GSEC1 | Changes every wave; 2005-06 has none |
| **Nigeria** | `wt_wave{N}` | `secta_plantingw{N}` | Wave 5 adds `wt_longpanel`, `wt_cross` |
| **Tanzania** | `weight`, `sdd_weights`, `y5_crossweight` | `hh_sec_a` / `upd4_hh_a` | 2020-21 has panel + cross-section weights |
| **Ethiopia** | `pw_w{N}` | `sect_cover_hh_w{N}` | |
| **Malawi** | `hh_wgt` | `hh_mod_a_filt` | |
| **EHCVM** | `hhweight` | `ehcvm_ponderations_{cc}{yr}.dta` | **Separate file** from cover page |

**Always inspect the actual data** to confirm variable names --- they are not documented consistently.

### Strata variable names

Strata are often implicit (Region x Urban/Rural) rather than explicit.

| Country | Strata variable | Values | Notes |
|---------|----------------|--------|-------|
| **Uganda** | `stratum` / `sregion` / `subreg` | 6--15 sub-regions | Name changes across waves |
| **Nigeria** | `strata` (wave 4+) | 6 zones | Earlier waves: construct from `zone` x `sector` |
| **Tanzania** | `strataid` | 53 region x urban/rural pairs | Explicit in all waves |
| **Ethiopia** | *none explicit* | Construct from `saq01` x `saq14` | Region x Urban/Rural |
| **Malawi** | *none explicit* | Use `district` (32 strata) | |
| **EHCVM** | *none explicit* | Construct from `s00q01` x `s00q04` | Region x Milieu |

### Cluster (PSU) variable names

The cluster identifier is what becomes `v` in the library's index convention.

| Country | Cluster variable pattern | Notes |
|---------|------------------------|-------|
| **Uganda** | `comm`, `h1aq4a`, `parish_code`, `parish_name`, `s1aq04a` | Switches from numeric codes to parish names circa 2015-16 |
| **Nigeria** | `ea` | Consistent across waves |
| **Tanzania** | `clusterid` | Numeric; `sdd_cluster`/`y5_cluster` are string alternatives |
| **Ethiopia** | `ea_id` | |
| **Malawi** | `ea_id` | |
| **EHCVM** | `grappe` | Consistent across all EHCVM countries |

## YAML configuration pattern

### Single-file pattern (most countries)

When weight, strata, cluster, and urban/rural are all in the cover page:

```yaml
sample:
    file: ../Data/GSEC1.dta
    idxvars:
        i: HHID
    myvars:
        v: comm
        weight: wgt09
        strata: stratum
        Rural: urban
```

### Multi-file pattern (EHCVM countries)

When weights are in a separate ponderation file:

```yaml
sample:
    dfs:
        - df_cover
        - df_weights
    df_cover:
        file: ../Data/s00_me_ner2018.dta
        idxvars:
            i: [grappe, menage]
        myvars:
            v: grappe
            strata: s00q01
            Rural: s00q04
    df_weights:
        file: ../Data/ehcvm_ponderations_ner2018.dta
        idxvars:
            i: [grappe, menage]
        myvars:
            weight: hhweight
    merge_on:
        - i
    final_index:
        - i
        - t
```

### Missing variables

Not all waves have all columns. For example, Uganda 2005-06 has no weight variable. Simply omit the `weight:` line from that wave's config --- the column will be NaN in the aggregated output.

## Label harmonization

Strata labels typically vary across waves (case, spelling, whitespace). Add a `#+name: strata` table to the country's `categorical_mapping.org`:

```org
#+name: strata
| Alternate Spelling | Preferred Label |
|--------------------+-----------------|
| East rural         | East Rural      |
| South-westrn       | South Western   |
| KAMPALA            | Kampala         |
| kampala            | Kampala         |
```

The library auto-applies this mapping when the column name (`strata`) matches a table name in `categorical_mapping.org`. The framework strips leading/trailing whitespace from string columns before matching, so space-padded labels from Stata files are handled automatically.

**Note:** Different waves may use genuinely different stratification schemes (e.g., Uganda 2009-10 has 6 strata while 2013-14 has 10 sub-regions). Do not force-harmonize across different schemes --- just fix cosmetic issues (case, whitespace, typos) within each scheme.

## How `sample` integrates with the framework

### Replacing roster-based `v` joins

Previously, scripts joined `v` from `household_roster` to add cluster identity to household-level tables. This was conceptually wrong (the roster is about demographics, not sampling) and created a circular dependency. With `sample`, the join should be:

```python
uga = ll.Country('Uganda')
samp = uga.sample()
v_lookup = samp[['v']].droplevel([], errors='ignore')  # indexed by (i, t)
df = df.join(v_lookup)
```

### Relationship to `cluster_features`

- `sample` is **household-level**: indexed by `(i, t)`, maps each household to its cluster
- `cluster_features` is **cluster-level**: indexed by `(t, v)`, carries Region, District, GPS, etc.

To get from household to Region: join `sample` on `(i, t)` to get `v`, then join `cluster_features` on `(t, v)` to get Region. This is exactly what `_add_market_index(market='Region')` does.

### Relationship to `_add_market_index`

The `_add_market_index` method in `country.py` uses `(t, v)` to join market/region identifiers from `cluster_features`. When `v` is already in the DataFrame's index, it joins directly. When `v` is absent, it falls back to joining via `household_roster` --- this fallback should eventually be updated to use `sample` instead.

## Verification

```python
import lsms_library as ll
uga = ll.Country('Uganda')
s = uga.sample()

# Basic checks
assert s.index.names == ['i', 't']
assert set(s.columns) >= {'v', 'weight'}
assert sorted(s.index.get_level_values('t').unique()) == uga.waves

# Coverage
print(s.groupby('t').size())           # ~2700--3300 per wave
print(s['weight'].isna().sum(), '/', len(s))  # NaN only for waves without weights
print(s['v'].isna().sum(), '/', len(s))       # Should be ~0

# Strata labels clean
for wave in uga.waves:
    vals = s.xs(wave, level='t')['strata'].dropna().unique()
    print(f'{wave}: {sorted(str(v) for v in vals)}')
```

## Reference implementation

Uganda is the reference: `lsms_library/countries/Uganda/` on the `feature/sample-table` branch. All 8 waves configured via YAML, no scripts. Weight variable names mapped per wave, strata harmonized via `categorical_mapping.org`.
