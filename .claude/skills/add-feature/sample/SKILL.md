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
    panel_weight: float
    strata: str
    Rural: str
```

- **Index:** household (`i`) x wave (`t`) --- one row per household per wave
- **`v`:** cluster/PSU identifier (EA code, parish code, or parish name depending on wave)
- **`weight`:** cross-sectional household sampling weight (positive for all interviewed households including refreshment sample)
- **`panel_weight`:** longitudinal/panel weight (positive only for continuing panel households; zero or NaN for refreshment sample and non-response)
- **`strata`:** stratification domain (sub-region label, harmonized via `categorical_mapping.org`)
- **`Rural`:** urban/rural classification

## Two weight types

Many LSMS panel surveys provide two distinct weights:

1. **Cross-sectional weight** (`weight`) --- positive for every interviewed household in that wave, including both continuing panel households and newly drawn refreshment households. Use for analysis within a single wave.

2. **Panel/longitudinal weight** (`panel_weight`) --- positive only for households that have been tracked from a prior wave. Zero or NaN for refreshment sample households and non-response. Use for analysis across waves (panel regressions, etc.).

For waves before a rotating panel was introduced (e.g., Uganda 2005--2012), or where only one weight is provided, the same variable goes in both columns. For waves with a refreshment sample (e.g., Uganda 2013-14 onward), the two weights diverge: `weight` covers everyone, `panel_weight` excludes new entrants.

**How to tell them apart:** Look for a `rotate` variable (0 = refreshment, 1 = continuing panel). Cross-tab `rotate` against NaN/zero patterns in each weight variable. The cross-sectional weight will have near-complete coverage; the panel weight will have NaN or zeros concentrated among `rotate=0` households.

The BID (Basic Information Document) for each wave describes the weight construction. These are available from the World Bank Microdata Library under "Related Materials" for each catalog entry.

## Finding the source variables

All variables typically live in the **cover page / Section 1 / household identification** file --- the same file used for `cluster_features`. Check the existing `cluster_features` block in each wave's `data_info.yml` to find the file path.

### Weight variable names by country

Weight variable names change across waves and countries. There is no standard.

| Country | Cross-section weight | Panel weight | File | Notes |
|---------|---------------------|-------------|------|-------|
| **Uganda** | `hmult`/`wgt09`/`wgt10`/`mult`/`wgt_X`/`h_xwgt_W5`/`wgt` | `hmult`/`wgt09`/`wgt10`/`mult`/`wgt`/`hwgt_W5`/`hwgt_W7`/`wgt` | GSEC1 | Both weights change name every wave |
| **Nigeria** | `wt_wave{N}` | `wt_longpanel` (wave 5) | `secta_plantingw{N}` | Wave 5 adds explicit panel/cross distinction |
| **Tanzania** | `weight`/`y5_crossweight` | `sdd_weights`/`y5_panelweight` | `hh_sec_a` | 2020-21 has both; earlier waves may have one |
| **Ethiopia** | `pw_w{N}` | (check BID) | `sect_cover_hh_w{N}` | |
| **Malawi** | `hh_wgt` | (check BID) | `hh_mod_a_filt` | |
| **EHCVM** | `hhweight` | (check BID) | `ehcvm_ponderations_{cc}{yr}.dta` | **Separate file** from cover page |

**Always inspect the actual data** to confirm variable names --- they are not documented consistently.

**Read the BID first.** Each wave has a Basic Information Document (BID) available from the World Bank Microdata Library under "Related Materials" for the catalog entry. The BID describes the sampling design, weight construction methodology, and often names the variables. Start here before inspecting data. For example, the Tanzania 2020-21 BID (Appendix A) documents the panel vs cross-section weight calculations in detail.

### Booster/refreshment sample trap

When a wave includes a booster or refreshment sample, **legacy ID variables may be NaN for the new households.** For example, Tanzania 2020-21 has both `clusterid` (numeric, legacy) and `y5_cluster` (string, current wave). The 545 booster urban households have `clusterid = NaN` and `strataid = NaN` because those are panel-frame IDs. The wave-specific `y5_cluster` covers everyone. Always verify coverage of each candidate variable by cross-tabbing against the booster/refresh indicator (e.g., `tracking_class`, `rotate`).

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

When both weights, strata, cluster, and urban/rural are all in the cover page:

```yaml
sample:
    file: ../Data/GSEC1.dta
    idxvars:
        i: HHID
    myvars:
        v: comm
        weight: wgt_X       # cross-sectional
        panel_weight: wgt   # longitudinal
        strata: stratum
        Rural: urban
```

When only one weight exists (pre-refreshment waves, or baseline):

```yaml
sample:
    file: GSEC1.dta
    idxvars:
        i: HHID
    myvars:
        v: comm
        weight: hmult        # same variable for both
        panel_weight: hmult
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

### Multi-round files (Tanzania 2008-15 pattern)

When a single `.dta` file contains multiple survey rounds with a `round` column, the YAML path cannot handle it --- use a Python script. The script reads the file, maps round numbers to wave labels, and splits panel vs refresh households for `panel_weight`:

```python
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}
df = get_dataframe('../Data/upd4_hh_a.dta')

sample = pd.DataFrame({
    'i': df['r_hhid'], 'round': df['round'],
    'v': df['clusterid'], 'weight': df['weight'],
    'strata': df['strataid'], 'Rural': df['urb_rur'],
})
sample['panel_weight'] = sample['weight']

# Round 4 refresh households get NaN panel_weight
if 'ha_07_1' in df.columns:
    is_refresh = (df['round'] == 4) & (df['ha_07_1'].astype(str).str.upper() == 'NO')
    sample.loc[is_refresh.values, 'panel_weight'] = pd.NA

sample['t'] = sample['round'].map(round_match)
sample = sample.drop(columns=['round']).set_index(['i', 't'])
to_parquet(sample, 'sample.parquet')
```

### `materialize: make` in data_scheme.yml

**Do NOT set `materialize: make`** on the `sample` entry in `data_scheme.yml` unless ALL waves require scripts. If some waves use YAML and one uses a script, leave the data_scheme entry without `materialize: make` --- the framework will use YAML for waves that have `data_info.yml` entries and fall back to Make for waves that have `.py` scripts writing parquets.

### Missing variables

Not all waves have all columns. Simply omit a `myvars` line and the column will be NaN in the aggregated output. Common cases:

- A wave provides only one weight: put it in both `weight` and `panel_weight`.
- A **panel-only wave** (no booster or refresh, e.g., Tanzania 2019-20 phone survey): the sole weight variable goes in both columns.
- A wave provides no weight at all: omit both lines (rare --- check the BID before assuming a weight doesn't exist; Uganda 2005-06 turned out to have `hmult` despite initial appearances).
- A wave has no explicit strata variable: omit `strata` or construct it from region x urban/rural if that's what the sampling documentation specifies.
- **Numeric strata IDs** (e.g., Tanzania's `strataid`): add a `strata()` formatting function in the wave's `.py` file to strip `.0` via `format_id`, or let the framework's `_normalize_v` handle it.

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
assert set(s.columns) >= {'v', 'weight', 'panel_weight'}
assert sorted(s.index.get_level_values('t').unique()) == uga.waves

# Coverage per wave
for wave in uga.waves:
    ws = s.xs(wave, level='t')
    w_na = ws['weight'].isna().sum()
    pw_na = ws['panel_weight'].isna().sum()
    print(f'{wave}: n={len(ws)}, weight_NaN={w_na}, panel_weight_NaN={pw_na}')

# v should be nearly complete
print(s['v'].isna().sum(), '/', len(s))       # Should be ~0

# Strata labels clean (no trailing spaces, consistent case)
for wave in uga.waves:
    vals = s.xs(wave, level='t')['strata'].dropna().unique()
    print(f'{wave}: {sorted(str(v) for v in vals)}')
```

There is also a dedicated test file `tests/test_sample.py` that auto-discovers countries with `sample` in their `data_scheme.yml` and runs 8 structural checks (index, columns, coverage, duplicates, weight non-negativity).

## Documenting what you learn

Add a **Sampling Design** section to the country's `CONTENTS.org` documenting:
- The stratification scheme and how it changed across waves
- The cluster/PSU identifier and how it changed
- Which weight variable is cross-sectional vs panel, per wave
- Coverage counts (non-null / total) for each weight
- The rotating panel / refreshment sample design if applicable
- References to BIDs with URLs

See `lsms_library/countries/Uganda/_/CONTENTS.org` for the reference.

## Multi-country dispatch

When adding `sample` to many countries at once, **dispatch one agent per country**, never batch multiple countries into a single agent. Each country requires data inspection, BID reading, and testing --- these are independent tasks that should run in parallel on separate cores.

```python
# Good: one agent per country, all launched in one message
for country in countries_needing_sample:
    Agent(f"Add sample to {country}", ...)

# Bad: one agent doing 11 countries sequentially
Agent("Add sample to Armenia, Azerbaijan, ..., Timor-Leste", ...)
```

To avoid re-doing work, check which countries already have `sample` before dispatching:

```python
import lsms_library as ll
from lsms_library.yaml_utils import load_yaml

for yml in ll.paths.COUNTRIES_ROOT.glob('*/_/data_scheme.yml'):
    ds = load_yaml(yml).get('Data Scheme', {})
    if 'sample' not in ds:
        print(f'{yml.parent.parent.name}: needs sample')
```

Some countries (CLAUDE.md notes Armenia, some Timor-Leste waves) have configs but no microdata on disk. Agents should detect this and skip gracefully rather than failing.

## Reference implementations

- **Uganda**: 8-wave panel with rotating refreshment sample, dual weights. `lsms_library/countries/Uganda/`
- **Tanzania**: Multi-round file (2008-15 script), booster sample (2020-21), panel-only phone survey (2019-20). `lsms_library/countries/Tanzania/`
- **GhanaLSS**: 7-wave repeated cross-section spanning 30 years, evolving weight types (self-weighting to expansion). `lsms_library/countries/GhanaLSS/`
- **Niger**: EHCVM multi-file pattern with varying weight sources per wave. `lsms_library/countries/Niger/`
- **Burkina Faso**: EHCVM with genuine dual weights in 2021-22 panel ponderation. `lsms_library/countries/Burkina_Faso/`
