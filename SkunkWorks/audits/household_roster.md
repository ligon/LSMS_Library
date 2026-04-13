# Audit Report: household_roster Feature

**Probe**: `ll.Feature('household_roster')()`  
**Date**: 2026-04-12  
**Data source**: 28 cached parquet files (2.24M rows)

## 1. Scope Deviations

Five countries declared `household_roster` in `data_scheme.yml` but produced no cached parquet (likely not yet built):
- Armenia
- Nepal
- Senegal
- Serbia and Montenegro
- Togo

These five are silently absent from the aggregated Feature output. Detection status: **unambiguous** (parquet cache layer masks missing builds).

## 2. Shape & Coverage

**Total rows**: 2,236,647  
**Countries loaded**: 28  
**Index structure**: (country, t, i, pid) — wave, household, individual  

Per-country row counts:

| Country | Rows |
|---------|------|
| Malawi | 260,447 |
| GhanaLSS | 246,594 |
| Nigeria | 234,547 |
| Mali | 188,706 |
| Burkina_Faso | 146,517 |
| Uganda | 147,612 |
| Ethiopia | 127,890 |
| Iraq | 127,506 |
| Niger | 125,653 |
| Tanzania | 112,885 |
| CotedIvoire | 111,789 |
| Tajikistan | 79,309 |
| Albania | 42,465 |
| Benin | 42,343 |
| Guinea-Bissau | 42,839 |
| South Africa | 43,687 |
| Pakistan | 36,079 |
| Guatemala | 37,771 |
| Timor-Leste | 34,113 |
| Kosovo | 17,917 |
| Liberia | 12,263 |
| Kazakhstan | 7,221 |
| Cambodia | 6,351 |
| China | 2,998 |
| Azerbaijan | 319 |
| Serbia | 131 |
| Guyana | 150 |
| India | 545 |

## 3. Columns Present vs Canonical

**Present**: Age, Birthplace, Educational Attainment, Marital, Marital_status, Relationship, Sex, in_housing  
**Canonical required**: Affinity, Age, Distance, Generation, Relationship, Sex

**Missing canonical-required columns**:
- `Affinity` — kinship class (consanguineal, affinal, step, foster, unrelated, guest, servant)
- `Distance` — collateral distance (int)
- `Generation` — vertical distance from head (int)

All three are marked `api_derived: true` in `data_info.yml`, indicating they should be computed post-load by `_finalize_result()` kinship expansion. Their absence indicates expansion is **not being applied** across the cached Feature output.

**Rogue columns** (not in canonical schema, 5 found):
- `Birthplace` (str) — country-specific; not harmonized
- `Educational Attainment` (string) — listed as canonical but stored as object
- `Marital` (string) — raw survey column
- `Marital_status` (string) — duplicate/variant
- `in_housing` (str) — country-specific housing flag

## 4. Dtypes

| Column | Dtype | Issues |
|--------|-------|--------|
| Age | object | **Should be int**; contains 1,086 non-numeric values (see §7) |
| Sex | string | **Spelling normalization not applied**; 16 non-canonical variants present (see §6) |
| Relationship | string | OK (original survey label, expected wide variety) |
| Birthplace | str | OK |
| Educational Attainment | string | OK (rogue but consistent) |
| Marital | string | OK |
| Marital_status | string | OK (duplicate) |
| in_housing | str | OK |

## 5. Index Integrity

**Index names**: (country, t, i, pid)  
**Duplicates**: 0 — index is unique, no household-person collisions per wave.

## 6. Feature-Specific: Kroeber Decomposition

All three derived Kroeber columns are **absent**:

| Column | Status |
|--------|--------|
| Sex | Present; **2,228,021/2,236,647 (99.6%) populated** |
| Generation | **MISSING** (api_derived) |
| Distance | **MISSING** (api_derived) |
| Affinity | **MISSING** (api_derived) |

**Root cause**: `_finalize_result()` kinship expansion not invoked. The Feature layer should call kinship decomposition on `Relationship` labels, but downstream output skips this step. Non-standard `Relationship` values (356 unique labels, highly language-specific) cannot be mapped to canonical kinship without decomposition.

**Unknown Relationship labels**: Probe did not capture `UserWarning: Unknown relationship labels` emissions. Relationship values remain raw survey strings (French "Fils/Fille", Swahili "Mtoto", Hausa "Omo", etc.); no canonicalization flagged.

### Sex Spelling Consistency

**Critical issue**: Sex spellings are **not canonical**. Expected {M, F}; found 16 variants across countries:

- Canonical (2): M, F
- Non-canonical (16): Male, male, MALE, Female, female, FEMALE, Masculin, masculino, Masculino, Féminin, Feminin, Feminino, femenino, 1. Male, 2. Female, -1

**By country** (sample of affected):
- Albania: 42,465 (all Male/Female)
- Nigeria: 234,536 (all Male/Female; 18 -1 codes)
- Ethiopia: 126,082 (all Male/Female)
- Niger: 121,788 (all Male/Female)
- CotedIvoire: 61,116 (Male/Female; 240+ "Fils/Fille")

The canonical schema declares `spellings` mappings, but no runtime normalization is applied in Feature output.

## 7. Age Distribution

**Overall**: min=−4, max=999, mean=23.0

**Data quality issues**:
- **Non-numeric values**: 1,086 rows contain text (e.g., "Less than one year of age", ".", "98 years and more")
  - Cambodia: 109
  - CotedIvoire: 240
  - Timor-Leste: 737
- **Negative Age**: 504 rows (min=−4)
  - South Africa: 497 (likely coding for missing)
  - Nigeria: 5, CotedIvoire: 2
- **Age > 120**: 15 rows (max=999)
  - Nigeria: 1 (999 — implausible)
  - Other: sporadic

**Age distribution per country (min, max, mean)**:
- Albania: 0–108, mean 34.1
- Ethiopia: 0–100, mean 23.4
- Nigeria: −1–999, mean 23.9 ← problematic
- South Africa: −4–110, mean 25.3 ← negative min
- Malawi: 0–119, mean 21.8
- Mali: 0–120, mean 22.1

Age dtype mismatch (object vs int) and missing coercion in Feature output prevent downstream numeric analysis.

## 8. Rows per Household

Typical household size (rows per unique (country, i, t)):
- Mean: 5.56
- Median: 5
- Range: 1–99

**Outliers** (>15 individuals per household): 6,532 cases (1.6% of households)

Top 10 largest:
1. Côte d'Ivoire 1985–86, hh71001: **99** individuals
2. Côte d'Ivoire 1985–86, hh71025: **81** individuals
3. Mali 2017–18, hh418001: **79** individuals
4. Mali 2017–18, hh425002: **76** individuals
5. Mali 2017–18, hh215007: **74** individuals

These extreme cases are plausible in multi-generational extended households (common in sub-Saharan and South Asia context); no data corruption evident.

## Surprises & Secondary Issues

1. **Rogue columns**: Five non-canonical columns (Birthplace, Educational Attainment, Marital, Marital_status, in_housing) are present. These are survey-specific and should either be documented as country extensions in data_scheme.yml or removed to enforce strict schema compliance.

2. **Sex normalization failure**: The canonical schema defines `spellings` for Sex (M/F with 8 aliases each), but the Feature output does not apply these mappings. Sex columns remain in raw survey format, breaking downstream interoperability (e.g., crosstabs across countries fail if one uses "Male" and another "MALE").

3. **Marital & Marital_status duplication**: Both columns are present; no guidance on which to use. This suggests incomplete data harmonization during country-level finalization.

4. **Missing household_characteristics derivation**: Although `household_characteristics` is a declared Feature, its dependency on Kroeber-decomposed `household_roster` is broken by the missing Generation/Distance/Affinity columns.

## Recommendations

1. **Enforce Kroeber expansion** in Feature output or document that api_derived columns are computed only at Country level, not in cross-country aggregation.
2. **Apply spelling normalization** to Sex, Relationship, and other enum-typed columns using the canonical `spellings` map before Feature concatenation.
3. **Coerce Age to int** with explicit handling of non-numeric values (mark as NaN or create a separate "Age_quality" flag).
4. **Validate Age range** (0–130) and emit warnings for negative or > 120.
5. **Document or remove rogue columns** (Birthplace, in_housing, duplicate Marital variants).
6. **Investigate scope gap**: Senegal, Togo, Armenia, Nepal, Serbia and Montenegro declared but not built; clarify build status or update data_scheme.yml.
