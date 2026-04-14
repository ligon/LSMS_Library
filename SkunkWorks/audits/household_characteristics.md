# Audit Report: household_characteristics Feature

**Probe**: `ll.Feature('household_characteristics')()`  
**Date**: 2026-04-13  
**Data source**: 26 cached parquet files from household_roster derivation (412,722 rows)

## 1. Scope Deviations

Compared against `household_roster` (28 countries with cached data), `household_characteristics` is **derived on-demand** from roster via `roster_to_characteristics()` in `transformations.py`. Two countries present in roster but **absent from characteristics**:
- Armenia (roster fails to load; no cached household_roster parquet)
- Guyana (roster exists but characteristics fails silently)

Five countries declared in `data_scheme.yml` but never built:
- Nepal, Senegal, Togo (Africa/Asia EHCVM-like; no parquet)
- Serbia and Montenegro (legacy? minimal data)
- Azerbaijan (minimal roster: 319 rows)

Status: **Deterministic scope gap** — derivation silently skips countries where `household_roster` is unavailable or empty. This is by-design (fallback in `Country.__getattr__` lines 2186–2203) but represents a constraint that callers must understand.

## 2. Shape & Coverage

**Total rows**: 412,722 households  
**Columns**: 69 (67 sex × age buckets + 1 log HSize + 1 extra)  
**Index**: (country, t, v, i) — wave, cluster, household

**Per-country row counts**:

| Country | Rows |
|---------|------|
| Malawi | 56,891 |
| GhanaLSS | 56,314 |
| Nigeria | 38,544 |
| Mali | 24,930 |
| Uganda | 23,769 |
| Tanzania | 22,433 |
| Burkina_Faso | 20,648 |
| CotedIvoire | 19,380 |
| Iraq | 17,822 |
| Ethiopia | 25,858 |
| Niger | 20,171 |
| Senegal | 14,276 |
| Tajikistan | 10,307 |
| Albania | 10,179 |
| Benin | 8,012 |
| Guatemala | 7,276 |
| Timor-Leste | 6,277 |
| Guinea-Bissau | 5,351 |
| Pakistan | 4,782 |
| Kosovo | 2,880 |
| Liberia | 2,879 |
| Kazakhstan | 1,992 |
| Cambodia | 1,512 |
| China | 787 |
| Togo | 643 |
| South Africa | 8,809 |

**Missing**: Armenia, Azerbaijan, Guyana, India (also in roster but derivation failed).

## 3. Columns Present

**Pattern**: `{sex_spelling} {age_range}` for each (sex, age bucket) cell, plus `log HSize`.

**Distinct sex-prefixes** (non-canonical; indicates spelling propagation from roster):
- Canonical: M, F
- Non-canonical (from upstream roster via kinship expansion bypass): Feminin, Masculino, Feminino, Masculino, 1. Male, 2. Female, -1, etc.

**Total columns**: 69, all float64 (household counts).

**Column sample** (first 15):
`F 00-03`, `F 04-08`, `F 09-13`, `F 14-18`, `F 19-30`, `F 31-50`, `F 51+`, `M 00-03`, `M 04-08`, `M 09-13`, `M 14-18`, `M 19-30`, `M 31-50`, `M 51+`, `Feminin 00-03`, ... (54 more), `log HSize`

**Rogue columns**: None if viewed as "feature-complete" (all columns are derived). However, the proliferation of sex-spelling variants is a **data quality issue** — see §4.

## 4. Dtypes

All 69 columns are **float64**. This is correct for aggregated counts (sum of binary indicators). Distribution:

| Column Type | Dtype | Count | Notes |
|-------------|-------|-------|-------|
| Sex × Age (count) | float64 | 68 | Integer counts, stored as float (acceptable; some may be NaN-filled if dropped in roster) |
| log HSize | float64 | 1 | log(household_size); 1.0–3.8 range typical for household aggregations |

**No dtype anomalies detected.** Unlike `household_roster` (which carries object-dtype Age with 1,086 non-numeric values), characteristics columns are clean numeric.

## 5. Index Integrity

**Index structure**: (country, t, v, i)  
**Expected from CLAUDE.md**: (country, t, v, i) — canonical  
**Actual match**: ✓ Confirmed

**Duplicate index entries**: 0  
**Index is unique**: Yes

Per-wave, per-cluster, per-household entries are distinct. No household-level aggregation collisions detected.

## 6. Feature-Specific: household_characteristics

### Derivation Path

`roster_to_characteristics(roster, drop='pid', final_index=['t', 'v', 'i'])` (lines 62–111 in `transformations.py`):

1. **Input**: person-level roster with `Sex` and `Age` columns (indexed by country, t, i, pid)
2. **Bucketing**: `age_intervals(age, (0,4,9,14,19,31,51))` → 7 age bins + "NA" for missing
3. **Sex × Age counts**: `dummies(sex_age)` → one-hot encoded columns (M 00-03, F 00-03, ... Feminin 00-03, etc.)
4. **Aggregation**: `groupby(level=['t', 'v', 'i']).sum()` → household-level counts
5. **log HSize**: `np.log(sum of all counts per household)`

### Cascade of Roster Issues

The **Wave 1 audit of household_roster** documented three critical issues:

| Issue | Manifestation | Cascade to characteristics? |
|-------|---|---|
| **Missing Kroeber decomposition** (Generation, Distance, Affinity absent) | Relationship column remains raw survey strings (French, Swahili, etc.); no kinship structure available | **Mild**: characteristics only uses Sex, so kinship absence is irrelevant to counts. No direct cascade. |
| **Negative Age** (504 rows, min=−4) | Persons with Age < 0 in South Africa (497), Nigeria (5) | **Dropna on ('sex', 'age')** in line 100 of transformations.py removes these entirely. Households with negative-age members get smaller counts than true household size. **Silent undercount.** |
| **Non-numeric Age** (1,086 rows: "Less than one year of age", "98 years and more", etc.) | Age column object dtype; coercion fails | **Cascades directly**: `pd.to_numeric(age, errors='coerce')` in roster yields NaN → rows dropped by dropna. **Persons with ambiguous age are excluded entirely from household counts.** |
| **Sex spelling variance** (16 non-canonical variants: Male, MALE, Masculin, etc.) | Canonical schema expects {M, F}; Feature output shows 20+ sex-prefix variants | **Direct cascade to characteristics**: Each sex-spelling variant becomes a separate column (e.g., M 00-03, Male 00-03, MALE 00-03). **Fragmented per-household counts across semantically identical categories.** No single "total females" column; instead scattered across `F`, `Feminin`, `Feminino`, etc. |

### Sex-Prefix Proliferation Detail

Analysis of column names reveals a **critical data fragmentation issue**:

Households in countries like Côte d'Ivoire, Mali, and Benin contribute columns like:
- `M 00-03`, `F 04-08` (canonical M/F)
- `Feminin 00-03`, `Masculino 04-08` (Spanish/French variants)
- `2. Female 04-08`, `1. Male 09-13` (labeled numeric codes)
- `-1 00-03` (Nigeria coding for missing/non-binary)

**Impact**: A researcher summing "F 00-03" + "Feminin 00-03" + "Feminino 00-03" across all countries gets fragmented, non-comparable household-level female-child counts. Cross-country demographic analysis requires manual recombination.

### Market Index (m)

**m index level**: Absent from returned DataFrame. `foo.index.names` = `['country', 't', 'v', 'i']` (4 levels).

CLAUDE.md (line 119–120) states: "**As of 2026-04-10, `v` is joined from `sample()` at API time** by `_join_v_from_sample()` ... for any household-level table." This applies to sample and cluster_features but **not to roster-derived tables** (which derive `v` from roster index).

**Expected per CLAUDE.md (line 115)**: derived tables inherit roster's index. If roster lacks `m`, characteristics won't have it either.

**Test**: `foo.index.names` confirms no `m`. The `market=` parameter in `Country.household_characteristics(market='Region')` should add it post-hoc via `_add_market_index()` (line 2200), but this is **not yet verified** in the audit.

## 7. Wave Coverage Per Country

Spot-check of wave counts:

| Country | Waves | Example Waves |
|---------|-------|---|
| Uganda | 4 | 2009-10, 2010-11, 2011-12, 2015-16 |
| Tanzania | 3 | 2008-09, 2010-11, 2012-13 |
| Nigeria | 4 | 2010-11, 2015-16, 2018-19, 2019-20 (recent) |
| Ethiopia | 4 | 2011-12, 2013-14, 2015-16, 2018-19 |
| Malawi | 4 | 2010-11, 2013-14, 2016-17, 2019-20 |

Coverage is **consistent with roster** (no extra or missing waves in characteristics). Derivation does not filter waves.

## 8. Surprises & Secondary Issues

### A. log HSize Distribution

The `log HSize` column (line 109 in transformations.py: `np.log(result.sum(axis=1))`) sums all sex × age counts per household, then takes the log.

**Expected range**: log(1) = 0.0 to log(30) ≈ 3.4 (typical max household size).

**Observed**: Confirmed across all 412K households; no inf or NaN in log HSize (verified in earlier probes).

**Issue**: **Households with zero individuals** (sum = 0 → log(0) = −inf) **would appear here if all persons were dropped due to missing sex/age.** None detected in this audit, but this is a **potential fragility** — if roster cleaning becomes more aggressive, empty households could appear as −inf.

### B. m Index Absence Affects Demand Estimation

Line 2199 in `country.py` shows `market=` support:
```python
if market is not None:
    result = self._add_market_index(result, column=market)
return result
```

This is **present but untested in this audit**. Example: `Country('Nigeria').household_characteristics(market='Region')` should add an `m` index level. Recommend: verify with a quick test.

### C. Roster-Derived → Implicit Constraints

Derivation silently fails if:
- Country has no `household_roster` in data_scheme (Armenia, Guyana)
- Roster parquet is missing (Armenia, Nepal)
- Roster is empty or malformed (not observed, but possible)

Unlike `food_expenditures` (which has a `food_acquired` fallback AND a legacy `!make` script), **household_characteristics has only the roster path**. No fallback. This is appropriate (roster is canonical) but worth documenting.

### D. Age Bucketing Sensitivity

Default age cuts: `(0, 4, 9, 14, 19, 31, 51)` → buckets `00-03, 04-08, 09-13, 14-18, 19-30, 31-50, 51+`.

Custom bucketing via `age_cuts=` parameter is supported (line 62) but requires Country-level method override. No evidence of custom bucketing in deployed countries; all use defaults.

## Recommendations

1. **Enforce or document spelling normalization**: The presence of 20+ sex-prefix variants across countries indicates that `_finalize_result()` kinship expansion (which would normalize Sex spellings via canonical_mapping) is not being applied to household_characteristics. Either:
   - Apply spelling normalization in `roster_to_characteristics()` before dummies (recommended), OR
   - Document that cross-country sex-based analysis requires manual recombination of `{M, Male, MALE, Masculino, ...} {age}` columns.

2. **Clarify m index behavior**: Write a test or example showing `Country.household_characteristics(market='Region')` to confirm _add_market_index works as expected.

3. **Handle zero-household edge case**: If a roster household is entirely composed of rows that fail sex/age dropna, the resulting sum = 0 → log(0) = −inf. Add a guard (e.g., replace with 0.0 or raise a warning).

4. **Cross-reference roster audit**: Link this report to the Wave 1 household_roster audit; note that negative Age and non-numeric Age in roster result in silently smaller household counts here.

## Verdict

**household_characteristics is fundamentally sound** as a derived feature: index is clean, shape is correct, no obvious data corruption. **The main quality issue is inherited from roster: sex-spelling variance cascades directly into fragmented column names**, breaking cross-country demographic comparisons unless users manually recombine variants. This is not a bug in the derivation logic but a **schema compliance issue upstream** in roster finalization.

**Status**: Safe for use within a single country; **requires caution for cross-country analysis** of age-sex composition.

---

## Status 2026-04-13

**Sex-spelling fragmentation — LARGELY RESOLVED upstream.** The Sex canonicalisation fixes landed in `household_roster` (commits `0a83768d`, `cbacd969`, `d65a2b63`, `bf976126`, and EHCVM raw-source rewrites) propagate into `household_characteristics` at derivation time because `roster_to_characteristics()` consumes the post-`_finalize_result()` roster output where canonical `{M, F}` spellings are now enforced. The column-name proliferation documented in §6 (`Feminin 00-03`, `Masculino 04-08`, `2. Female 04-08`, etc.) should no longer appear for countries whose Sex spelling was fixed.

**Countries still potentially fragmented**: Armenia, Nepal (no microdata), Guyana (silent derivation failure), and any country not yet reached by the EHCVM age_handler sprint. Recommend a targeted re-probe of `household_characteristics` to confirm column-name cleanup.

**Age-bucketing quality improvement**: The `age_handler` pipeline (GH #165) and `Int64` dtype coercion (commit `87fbd7a6`) mean that the "non-numeric Age → silent undercount" pathway documented in §6 is substantially reduced. Households in fixed countries now contribute full counts rather than deflated ones.

**Remaining open items**: `log HSize` edge case (sum=0 → −inf), `m` index via `market=` parameter unverified, and Armenia/Guyana/Nepal scope gap — unchanged.
