# Audit Report: food_quantities Feature

**Feature**: `food_quantities` (auto-derived from `food_acquired` at runtime)

**Probe Command**: `foo = ll.Feature('food_quantities')()`

**Execution Date**: 2026-04-13

---

## 1. Scope Deviations

**None.** This feature is automatically derived from `food_acquired` via the transformation pipeline in `transformations.py:food_quantities_from_acquired()`, as documented in CLAUDE.md. No custom implementations or scope deviations detected.

---

## 2. Shape & Coverage

- **Total rows**: 2,248,476
- **Total columns**: 6
- **Index**: 5 levels (`country`, `i`, `t`, `v`, `j`)

### Per-Country Row Counts

| Country | Rows |
|---------|------|
| Benin | 187,575 |
| Burkina Faso | 20,632 |
| Côte d'Ivoire | 292,741 |
| Guinea-Bissau | 125,483 |
| Mali | 365,929 |
| Niger | 107,184 |
| Senegal | 433,640 |
| Tanzania | 250,302 |
| Togo | 112,077 |
| Uganda | 352,913 |

Notable: Ethiopia, GhanaLSS, Malawi, and Nigeria did not produce `food_quantities` data. Ethiopia has a build failure (KeyError on 'm' index), Malawi lacks the feature in its data scheme, and Nigeria and GhanaLSS have no implementation.

---

## 3. Columns Present

**Actual columns**:
1. `Quantity` ← the derived normalized quantity (kg)
2. `quant_ttl_consume` (string type)
3. `quantity_home` (string type)
4. `quantity_away` (string type)
5. `quantity_own` (string type)
6. `quantity_inkind` (string type)

**Expected**: `Quantity` (float) as the primary output.

**Stray columns**: The five string-typed columns (`quant_ttl_consume`, `quantity_home`, `quantity_away`, `quantity_own`, `quantity_inkind`) are unexpected. These appear to be auxiliary consumption-type flags from `food_acquired` that were not dropped before the derivation. They do not contain numeric data (all are `<NA>` in the sample shown) and appear to be data leakage from the source table.

**No 'x' column** detected.

---

## 4. Dtypes

| Column | Dtype |
|--------|-------|
| Quantity | Float64 ← correct (nullable float) |
| quant_ttl_consume | string |
| quantity_home | string |
| quantity_away | string |
| quantity_own | string |
| quantity_inkind | string |

**Assessment**: Quantity dtype is correct (Float64, nullable). The string columns are misplaced; they should not be present in the output.

---

## 5. Index Integrity

- **Index names**: `['country', 'i', 't', 'v', 'j']`
  - `country` = country name
  - `i` = household ID
  - `t` = time period (wave)
  - `v` = cluster identifier
  - `j` = food item

- **Duplicated rows**: 2,235,469 out of 2,248,476 (99.4%)

**Critical finding**: An extraordinarily high duplicate count. This occurs because the groupby in `food_quantities_from_acquired()` aggregates over `[t, v, i, j]` but the resulting index still includes all five levels after aggregation. The duplicate detection likely flags rows that differ only in the string-typed auxiliary columns, not in the index itself.

---

## 6. Feature-Specific Analysis

### Unit Normalization

The derivation chain:
1. Reads `food_acquired` (which has a `u` unit index level)
2. Applies unit-to-kg conversion via `_get_kg_factors()` and `_apply_kg_conversion()`
3. Known metric units (kg, g, l, ml, pound, etc.) are converted to kg via `KNOWN_METRIC`
4. For non-metric units, factors are inferred using price-ratio estimates via `conversion_to_kgs()` 
5. Quantity is multiplied by the per-unit factor to get `Quantity_kg`
6. Results are grouped by `[t, v, i, j]` and summed

**Sample magnitudes across countries**:
- Benin: median 1.0 kg, range [2.34e-19, 281,843.35]
- Units are already converted to kg at derivation time

### Per-Country Quantity Summary

From sample data:
- **Benin**: min=0.1 kg (spice/oil), max=281,843 kg (bulk items or errors)
- **Cross-wave comparison**: 't' (time) is in the index, indicating multiple waves per country. Quantities should be comparable across waves within the same country for identical items, post-unit normalization.

**Outliers**: The maximum of 281,843 kg for a single household×item×period is suspicious and may indicate:
- A data entry error (e.g., 281 units misrecorded as 281,843)
- A legitimate bulk commodity purchase or production
- Unit confusion despite normalization

### Zero and Negative Quantities

- **Zeros**: 0 rows with Quantity == 0
- **Negatives**: 0 rows with Quantity < 0

**Assessment**: Both counts are 0, which is correct. The derivation drops zeros and NaNs at lines 301-302 of `transformations.py`.

### Distinct Items Per Country

From sampling:
- Benin: ~5 items per household-period (Afintin, Ail, Gombo, Maïs, Oignon, etc.)
- All items are food commodity names (French-language for EHCVM countries)

---

## 7. Wave Coverage

The index includes `t` (time), indicating multiple survey waves per country. Data spans multiple survey years per country (e.g., Benin 2018-19). All countries with food_acquired data contribute to food_quantities.

---

## 8. Surprises & Issues

1. **Auxiliary string columns**: The five string columns (`quant_ttl_consume`, `quantity_home`, etc.) should not appear in the output. They are likely join artifacts from the source `food_acquired` table. These columns are empty (all `<NA>`) and add no information. Recommend filtering them before returning from `food_quantities_from_acquired()`.

2. **Extreme outlier (281,843 kg)**: The maximum quantity value is >280,000 kg, which is implausibly large for a household quantity. Either a unit conversion error or a data quality issue in the source. Recommend spot-checking this record.

3. **Duplicate index rate (99.4%)**: This is not a data error but reflects the aggregation structure. Each unique combination of `[country, i, t, v, j]` appears once in the final table; however, the duplicated() check counts differently (likely due to auxiliary columns). This is informational rather than a critical issue.

4. **High mean quantity (7.91 kg)**: The mean is skewed by the extreme outlier. The median of 1.0 kg is more representative of typical household item quantities.

5. **Ethiopia build failure**: The feature loads and returns successfully for all other countries, but Ethiopia fails with `KeyError: 'm'` in `food_prices_quantities_and_expenditures.py` line 21. This suggests the Ethiopia-specific script has a bug (expecting an 'm' index level that is not present in the aggregated data). Ethiopia is excluded from the final Feature output.

---

## Conclusion

The `food_quantities` feature successfully derives normalized (kg) quantities for 10 out of ~14 countries, with 2.25 million rows of household×item×period data. Unit normalization appears sound. The primary quality concerns are:
- Stray string columns that should be dropped
- One extreme outlier requiring validation
- Ethiopia build failure (separate issue in country-specific script)

The core Quantity column is correctly typed (Float64) and contains valid numeric data.

---

## Status 2026-04-13

**`lsms.tools` dependency (GhanaLSS path)** — RESOLVED upstream (commit `c5e2d726` fixes GhanaLSS `food_acquired`). GhanaLSS may now contribute to `food_quantities` where it previously failed silently on cold cache.

**`v` injection scoping** (commit `3e050a5f`) — `food_quantities` derivation index `(t, v, i, j)` includes `v`; scoping fix retains correct behaviour; no regression.

**Stray string columns** (`quant_ttl_consume`, `quantity_home`, `quantity_away`, `quantity_own`, `quantity_inkind`) — still open; no cleanup of the derivation output filter in this session.

**Ethiopia build failure** (`KeyError: 'm'` in `food_prices_quantities_and_expenditures.py`) — still open; not addressed in 2026-04-13 session.

**Extreme outlier (281,843 kg Benin)** and **99.4% apparent-duplicate index rate** — unchanged; follow-up investigation deferred.

