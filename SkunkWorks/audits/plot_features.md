# Audit Report: plot_features Feature

**Probe**: `ll.Feature('plot_features')()`  
**Date**: 2026-04-12  
**Data source**: 0 cached parquet files (0 rows)

## 1. Scope Deviations

**Status**: CRITICAL — Table declared in canonical schema but unimplemented.

`plot_features` is listed in the global `data_info.yml` Index Info section with canonical index `(t, v, i, plot_id)`, signifying that the LSMS Library architecture expects this table to be assembled cross-country. However, **zero countries declare it in their `data_scheme.yml` files**, and zero rows are produced by Feature aggregation.

All 13 LSMS-ISA countries (Ethiopia, Malawi, Nigeria, Tanzania, Uganda, Mali, Niger, Senegal, Burkina_Faso, Benin, Togo, Guinea-Bissau, and the designated GhanaSPS) are **completely absent** from the plot_features declaration universe. GhanaSPS does not even have a `data_scheme.yml` file in the repository. This is a **full-coverage gap**, not a sparse gap.

## 2. Shape & Coverage

**Total rows**: 0  
**Countries loaded**: 0  
**Index structure**: Undefined (no data)

The Feature aggregation returns an empty DataFrame with zero rows and zero columns. The index contains a single `None` level (default pandas multi-index placeholder). No per-country row counts can be computed because no countries have contributed data.

**Expected coverage**: All 13 LSMS-ISA countries are agricultural programs. According to survey design documentation and the canonical index schema, plot-level data (plot ownership, area, crop composition, soil type, irrigation status, etc.) is standard in LSMS-ISA waves. The absence of declarations suggests either:
- (a) Table extraction scripts have not been written for any country.
- (b) The table was declared in the schema prematurely, before implementation began.
- (c) Historical data extraction has not been migrated to the current library version.

## 3. Columns Present vs Canonical

**Present**: None (empty DataFrame)  
**Canonical expected**: None defined in `data_info.yml`

The global `data_info.yml` lists `plot_features` in the Index Info section (with index structure `(t, v, i, plot_id)`) but provides **no column definitions** in the Columns section. This is a structural anomaly: the table is architecturally declared but has no schema specification.

This breaks the library's test suite: the parametrized test `test_column_table_has_countries()` in `tests/test_feature.py` (line 70–74) iterates over all tables listed in `data_info.yml` Columns and asserts that each has ≥1 country declaration. By design, this test would **fail for plot_features** if a Columns schema were added without corresponding country data_scheme declarations.

**Rogue columns**: Not applicable (no data).

## 4. Dtypes

No columns present; dtypes unknown.

## 5. Index Integrity

**Index names**: `[None]` (default; no actual multi-index present due to zero rows)  
**Duplicated index rows**: 0 (vacuously true for empty set)

The empty DataFrame has no meaningful index to validate. Once countries implement plot_features, the canonical index should be `(t, v, i, plot_id)` where:
- `t`: wave/survey year
- `v`: cluster identifier  
- `i`: household ID
- `plot_id`: plot identifier within household

## 6. Feature-Specific Observations

### De-Facto Schema (None)

No extraction scripts exist in the scanned countries. A plot_features table, once implemented, would typically contain:

- **Area/Size columns**: Plot area in hectares or country-specific units (e.g., kasha, kanal, manzana). Harmonization to hectares is critical for cross-country analysis.
- **Crop composition**: Primary and secondary crops planted, crop codes, acreage per crop.
- **Soil characteristics**: Soil type, color, drainage, erosion status.
- **Ownership and tenure**: Owner name, plot ownership type (owned, leased, inherited, communal), land rights documentation.
- **Irrigation**: Irrigation status (rainfed vs. irrigated), water source, irrigation method.
- **Spatial identifiers**: GPS coordinates (latitude/longitude), parcel/plot location within settlement.

### Area Unit Harmonization

Currently undefined (no data). This is a **critical known issue for agricultural data** (wave audits in prior SkunkWorks documents noted area unit divergence in other tables). Countries typically use distinct traditional units:
- Ethiopia: timad, hectare
- Malawi: acres, hectares
- Nigeria: plot size codes, acres
- Tanzania: acres, hectares
- Mali, Niger, Senegal: hectares, traditional local units
- Uganda: acres
- Burkina Faso, Benin, Togo, Guinea-Bissau: hectares, local units

Canonical harmonization to hectares (or explicit multi-unit columns) is essential. Without implementation, this will be a blocking issue for agricultural analysis.

### Plot Count Distribution per Household

Unknown (no data). Once populated, this metric is critical to understand:
- How many plots does the median household cultivate across countries/waves?
- Does this vary by country (e.g., Uganda: 1–2 plots vs. Ethiopia: 3–5)?
- Does fragmentation increase with wave progression?

### Null Fractions

Not computed (no columns, no data). Once implemented, per-country null fractions will reveal:
- Which countries record soil type vs. which skip it?
- Are GPS coordinates universally captured or country-specific?
- Does irrigation status reporting vary?

## 7. Wave Coverage per Country

Not applicable (zero rows). Once data is declared, per-country wave granularity will be revealed. Example expected structure:

| Country | Waves | Rows (est.) |
|---------|-------|-------------|
| Ethiopia | 2011-12, 2013-14, 2015-16, 2018-19, 2021-22 | 5,000–15,000 |
| Malawi | 2004-05, 2010-11, 2013-14, 2016-17, 2019-20 | 3,000–10,000 |
| Uganda | 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20 | 8,000–20,000 |
| Nigeria | 2010-11, 2012-13, 2015-16, 2018-19, 2023-24 | 7,000–18,000 |
| Other countries | 2–4 waves each | 2,000–8,000 |

(Estimate: ~100,000–150,000 total plot records across all LSMS-ISA waves once fully declared.)

## 8. Surprises & Critical Findings

### CRITICAL FINDINGS

1. **Complete coverage gap**: All 13 LSMS-ISA countries are silent on plot_features. This is **not sparse coverage** (as expected per probe instructions) but **zero coverage**.

2. **Schema-code mismatch**: `data_info.yml` declares plot_features in Index Info but provides no Columns schema. This violates the library's assertion that "every table with Columns should be declared by ≥1 country." The converse is also true but unenforced: "every table in Index Info should either have Columns + country declarations or be excluded from Index Info."

3. **Test failure**: The parametrized test `test_column_table_has_countries()` will correctly fail if a Columns section is added for plot_features without corresponding country data_scheme declarations. This is a **guard that prevents premature schema declaration**.

4. **Architectural blocker**: GhanaSPS, which is the designated "LSMS-ISA country" for Ghana, has **no data_scheme.yml file**. This suggests the country is not yet fully integrated into the library structure or is a placeholder awaiting implementation.

5. **Unit harmonization deferred**: This table is the primary vehicle for cross-country agricultural area harmonization. Its absence means the library has **no standardized hectare-converted area column** for plot-level analysis.

### Design Implication

The presence of plot_features in the canonical schema indicates **planned future development**. The probe successfully identifies that this is a **planned-but-unstarted feature**, not a bug or regression. The correct resolution is:

- **Short-term (v0.7.x)**: Either remove plot_features from `data_info.yml` Index Info entirely (if deprioritized), or add a minimal Columns schema (empty list) to clarify intentionality.
- **Medium-term (v0.8.0)**: Implement extraction scripts for ≥1 pilot country (recommend: Uganda or Ethiopia, which have longest LSMS-ISA wave series).
- **Long-term (v0.9.0+)**: Achieve full coverage (all 13 LSMS-ISA countries) with harmonized area columns and standard crop taxonomies.

## Conclusion

`plot_features` is **architecturally reserved but entirely unimplemented**. The Feature aggregation correctly returns zero rows because zero countries have declared it. This is not a data quality issue or a missing cache; it is a **schema-implementation mismatch** that should be resolved by either implementing the table or deferring its schema declaration to the next major version.

**Recommendation**: File a GitHub issue to clarify intent and assign implementation priority (this report documents that the table is on the roadmap but not yet available).
