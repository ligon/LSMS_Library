# Food Acquired Feature Audit

**Execution**: 2026-04-12  
**Probe**: `ll.Feature('food_acquired')()`  
**Total rows**: 5,128,136 | 54 columns  
**Canonical index spec**: `(t, m, v, i, j, u)` | **Observed**: `(country, hh_id, t, v, i, j, u)` — **7 levels**

---

## 1. Scope Deviations

**Status**: Index structure mismatch detected.

The canonical schema specifies index `(t, m, v, i, j, u)` (6 levels) but the Feature output presents `(country, hh_id, t, v, i, j, u)` (7 levels). The `country` and `hh_id` dimensions are added by the cross-country aggregation layer in `Feature()`, not part of individual country tables. Critically, `m` (market/region) is **demoted to a column** rather than remaining in the index, appearing as `m` (string dtype, 87.2% null, Nigeria-only). This is a **scope deviation from spec**, indicating that the `_finalize_result` or `_join_v_from_sample` methods are not properly preserving index semantics for food_acquired across countries.

---

## 2. Shape & Coverage

**Total rows**: 5,128,136  
**Countries loaded**: 13/47 registered  
**Per-country distribution**:

| Country | Rows | % of total |
|---------|------|-----------|
| GhanaLSS | 1,459,786 | 28.5% |
| Nigeria | 646,185 | 12.6% |
| Mali | 433,779 | 8.5% |
| Senegal | 433,871 | 8.5% |
| Burkina_Faso | 331,165 | 6.5% |
| CotedIvoire | 292,741 | 5.7% |
| Ethiopia | 264,418 | 5.2% |
| Tanzania | 250,302 | 4.9% |
| Niger | 237,733 | 4.6% |
| Uganda | 352,913 | 6.9% |
| Benin | 187,672 | 3.7% |
| Guinea-Bissau | 125,484 | 2.4% |
| Togo | 112,087 | 2.2% |

**Silently absent** (attempted but failed or not implemented): Malawi (failed Makefile), Nepal (failed materialization), and 34 non-LSMS countries in the registry (no data sources).

---

## 3. Columns vs Canonical

**Expected canonical columns**: `Quantity`, `Price`, `Produced`  
**Observed**: 54 columns total; only 3 match canonical.

**Rogue columns identified** (51 non-canonical):
- **Core price/quantity variants**: `Expenditure`, `quantity`, `units`, `total expenses`, `quantity obtained`, `units obtained`, `price per unit`, `value_purchased`, `quantity_purchased`, `unitvalue`, `Kgs`, `Kgs Purchased`, `purchased_value`, `purchased_value_yearly`, `produced_value_daily`, `produced_value_yearly`, `produced_price`, `produced_quantity`, `value_purchase`, `unitvalue_purchase`
- **Decomposition columns**: `quant_ttl_consume`, `unit_ttl_consume`, `quant_purchase`, `unit_purchase`, `quant_own`, `unit_own`, `quant_inkind`, `unit_inkind`, `unitvalue_home`, `unitvalue_away`, `unitvalue_own`, `unitvalue_inkind`
- **Source/market annotations**: `market`, `farmgate`, `market_home`, `market_away`, `market_own`, `value_home`, `value_away`, `value_own`, `value_inkind`, `quantity_home`, `quantity_away`, `quantity_own`, `quantity_inkind`
- **Metadata**: `h`, `visit`, `agg_u`, `Purchased`, `m`

**Implication**: The aggregation is conflating country-specific decompositions and source-value splits (purchased vs own-produced vs inkind) into the canonical table without harmonization. This violates the principle that `food_acquired` should be a uniform cross-country snapshot.

---

## 4. Dtypes

**Canonical numeric columns**:
- `Quantity` (Float64): Correct
- `Price` (Float64): Correct (but 98.1% null; see Section 6)
- `Produced` (Float64): Correct

**Mixed/problematic types**:

| Column | Dtype | Issue |
|--------|-------|-------|
| `units` | object/str | Expected to be implicit in index `u` |
| `h` | str | Numeric-like strings (100% parseable as float) |
| `u` | str | Unit codes; mixed symbolic and numeric across countries |
| `m` | string | Market annotations; Nigeria-only (6 unique values) |
| `total expenses` | string | **Stringified floats** (100% numeric) |
| `quantity obtained` | string | **Stringified floats** (100% numeric) |
| `price per unit` | string | **Stringified floats** (100% numeric) |
| `purchased_value` | string | **Stringified floats** (100% numeric) |

**Flag**: 5 string columns contain 100% numeric-like data; should be numeric. Indicates country-level source data was not properly coerced during aggregation.

---

## 5. Index Integrity

**Index type**: Plain `pd.Index` (not MultiIndex)  
**Index names**: `[None]` — unnamed, should be `['country', 'hh_id', 't', 'v', 'i', 'j', 'u']`  
**Duplicates**: 0  
**Concentration**: No duplicates across 5M+ rows; index uniqueness is preserved.

**Issue**: Index is a tuple of strings rather than a named MultiIndex. This breaks introspection and standard pandas accessor patterns (e.g., `foo.index.get_level_values('country')` fails).

---

## 6. Food_acquired-Specific Analysis

### Item (j) Harmonization

**Distinct items globally**: 1,039 unique values across 13 countries.

**Per-country item counts**:
- Niger: 289 items
- Mali: 236 items
- Senegal: 177 items
- Benin: 137 items
- Togo: 138 items
- CotedIvoire: 136 items
- Guinea-Bissau: 135 items
- Uganda: 128 items
- Burkina_Faso: 201 items
- Ethiopia: 114 items (note: many entries are unit names, not items; see below)
- GhanaLSS: 1 item (all NaN)
- Nigeria: 82 items (note: many entries are numeric codes)
- Tanzania: 1 item (all NaN)

**Samples (first 10 per country)**:
- Benin: Afintin (Moutarde), Ail, Beurre de karitÃ©, Cube alimentaire, Gombo frais, ...
- Nigeria: 6.0, 5.0, 1.0, 3.0, 2.0, 4.0, nan, 41.0, 61.0, 91.0 (numeric codes, not labels)
- Uganda: Kg, Bottle(500ml), Akendo (Small), Fish Whole (Medium), ... (mix of units and items)

**Harmonization status**: **No canonical vocabulary**. Each country emits local item labels in local languages (French for West Africa, local names for Ethiopia/Uganda). No `categorical_mapping/harmonize_food` bridge exists; items remain country-specific strings or numeric codes. GhanaLSS and Tanzania have corrupted item columns (all NaN).

### Unit (u) Harmonization

**Distinct units globally**: 385 unique values.

**Per-country unit counts**:
- Benin: 79 units
- Ethiopia: 114 units (many are named measures: Kubaya/Cup, Kunna/Mishe, etc.)
- Burkina_Faso: 59 units (many numeric codes: 101, 103, 104, ...)
- Niger: 78 units (numeric codes: 100, 101, 102, ...)
- Mali: 63 units
- Senegal: 62 units (numeric codes + symbolic: 1, Barquette, Bassine, Bol, ...)
- CotedIvoire: 52 units
- Togo: 38 units (numeric codes)
- Guinea-Bissau: 29 units
- GhanaLSS, Nigeria, Tanzania, Uganda: 0 units (all NaN)

**Samples**:
- Benin: Abotoca, Agoua, Alvéole/Plateau, Avec os au Kg, Bassine, ...
- Burkina_Faso, Niger, Togo, Senegal: Heavy numeric encoding (101=cup, 102=bag, etc.)
- Ethiopia: Named measures (e.g., "101. Kubaya/Cup Small", "111. Kunna/Mishe/Kefer/Enkib Small")
- Guinea-Bissau: Portuguese units (Bacia, Balde, Cacho, Calma, Caneca, Colher, Copo, ...)

**Harmonization status**: **No canonical set**. Countries use numeric codes (which may differ in meaning), local language labels (French, Portuguese), and named measures. Countries with zero non-null units (GhanaLSS, Nigeria, Tanzania, Uganda) skew the aggregation; no fallback to a standard "kg" or "item" default.

### Price Distribution

**Column**: `Price` (Float64)  
**Global non-null**: 45,142 / 5,128,136 (0.9%)  
**Negatives**: 0  
**Zeros**: 36

**Per-country breakdown**:
- GhanaLSS: 45,142 non-null (3.1% of 1.46M rows) | Range [0, 60,000] | P25=1, P50=3, P75=6
- All other countries: 0% non-null (Price column is entirely NaN)

**Implication**: Price is **almost entirely absent**. Only Nigeria potentially has price data in other columns (e.g., `price per unit`, `unitvalue`, `unitvalue_purchase`, which are strings). The canonical `Price` column is effectively a placeholder; real price information lives in 51 rogue columns with inconsistent names and dtypes.

### Quantity Distribution

**Column**: `Quantity` (Float64)  
**Global non-null**: 4,480,994 / 5,128,136 (87.4%)  
**Negatives**: 0  
**Zeros**: 65,989 (1.5% of non-null)

**Per-country breakdown**:
- Benin: 100% non-null | Range [0.04, 420] | P25=1, P50=2, P75=4
- CotedIvoire: 100% non-null | Range [0.01, 350] | P25=1, P50=2, P75=4
- Mali: 100% non-null | Range [0, 19,999.8] | P25=1, P50=2, P75=5 (includes large outliers)
- Nigeria: 99.97% non-null | Range [0, 15,000] | P25=1, P50=3, P75=10
- Niger: 100% non-null | Range [0, 700] | P25=1, P50=2, P75=5
- Senegal: 100% non-null | Range [0, 700] | P25=1.5, P50=3.25, P75=7
- Togo: 100% non-null | Range [0.01, 1,750] | P25=1, P50=2, P75=3
- Uganda: 0% non-null (all NaN)
- Guinea-Bissau: 100% non-null | Range [0, 400] | P25=1.5, P50=3, P75=7
- Burkina_Faso: 62% non-null (125K nulls) | Range [0, 550] | P25=1, P50=2, P75=5
- Ethiopia: 0% non-null (all NaN)
- GhanaLSS: 23.4% non-null (1.1M nulls) | Range [0, 7,500] | P25=1, P50=2, P75=3
- Tanzania: 0% non-null (all NaN)

**Quality concern**: Ethiopia, Tanzania, Uganda missing Quantity entirely; GhanaLSS severely sparse (77% of rows lack quantity).

### Market/Region (m) Index Level

**Expected**: `m` as 6th index level (after `t`, `v`, `i`, `j`, `u`)  
**Observed**: `m` demoted to column; string dtype  
**Non-null**: 646,185 / 5,128,136 (12.6%; Nigeria-only)  
**Unique values**: 6 (e.g., market names in Nigeria context)  
**Per-country presence**:
- Nigeria: 100% non-null, 6 unique values
- All others: 0% non-null (all NaN)

**Impact**: `m` is not part of the index structure. Only Nigeria populates it; other countries' market/region data, if present, remain unexposed. This breaks the canonical spec's promise to index on markets across countries.

---

## 7. Wave Coverage vs Registered Waves

**Per-country waves loaded**:

| Country | Waves | Count |
|---------|-------|-------|
| Benin | 2018-19 | 1 |
| Burkina_Faso | 2013_Q4, 2018-19, 2021-22 | 3 |
| CotedIvoire | 2018-19 | 1 |
| Ethiopia | 2011-12, 2013-14, 2015-16, 2018-19, 2021-22 | 5 |
| GhanaLSS | 1987-88, 1988-89, 1991-92, 1998-99, 2005-06, 2012-13, 2016-17 | 7 |
| Guinea-Bissau | 2018-19 | 1 |
| Mali | 2014-15, 2018-19, 2021-22 | 3 |
| Niger | 2018-19, 2021-22 | 2 |
| Nigeria | 2010Q3, 2011Q1, 2012Q3, 2013Q1, 2015Q3, 2016Q1, 2018Q3, 2019Q1 | 8 |
| Senegal | 2018-19, 2021-22 | 2 |
| Tanzania | 2008-09, 2010-11, 2012-13, 2014-15, 2019-20, 2020-21 | 6 |
| Togo | 2018 | 1 |
| Uganda | 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20 | 8 |

**Consistency**: Wave labels vary in format (year-year, year_Q, yearQ). No truncation/standardization performed before aggregation.

---

## 8. Surprises & Framework Bugs

1. **Stringified numeric columns**: Five columns (`total expenses`, `quantity obtained`, `price per unit`, `purchased_value`, `h`) are string dtype but contain 100% parseable floats. Indicates downstream country aggregators are not coercing dtypes uniformly.

2. **Corrupt item columns**: GhanaLSS and Tanzania have `j` (item) completely null; instead of failing or flagging, the Feature() aggregator silently includes 1.7M null rows. The probe should warn or skip these countries.

3. **Nigeria numeric item codes**: Nigeria's `j` column contains raw numeric codes (1.0, 2.0, 3.0, ..., 91.0) instead of item labels, indicating source data was not harmonized.

4. **Missing canonical Price**: The canonical `Price` column is 99.1% null globally (present only in GhanaLSS). Other countries stash price info in rogue columns like `price per unit`, `unitvalue`, or `unitvalue_purchase` as strings. Suggests aggregation merged tables with different schemas without reconciliation.

5. **Index type regression**: Index should be a named MultiIndex for `(country, hh_id, t, v, i, j, u)` but is a plain tuple Index with no names. This breaks `.index.get_level_values()` and other MultiIndex introspection. Likely a bug in `_finalize_result()`.

6. **m demoted from index**: The canonical spec mandates `m` in the index; `_finalize_result` or `_join_v_from_sample` appears to drop it from the index and leave it as a column, and then only populate it for Nigeria.

7. **Unit codes vs labels**: Countries use numeric unit codes (Burkina, Niger, Senegal, Togo) that may not be harmonized across surveys. Ethiopia uses prose descriptions ("Kubaya/Cup Small") while Guinea-Bissau uses Portuguese names. No canonical mapping exists.

---

## Summary

The `food_acquired` Feature aggregation exhibits **class-A framework issues**:
- Index structure does not match canonical spec (missing `m`, unnamed levels, plain Index vs MultiIndex).
- 51 of 54 columns are non-canonical, indicating ad-hoc merging of country-specific price/quantity decompositions.
- Stringified numeric data and corrupt (null) item columns suggest missing dtype enforcement in country-level aggregators.
- Price almost completely absent (0.9% non-null); Quantity sparse or missing in 4 countries.
- Item and unit vocabularies are unharmonized; no cross-country mapping bridge.
- Tanzania, GhanaLSS, Ethiopia, Uganda lack critical dimensions (Quantity or item codes).

**Root cause**: The aggregation layer does not enforce the canonical schema before merging country tables. Country-specific decompositions (e.g., Nigeria's market breakdown, Mali's purchased/own/inkind splits) are exposed as rogue columns rather than being normalized or filtered. This breaks composability and API consistency.

---

## Status 2026-04-13

**GhanaLSS `lsms.tools` dependency — RESOLVED.** Commit `c5e2d726` replaces the retired `lsms.tools.from_dta` in `GhanaLSS/food_acquired` with `local_tools.get_dataframe`, fixing the `ModuleNotFoundError` on cold cache for GhanaLSS.

**CotedIvoire expenditures `lsms.tools` dependency — RESOLVED.** Commit `4a59e418` fixes the retired `lsms.tools.get_food_expenditures` call in CotedIvoire expenditures path.

**83 extra non-canonical columns (§3) — STILL OPEN.** The 51-column leakage (country-specific decompositions, stringified numerics, corrupt item columns for GhanaLSS/Tanzania) has not been addressed. No canonicalisation layer was added in this session. This remains the primary structural finding requiring follow-up work.

**Index structure mismatch** (unnamed MultiIndex, `m` demoted to column, `hh_id` in index) — still open; no framework change to food_acquired index handling.

**Nigeria numeric item codes** and **item/unit vocabulary harmonization** — still open.

**Follow-up**: The 83-column leakage is a blocking issue for cross-country food_acquired use and should be the top priority for the next food_acquired sprint.

