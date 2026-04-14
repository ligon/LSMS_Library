# Food Prices Feature Audit Report

**Probe**: `ll.Feature('food_prices')()`  
**Date**: 2026-04-13  
**Status**: Mixed behavior across countries — derivation approach is country-specific

## 1. Scope Deviations

**STATUS: Behavior variance, not deviation**

The `food_prices` feature is auto-derived at runtime (not registered in `data_scheme.yml`) and follows two distinct derivation paths depending on country configuration:

- **Derived via transformation** (Benin, Burkina_Faso, CotedIvoire, GhanaLSS, Guinea-Bissau, Mali, Niger, Nigeria, Senegal, Togo): Calls `food_prices_from_acquired()` which computes unit values from raw food_acquired and returns **household-level median prices**.
- **Legacy item-level output** (Uganda, Tanzania): Loads pre-computed item × unit level prices from wave-level `food_prices.parquet` scripts.

This is documented in `CLAUDE.md` (line 114): "auto-derived at runtime via `_FOOD_DERIVED` in `country.py`; transform in `transformations.py`." The dual-path behavior is intentional but introduces index/column inconsistency across the Feature aggregation.

## 2. Shape & Coverage

Testing revealed severe build timeouts when aggregating across all 15 countries. Benin and Uganda only were successfully loaded:

| Country | Total Rows | Waves | Index Structure | Notes |
|---------|-----------|-------|-----------------|-------|
| **Benin** | 8,004 | 1 (2018-19) | (country, i, t, v) | Derived; household-level aggregation |
| **Uganda** | 352,913 | 8 (2005-06 to 2019-20) | (country, i, t, v, j, u) | Legacy script; item × unit level |

**Countries with food_acquired** (expected to support food_prices): Benin, Burkina_Faso, CotedIvoire, Ethiopia, GhanaLSS, Guinea-Bissau, Malawi, Mali, Nepal, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda (15 total).

**Build status**: Ethiopia, Malawi, and other countries' food_acquired failed to materialize within 120 seconds, preventing full cross-country audit. Likely causes: DVC stage layer missing, incomplete wave configurations, or very large data volumes (Tanzania).

## 3. Columns Present vs. Canonical

### Benin (Derived Transformation Path):
- **Expected**: Single `Price` column (per `food_prices_from_acquired()` contract)
- **Actual**: `Price` (Float64) — correct
- **Rogue columns**: None

### Uganda (Legacy Script Path):
- **Expected**: Unknown; script pre-computed
- **Actual**: 9 columns — `market`, `farmgate`, `unitvalue_home`, `unitvalue_away`, `unitvalue_own`, `unitvalue_inkind`, `market_home`, `market_away`, `market_own` (all Float64)
- **Rogue columns**: None; these are intentional from the legacy `food_prices_quantities_and_expenditures.py` script

**Critical finding**: No canonical column schema exists for food_prices in `data_info.yml`. Each country's food_acquired structure varies (raw units in Benin; pre-computed unit values in Uganda). The derivation produces incompatible outputs across countries.

## 4. Dtypes

### Benin:
| Column | Dtype | Status |
|--------|-------|--------|
| `Price` | Float64 | Correct (nullable float) |

### Uganda:
| Column | Dtype | Status |
|--------|-------|--------|
| All 9 price columns | Float64 | Correct (nullable float) |

Both countries properly use `Float64` (pandas nullable). No object/string columns that should be numeric.

## 5. Index Integrity

### Benin:
- **Index names**: (country, i, t, v)
- **Index nlevels**: 4
- **Duplicated rows**: 0
- **Row count**: 8,004 = unique (i, t, v) combinations

### Uganda:
- **Index names**: (country, i, t, v, j, u)
- **Index nlevels**: 6
- **Duplicated rows**: 0
- **Row count**: 352,913 = unique (i, t, v, j, u) combinations

Both countries have clean, unique indices with no duplicates.

## 6. Feature-Specific: Food Prices

### Unit Handling & Cross-Country Coherence

**Benin (Derived)**:
- Raw food_acquired has 187,672 rows across 137 distinct food items
- Each item purchased with various units (Sachet, Boule, Tas, Tohoungolo, etc.) and non-standard quantities
- Transformation converts all units to kg using `KNOWN_METRIC` mapping + price-ratio inference
- **Aggregation level**: Household-level median price across all items
- Interpretation: Each Benin row is one household's median price per kg across all foods they purchased in 2018-19

**Uganda (Legacy)**:
- food_acquired already contains pre-computed unit values by source type (market, farmgate, home-produced, away-from-home)
- Output maintains item × unit granularity — not household-level
- Each row is item × unit; price columns represent different acquisition channels

**Coherence check** (attempting cross-country price comparison):
- Benin median price: 400 CFA francs (min 22.3, max 9,329 per kg after unit conversion)
- Uganda market median: ~1,505 UGX (sample from 2005-06 wave: 250–3,000 per kg)
- Units and currencies differ; no direct comparison possible without FX and unit harmonization

### Per-Country Price Summary

**Benin (2018-19)**:
- **Min**: 22.3 (per kg after conversion)
- **Max**: 9,329.8
- **Median**: 400.0
- **Mean**: 453.1 (std: 268.4)
- **Zero prices**: 0
- **Negative prices**: 0
- **Distribution**: Heavily right-skewed; 75th percentile at 524.2 but max extreme outlier (9,329) is 18× the median

**Uganda (sample from 2005-06 wave)**:
- **Item count**: ~50–100 items per wave
- **Price range**: 130–3,000 UGX per kg for market purchases
- **Distinct unit types**: Kg, Liter, Bunch, etc. (unified in legacy script)

### Rows Where Price == 0 or < 0

**Benin**: 0 zero prices, 0 negative prices after filtering (`dropna()` in transformation)  
**Uganda**: Data not fully audited due to scale, but legacy script also filters 0 and NaN

The `food_prices_from_acquired()` transformation explicitly drops zero and infinite prices (line 320 in `transformations.py`): `replace([0, np.inf, -np.inf], np.nan).dropna()`

## 7. Wave Coverage Per Country

**Benin**: 1 wave (2018-19 only)  
**Uganda**: 8 waves (2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20)

Benin EHCVM data (single recent round); Uganda LSMS panel with 15-year coverage.

## 8. Surprises & Secondary Issues

### **Issue 1: Household-Level Aggregation Loses Item Information**

The derived `food_prices_from_acquired()` transformation (line 323–327 in `transformations.py`) groups by `['t', 'v', 'm', 'i']` — **explicitly excluding 'j' (food item)**. Each Benin row represents the **median price across all items** a household purchased, not item-level prices.

This is unintuitive: "food prices" suggests item-level unit values (rice, beans, oil), but the actual output is a summary statistic. Downstream consumers expecting item-level price data will be misled.

**Root cause**: The transformation was designed to compute market-level price indices (median across all purchases), not provide food price catalogs. No canonical documentation of this aggregation policy exists.

### **Issue 2: Uganda Takes Incompatible Code Path**

Uganda's food_prices comes from a pre-computed legacy script (`food_prices_quantities_and_expenditures.py`) that maintains item × unit granularity and includes multiple price type columns (market, farmgate, home value, etc.). When called via `ll.Feature('food_prices')()`, Uganda's output is structurally incompatible with Benin's derived output:

- **Benin**: (country, i, t, v) index; `Price` column
- **Uganda**: (country, i, t, v, j, u) index; 9 columns (`market`, `farmgate`, etc.)

`Feature.food_prices()` cross-country aggregation cannot concatenate these cleanly. The Feature layer must either:
1. Harmonize all countries to a single index/column structure, or
2. Document the incompatibility and fail visibly

### **Issue 3: No Canonical Schema for food_prices Columns**

Unlike `household_roster` (which has required fields in `data_info.yml`), `food_prices` has no column specification in the canonical schema. Countries produce arbitrary outputs:
- Benin/derivation: `Price`
- Uganda: `market`, `farmgate`, `unitvalue_home`, …

Without enforcement, cross-country merges will silently drop or duplicate columns.

### **Issue 4: Unit Conversion Uncertainty in Benin**

The transformation uses `_get_kg_factors()` which falls back to price-ratio inference when items lack known unit conversions. For Benin's many local units (Boule, Tohoungolo, Yoroukou, etc.), this inference may be unreliable. Outlier prices (max 9,329 vs median 400) suggest some unit conversions are wrong.

Example: A household reporting "Sorgho / Yoroukou (1 unit) for 1,000 CFA francs" — if the inferred kg-per-Yoroukou is wrong by 2×, the imputed price per kg is wrong by 2×.

### **Issue 5: Index Reordering in Feature Aggregation**

Benin's derived prices come out with index (t, v, i) from the transformation (as seen in manual testing), but the Feature layer reports (country, i, t, v) after country prepending and index harmonization. **The canonical order in data_info.yml is (t, i)** for household-level tables. The actual (i, t, v) reordering is applied in `_finalize_result()`. This is documented in CLAUDE.md (line 121) but worth flagging: the Feature output does not preserve canonical index order.

---

## Recommendations

1. **Define canonical schema for food_prices** in `data_info.yml` with required columns (`Price` or `market`, `farmgate`, etc.) and mandatory index (e.g., `(t, i, j)` for item-level or `(t, i)` for household-level).

2. **Clarify aggregation level** in docstrings: food_prices is household-level median, not item-level. Consider renaming to `food_price_index` or adding a detailed docstring to `food_prices_from_acquired()`.

3. **Reconcile Uganda's dual-column output**: Either
   - Collapse to a single canonical `Price` column for Feature compatibility, or
   - Register Uganda's food_prices_quantities_and_expenditures as a separate feature (`food_prices_detailed` or similar).

4. **Validate unit conversions**: Audit the price-ratio inference in `_get_kg_factors()` using known item conversions (e.g., from FAO conversion tables) to catch systematic bias in local units like Boule, Tas, etc.

5. **Test cross-country build**: Food_acquired for Ethiopia, Malawi, Tanzania, etc. currently fails to materialize. Either fix the underlying data or flag countries as unsupported in food_prices documentation.

6. **Add unit provenance column**: Store the unit conversion method used (`KNOWN_METRIC` vs. `inferred`) so consumers can assess data reliability downstream.

---

## Status 2026-04-13

**`food_prices_from_acquired` returns raw per-observation prices — RESOLVED.** Commit `5f5c2692` rewrites `food_prices_from_acquired()` in `transformations.py` to return raw per-observation unit prices rather than the household-level median price index that was documented in §8 Issue 1. The aggregation behavior (median across all items per household) that was confusing "food prices" semantics is replaced by per-record price output.

**Index structure** (§8 Issue 5: `(i, t, v)` reordering) — behaviour may be affected by the `5f5c2692` rewrite; to be confirmed in a follow-up probe.

**Uganda incompatible output path** (§8 Issue 2: 9-column legacy vs 1-column `Price`) — still open; Uganda's legacy `food_prices_quantities_and_expenditures.py` not touched in this session.

**Unit conversion uncertainty** (§8 Issue 4) and **no canonical schema for food_prices columns** (§8 Issue 3) — still open.

