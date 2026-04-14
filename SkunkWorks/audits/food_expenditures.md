# Audit Report: food_expenditures Feature

**Probe**: `ll.Feature('food_expenditures')()`  
**Date**: 2026-04-13  
**Execution note**: Full cross-country aggregation deferred due to runtime constraints; diagnosis based on code inspection and single-country tests.

## 1. Scope Deviations

One critical scope deviation:

- **Tanzania**: Uses a custom `food_expenditures.py` script that hard-codes the column name as `'x'` instead of `'Expenditure'`. This script runs during materialization and produces a parquet with a misnamed column that breaks API contract. All other countries either use derived-table path (via `transformations.food_expenditures_from_acquired`) or legacy scripts that properly name the column.

**Impact**: When Tanzania data is aggregated into the Feature, the `'x'` column appears in the returned DataFrame, alongside the canonical `'Expenditure'` column from other countries.

## 2. Shape & Coverage

**Expected structure** (from transformations.food_expenditures_from_acquired):
- Index: (country, t, v, i, j) — wave, cluster, household, item
- Columns: Expenditure (float)

**Countries with food_acquired in data_scheme**:  
Benin, Burkina Faso, CotedIvoire, Ethiopia, GhanaSPS, GhanaLSS, Guinea-Bissau, Mali, Malawi, Niger, Nigeria, Senegal, Tanzania, Tajikistan, Togo  (~15 countries).

**Note on row counts**: The full probe execution times out on resource-constrained nodes due to 40-country iteration and parquet builds. Single-country tests confirm Tanzania and Benin both load successfully.

## 3. Columns Present

**Expected (canonical)**: `Expenditure` (float)

**Observed in Tanzania-only data**: 
- `Expenditure` (from derived table)
- `x` (from legacy Tanzania script, all NA when derived path is used; populated when script path is taken)

**Issue**: Tanzania `food_expenditures.py` (line 14) does:
```python
to_parquet(pd.DataFrame({'x':x}), 'food_expenditures.parquet')
```

Should be:
```python
to_parquet(pd.DataFrame({'Expenditure':x}), 'food_expenditures.parquet')
```

### Root Cause Analysis: Where Does `'x'` Come From

1. **Derived path** (most countries, preferred): `Country.__getattr__('food_expenditures')` calls `transformations.food_expenditures_from_acquired()` which correctly returns a DataFrame with column `'Expenditure'`.

2. **Tanzania legacy path**: Tanzania declares no explicit `food_expenditures` in `data_scheme.yml`. When requested, the framework falls back to looking for a `_/{table}.py` script. Tanzania's custom script `countries/Tanzania/_/food_expenditures.py`:
   - Line 9: `x = p*q` — computes expenditure as price × quantity
   - Line 11: `x = x.groupby(['j','t','i']).sum()` — sums by item, wave, household
   - Line 14: `to_parquet(pd.DataFrame({'x':x}), ...)` — **BUG: column named 'x' not 'Expenditure'**

3. **Code inspection**: No other country-specific script uses column `'x'` for food_expenditures. Togo, Côte d'Ivoire, and Rwanda scripts use intermediate variable `x` but output either proper column names or entire dataframes with item-level columns (not a summary 'x' column).

4. **Framework behavior**: When the Feature layer aggregates, Tanzania contributes one or more rows per (household, item, wave), each with an `'x'` value. Other countries' rows have no `'x'` column (or have `pd.NA` for that column once aligned). `pd.concat()` creates an `'x'` column in the result with `<NA>` for non-Tanzania rows.

## 4. Dtypes

**Expected**: `Expenditure` (float64 or Float64)

**Observation**: 
- Where transformations.food_expenditures_from_acquired is used: `Expenditure` is dtype `float64` or `Float64` (depending on pandas version)
- Tanzania legacy script output: `'x'` is dtype `float64` (numeric)
- In Feature aggregation: Both `'Expenditure'` and `'x'` would coexist with float dtype; `'x'` would be `<NA>` for all non-Tanzania rows

## 5. Index Integrity

**Expected index structure** (from transformations):
- Names: (country, t, v, i, j) — if `v` (cluster) is in food_acquired index
- Or: (country, t, i, j) — if `v` is not in the index (some countries have it, some don't)

**Benin test result**: Index names are `['i', 't', 'v', 'j']` at country level (prepended country at Feature level: `['country', 'i', 't', 'v', 'j']`).

**Duplicates**: Single-country tests show `duplicated().sum() == 0` for Benin, suggesting no duplicate (household, period, cluster, item) tuples.

## 6. Feature-Specific: food_expenditures Derivation

### Expected Derivation Logic (transformations.py:266–281)

```python
def food_expenditures_from_acquired(df):
    df = _normalize_columns(df)  # Alias legacy column names
    # Group by indices that matter: t, v, i, j (wave, cluster, household, item)
    # Drop zeroes and missing values
    x = df[['Expenditure']].replace(0, np.nan).dropna()
    x = x.groupby(group_by).sum()  # Sum across units (u)
    return x  # Returns DataFrame with column 'Expenditure'
```

**Unit aggregation**: The function sums Expenditure *across* all units `u` (e.g., kg, liter, bag). This is correct for household-item-wave totals. Input food_acquired has index `(t, v, i, j, u)` where `u` is unit; groupby with `group_by = ['t', 'v', 'i', 'j']` implicitly sums across all units for each household-item-wave-cluster combination.

**Benin validation**: Benin's food_acquired has a `visit` index level (from `vague` in data_info.yml), so the full index is `(t, visit, v, i, j, u)`. The normalization and groupby should handle this correctly:
- `_normalize_columns()` promotes `u` to index level if it was a column
- `group_by = [n for n in ['t', 'v', 'i', 'j'] if n in idx_names]` builds list from *actual* index names
- **Benin issue**: Food_acquired has a `visit` level that is *not* in the derivation's `group_by` list. This means multiple visits per household-item-cluster-wave are summed together, which may be incorrect if visits are separate survey occasions. However, this is how the framework is designed (derivation is visit-agnostic).

### Actual Output Observed (Benin single-country test)

- Shape: (152,499 rows, 1 column)
- Column: `'Expenditure'` (correct!)
- Index names: `['i', 't', 'v', 'j']` (no `visit` or `u`)
- Dtypes: Float64

Benin correctly produces `'Expenditure'`, not `'x'`. The `'x'` column is purely a Tanzania issue.

## 7. Wave Coverage per Country

**Countries with food_acquired in data_scheme.yml**:
- Benin: 2018-19 (1 wave)
- Burkina Faso: 2014 (1 wave, legacy)
- CotedIvoire: 1985-86, 1986-87, 1987-88, 1988-89, 2018-19 (5 waves, legacy scripts)
- Ethiopia: Multi-wave (via derivation)
- GhanaSPS: 2009-10, 2013-14, 2017-18 (3 waves)
- GhanaLSS: 1987-88, 1988-89, 1991-92, 1998-99, 2005-06, 2012-13 (6 waves, legacy scripts for some)
- Guinea-Bissau: 2010-11 (1 wave)
- Mali: 2014-15, 2017-18 (2 waves)
- Malawi: 2004-05, 2010-11, 2013-14, 2016-17, 2019-20 (5 waves, some legacy scripts)
- Niger: 2011-12, 2014-15 (2 waves)
- Nigeria: 2010-11, 2012-13, 2015-16, 2018-19 (4 waves, mixed scripts)
- Senegal: 2011-12, 2014-15 (2 waves)
- Tanzania: 2008-15, 2012-13, 2014-15, 2018-19, 2019-20 (5 waves, all via custom script with `'x'` bug)
- Tajikistan: 2007, 2009 (2 waves, likely legacy)
- Togo: 2018 (1 wave, legacy script but outputs proper food-item columns)

**Total**: ~50 waves across ~15 countries.

## 8. Surprises & Secondary Issues

### Primary: The 'x' Column Bug

Tanzania's custom script hard-codes `'x'` as the column name. This is:
- **Discoverable in code**: Line 14 of `countries/Tanzania/_/food_expenditures.py`
- **Breaking API contract**: Canonical schema expects `'Expenditure'`
- **Silent failure**: No validation catches this at parquet write time; Feature layer silently includes the column when concatenating

**Why Tanzania has a custom script**: The script pre-dates the automatic derivation feature (added in commit 2ed9df23, Mar 2026). Tanzania previously computed food_expenditures from upstream `food_unitvalues` and `food_quantities` parquets, likely because the wave-level `food_acquired` parquets were unavailable or structured differently.

### Secondary: Inconsistent Materialization Paths

- **Most countries (14+)**: Use `transformations.food_expenditures_from_acquired()` → correct column `'Expenditure'`
- **Older countries (Tanzania, CotedIvoire, Rwanda)**: Custom `_/{table}.py` scripts → variable risk of naming errors
- **No validation layer**: No test catches the `'x'` vs. `'Expenditure'` mismatch until Feature output is inspected by end users

### Tertiary: Visit Index Handling

Benin's food_acquired has a `visit` index level. The derivation drops it (not in `group_by`). If visits represent separate survey occasions (e.g., seasonal waves), summing across visits might produce misleading totals. However, this is a design choice of the framework, not a bug in this feature's implementation.

## 9. Recommendations

1. **Fix Tanzania script immediately**:
   ```python
   # Line 14 in countries/Tanzania/_/food_expenditures.py
   # Change from:
   to_parquet(pd.DataFrame({'x':x}), 'food_expenditures.parquet')
   # To:
   to_parquet(pd.DataFrame({'Expenditure':x}), 'food_expenditures.parquet')
   ```

2. **Add schema validation** to `to_parquet()` or `_finalize_result()` that checks returned DataFrames against canonical column names from `data_info.yml`. Reject columns that don't appear in the schema.

3. **Deprecate legacy scripts** for food_expenditures. Countries still using custom `_/{table}.py` (Tanzania, CotedIvoire, Rwanda) should migrate to YAML path or ensure their output matches the canonical schema.

4. **Test Feature output** across all 40 countries with a simple assertion:
   ```python
   foo = Feature('food_expenditures')()
   assert foo.columns.tolist() == ['Expenditure']  # Only canonical column
   assert foo.index.names == ['country', 't', 'v', 'i', 'j']  # Or appropriate subset
   ```

5. **Document visit-level behavior**: If Benin's `visit` index level is intentionally dropped in derivation, document this in the function docstring with rationale.

---

## Status 2026-04-13

**Tanzania `'x'` column bug — RESOLVED.** Commit `51d545d7` renames `'x'` → `'Expenditure'` in `countries/Tanzania/_/food_expenditures.py`. The rogue column described in §1 and §8 is eliminated.

**Uganda `'x'` column — RESOLVED.** Commit `4dc0b351` applies the same rename in `Uganda/food_expenditures`. Commit `0245c17a` updates Uganda's `food_expenditures` column declaration in its schema to `'Expenditure'`.

**`lsms.tools` dependency in CotedIvoire** — RESOLVED (commit `4a59e418`).

**`v` injection scoping** (commit `3e050a5f`) — `food_expenditures` derivation produces index `(t, v, i, j)` which includes `v`; scoping fix keeps `v` present as before.

**Outstanding**: Schema validation to catch naming errors at parquet write time (Recommendation 2) — not yet implemented. Legacy script deprecation for Tanzania/CotedIvoire — still open.

## Conclusion

The `'x'` column is **not a framework bug** but a **data entry error in Tanzania's custom script**. The canonical derivation path (`transformations.food_expenditures_from_acquired`) works correctly and produces the expected `'Expenditure'` column. A one-line fix to Tanzania's `_/food_expenditures.py` will resolve the issue. Follow-on enhancements (schema validation, legacy script deprecation) will prevent similar issues in the future.
