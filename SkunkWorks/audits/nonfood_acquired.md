# Audit Report: `nonfood_acquired` Feature

**Date**: 2026-04-12  
**Probe**: `ll.Feature('nonfood_acquired')()`  
**Result**: Empty DataFrame (0 rows, 0 columns)

## 1. SCOPE DEVIATIONS

**Status**: CRITICAL DEVIATION — Feature is declared but unimplemented across all countries.

The `nonfood_acquired` table is defined in the canonical schema (`lsms_library/data_info.yml`, line 20) with canonical index `(t, v, i, j)`, matching the intended structure described in prior work (#161–163). However:

- **Zero countries declare it**: No country's `data_scheme.yml` includes `nonfood_acquired`.
- **Zero implementations exist**: No `.py` script implements `nonfood_acquired()` anywhere in the codebase.
- **Canonical columns undefined**: The global `data_info.yml` lists the index structure but provides no `Columns:` section for `nonfood_acquired` (unlike `household_roster`, `cluster_features`, etc.), meaning per-country column harmonization rules are absent.
- **No required column list**: `Feature('nonfood_acquired').columns` returns `[]`.

This is a placeholder schema awaiting implementation. Per the HANDOFF document (2026-04-01), this feature is listed under "Hard" priority items with the note: "*nonfood_acquired* — no reference impl exists, needs schema design."

## 2. Shape & Coverage

| Metric | Value |
|--------|-------|
| **Total rows** | 0 |
| **Total columns** | 0 |
| **Countries with data** | 0 |
| **Countries in codebase** | 0 |

No data returned. The probe `ll.Feature('nonfood_acquired')()` yields an empty DataFrame due to `Feature.countries` being an empty list.

## 3. Columns Present vs. Canonical

**Present**: None (empty DataFrame).

**Expected by canonical schema**:
- Index levels: `(t, v, i, j)` — i.e., wave, cluster, household, expenditure item.
- Note: Index should NOT include `u` (unit) or `visit` as per the spec.

**Missing**: All columns (none defined). Since no country implements `nonfood_acquired`, there are no value columns to audit. The schema is incomplete; canonical column definitions (value column name, type, required status, spelling mappings) must be added to `Columns:` in `data_info.yml` before countries can declare this table.

## 4. Dtypes

N/A — no data to type-check.

## 5. Index Integrity

**Index structure**: The probe result is an empty DataFrame with a single unnamed integer index (not a MultiIndex).

```python
foo = ll.Feature('nonfood_acquired')()
foo.index.names  # [None]
foo.index.nlevels  # 1
foo.index.duplicated().sum()  # 0
```

No cross-country index can be constructed. When countries eventually declare this feature, the runtime will prepend a `country` level, yielding a canonical MultiIndex: `['country', 't', 'v', 'i', 'j']`.

## 6. Feature-Specific: `nonfood_acquired` (Not Implemented)

### Item vocabulary (`j`)

- **Status**: Not applicable. No country has provided data.
- **Expected behavior**: Per the schema, `j` should enumerate non-food expenditure items (e.g., soap, fuel, clothes, household goods). 
- **Harmonization**: Unknown. The canonical schema provides no mappings. Food-acquired uses `harmonized_food_labels()` from `uganda.py`; there is an analogous `harmonized_nonfood_items()` used in Uganda's `nonfood_expenditures()` function (loading from `../../_/nonfood_items.org`), but it is not yet integrated into a cross-country `nonfood_acquired` feature.

### Value columns

- **Status**: Not yet specified. 
- **Likely candidates** (based on `food_acquired` structure): single column such as `Expenditure` or `Value`, or possibly separate columns for different sources (e.g., purchased, produced, given, away) analogous to `food_acquired`'s `value_home`, `value_away`, `value_own`, `value_inkind`.
- **Data quality checks deferred**: Cannot assess negatives, zeros, outliers, or stringification artifacts without data.

### Cross-country signature

- **Food_acquired countries** (15): Benin, Burkina_Faso, CotedIvoire, Ethiopia, GhanaLSS, Guinea-Bissau, Malawi, Mali, Nepal, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda.
- **Nonfood_acquired countries**: None.
- **Related: nonfood_expenditures** (2): Uganda, Nigeria — but in a different format (wide matrix of items × households).

There is a conceptual mismatch: Uganda and Nigeria have `nonfood_expenditures` (in wide item-as-column format), but no country has `nonfood_acquired` (long format with `j` as index level). Migration from the wide `nonfood_expenditures` schema to a long `nonfood_acquired` schema (parallel to `food_acquired`) is needed.

## 7. Wave Coverage

**N/A**: No countries declare this feature; thus no wave coverage to validate against `Country(name).waves()`.

## 8. Surprises & Key Findings

1. **Schema incompleteness**: `nonfood_acquired` is sketched in the index info but lacks:
   - Column definitions in `data_info.yml`.
   - Any `!make` declarations in country `data_scheme.yml` files.
   - Reference implementations (`.py` scripts) in any country folder.

2. **Architectural divergence**: Uganda and Nigeria have `nonfood_expenditures` implemented, but it returns a wide matrix (items as columns), not the long-form `nonfood_acquired` structure specified in the canonical schema. Bridging this gap requires:
   - Deciding on the canonical value column(s) for `nonfood_acquired`.
   - Possibly refactoring Uganda's `nonfood_expenditures()` to emit the long form.
   - Or providing a separate `nonfood_acquired.py` that calls `nonfood_expenditures()` and reshapes.

3. **Harmonization unknown**: The `nonfood_items.org` reference file exists in Uganda's directory, but there is no global mapping analogous to `categorical_mapping/harmonize_food` for food items. It is unclear whether nonfood items are harmonized across countries or country-specific.

4. **Priority status**: Per the HANDOFF document, `nonfood_acquired` is marked "Hard" and explicitly noted as lacking a reference implementation and needing schema design. This aligns with the empty probe result.

## Recommendations (Out of Audit Scope)

- Define canonical column(s) in `data_info.yml:Columns:nonfood_acquired:`.
- Create a reference implementation in one country (e.g., Uganda).
- Establish a nonfood item harmonization mapping (possibly in `categorical_mapping/`).
- Test cross-country aggregation via `ll.Feature('nonfood_acquired')()`.
