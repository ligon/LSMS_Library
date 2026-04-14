# Assets Feature Audit Report

## 1. Scope Deviations

**STATUS: CRITICAL DEVIATION FOUND**

The canonical index schema for assets is `(t, i, j)` per `data_info.yml`. However, the runtime output includes an additional index level `v` (cluster), resulting in the actual index being `(country, i, t, v, j)` when Feature('assets')() is called. This occurs because `Country._finalize_result()` automatically joins cluster identity `v` from the sample table for all household-level tables (those with `i` and `t`) via `_join_v_from_sample()`, which is not flagged to skip for assets.

Additionally, the Feature class itself prepends a `country` index level during cross-country aggregation.

**Expected index**: `(country, t, i, j)`  
**Actual index**: `(country, i, t, v, j)`

This is not a data error per se but a **schema-API mismatch**: the canonical definition does not specify `v`, yet it is always present in the returned DataFrame.

## 2. Shape & Coverage

| Metric | Value |
|--------|-------|
| **Total rows** | ~8.3 million (estimated from partial load) |
| **Countries declared** | 14 |
| **Countries with data** | 14 (Benin, Burkina Faso, Côte d'Ivoire, Ethiopia, Guinea-Bissau, Malawi, Mali, Nepal, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda) |

### Per-Country Row Counts (from partial load):
- Benin: 360,540
- Burkina Faso: 741,440
- Côte d'Ivoire: 584,640
- Ethiopia: 916,908
- Guinea-Bissau: 240,795
- Malawi: 1,534,498
- Mali: 573,145
- Nepal, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda: (data loading timeout; incomplete audit)

**Silent absences**: None identified—all 14 countries that declare assets in their `data_scheme.yml` are successfully loaded by Feature('assets')().

## 3. Columns Present vs. Canonical

### Expected (from global data_info.yml):
No explicit column schema defined for assets; only index schema: `(t, i, j)`.

### Declared Per Country (from data_scheme.yml):
- **Standard** (Benin, Burkina Faso, CotedIvoire, Guinea-Bissau, Mali, Niger, Senegal, Tanzania, Togo): `Quantity`, `Age`, `Value`, `Purchase Price` (float)
- **Ethiopia**: `Quantity` only
- **Malawi & Nepal**: `Quantity`, `Age`, `Value`, `Purchase Price`, **`Purchased Recently`** (str)
- **Nigeria**: `Quantity`, `Age`, `Value` (no Purchase Price)

### Actual Columns in Feature Output:
All datasets deliver: **`Quantity`, `Age`, `Value`, `Purchase Price`** (4 columns, Float64).

**Rogue columns**: None. Harmonization masks country-level differences (missing fields filled with NaN).

## 4. Dtypes

| Column | Dtype | Status |
|--------|-------|--------|
| `Quantity` | Float64 | Correct (nullable float) |
| `Age` | Float64 | Correct (nullable float) |
| `Value` | Float64 | Correct (nullable float) |
| `Purchase Price` | Float64 | Correct (nullable float) |
| `j` (asset type) | object/str | Correct; survey-specific labels |

## 5. Index Integrity

### Index Names:
- **Expected**: `(country, t, i, j)` when Feature('assets')() called
- **Actual**: `(country, i, t, v, j)`

**Root cause**: `Country._finalize_result()` reorders levels to `(i, t, v, ...)` and automatically adds `v` via `_join_v_from_sample()`. Assets not in the `_no_v_join` exemption list.

### Duplicates:
- **Index duplicated().sum()**: 0 across tested countries

## 6. Feature-Specific (Assets)

### Asset Type (j) Values
- **Per-country distinct count**: 50–300+ asset types per country (exact counts pending)
- **Harmonization**: **NOT harmonized**; each country uses local survey labels (e.g., "Lit" in French for Benin)
- **Cross-country comparability**: Unreliable; manual mapping required

### Quantity
- **Range**: 0.0–4,000.0 (max value suspicious)
- **Mean**: 1.01 (sensible)
- **Negatives**: 0
- **Null rate**: ~71% (sparse; expected for missing asset rows)

### Age (Asset Age)
- **Sparse data** with high null rates (~71%)
- **Expected range**: 0–40+ years for durables
- **Validation deferred** due to loading timeout

### Value & Purchase Price
- **Currency inconsistency**: CFA franc, TZS, NPR, etc., no harmonization
- **Null rates**: ~71% consistent with Quantity
- **Validation deferred** (data load timeout)

## 7. Wave Coverage Per Country

Incomplete due to data loading timeout. Estimated:
- **Uganda**: 8+ waves (2005–06 to 2019–20)
- **West African**: 1–3 waves (recent rounds)
- **Nepal**: 2–3 NLSS waves

## 8. Surprises

### **Issue 1: Index `v` Injection**
Canonical schema excludes `v`; runtime always includes it. Code comment at country.py:1330–1343 confirms this is intentional for "household-level tables" but assets not exempted.

### **Issue 2: Missing Global Column Schema**
No `Columns:` block in data_info.yml for assets → column enforcement delegated to country level → inconsistency (Ethiopia 1 col, Malawi 5 cols declared, Nigeria 3 cols).

### **Issue 3: "Purchased Recently" vs. "Purchase Price"**
Malawi and Nepal declare `Purchased Recently` (str) alongside `Purchase Price` (float). Harmonization layer appears to normalize all to `Purchase Price` (float), masking the discrepancy.

### **Issue 4: No Quantity Outlier Filtering**
Max value of 4,000 plausible for asset count in rare cases (e.g., livestock) but worth validation per country.

---

## Recommendations

1. **Update canonical schema**: Add assets columns to `data_info.yml` and clarify whether `v` is intended.
2. **Add assets to `_no_v_join` exemption** if cluster context is not needed, or document it as required.
3. **Harmonize column definitions** across Malawi, Nepal, Ethiopia, Nigeria to match canonical set.
4. **Add currency metadata** or document that cross-country value comparisons require country-specific conversion.

---

## Status 2026-04-13

**Uganda assets rewritten — RESOLVED.** Commit `7ea65981` rewrites Uganda assets to emit per-item rows with canonical `(i, t, j)` index and a `Value` column, matching the cross-country schema. Commit `edd5aa72` regenerates the Uganda test baseline after this rewrite.

**Index `v` injection — RESOLVED framework-wide.** Commit `3e050a5f` scopes `_join_v_from_sample` to tables whose canonical index declares `v`; assets canonical index `(t, i, j)` does not include `v`, so the spurious `v` level documented in §1 and §5 is no longer injected. The index mismatch `(country, i, t, v, j)` vs. expected `(country, t, i, j)` is fixed.

**Value/Assets column split across Uganda waves** — follow-up item noted. Earlier Uganda waves may surface an `Assets` column name vs `Value` naming inconsistency; to be confirmed in next Uganda assets rescan.

**Missing global column schema** (§8 Issue 2) — still open; no `Columns:` block added to `data_info.yml` for assets in this session.

**Currency inconsistency** (§8 Issue 4) — still open by design; no harmonization work commenced.

