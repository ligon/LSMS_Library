# housing — Rescan 2026-04-13

**Probe**: `Country(X).housing()` via API  | **Countries probed**: 13  | **OK**: 13  | **Error**: 0  | **Total rows (ok)**: 243,319

> Prior audit: `SkunkWorks/audits/housing.md` (2026-04-12). Changes noted inline.

## 1. Per-Country Status

| Country | Status | Rows | Index Names | Columns |
|---------|--------|-----:|:------------|:--------|
| Benin | ok | 8,012 | `i, t, v` | Roof, Floor |
| Burkina_Faso | ok | 21,037 | `i, t, v` | Roof, Floor |
| CotedIvoire | ok | 12,992 | `i, t, v` | Roof, Floor |
| Ethiopia | ok | 25,914 | `i, t, v` | Roof, Floor |
| Guinea-Bissau | ok | 5,351 | `i, t, v` | Roof, Floor |
| Malawi | ok | 57,058 | `i, t, v` | Roof, Floor |
| Mali | ok | 24,931 | `i, t, v` | Roof, Floor |
| Niger | ok | 3,968 | `i, t, v` | Roof, Floor |
| Nigeria | ok | 24,006 | `i, t, v` | Roof, Floor |
| Senegal | ok | 7,156 | `i, t, v` | Roof, Floor |
| Tanzania | ok | 22,433 | `i, t, v` | Roof, Floor |
| Togo | ok | 6,171 | `i, t, v` | Roof, Floor |
| Uganda | ok | 24,290 | `i, t, v` | Roof, Floor |

## 2. Index Consistency

All OK countries share a single index structure: `('i', 't', 'v')`

## 3. Canonical Column Checks

No canonical column violations in OK countries.

## 4. Extra (Non-Canonical) Columns

| Column | Countries |
|--------|-----------|
| `Roof` | Benin, Burkina_Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda |
| `Floor` | Benin, Burkina_Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda |

## 5. Warnings

- (13×) `ResourceWarning: unclosed file <_io.TextIOWrapper name='/global/scratch/fsa/fc_jevons/ligon/mirrors/`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Benin`
- (2×) `UserWarning: Data scheme does not contain panel_ids for CotedIvoire`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Guinea-Bissau`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Togo`

## 6. Comparison with 2026-04-12 Audit

*Prior audit key excerpt (first 500 chars):*

```
# Housing Feature Audit Report

## 1. SCOPE DEVIATIONS

None. The probe executed successfully with all 13 countries present in the feature aggregation. No schema mismatches or malformed indices observed.

## 2. Shape and Coverage

**Total rows:** 243,319 households across all waves and countries.

**Per-country row counts:**

| Country       | Row Count |
|---------------|-----------|
| Benin         | 8,012     |
| Burkina Faso  | 21,037    |
| Côte d'Ivoire | 12,992    |
| Ethiopia      | 25,9
```

*Rescan status*: See sections above. Where prior audit noted errors/violations,
check whether those countries still appear in the error or violation lists above.
