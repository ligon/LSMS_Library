# assets — Rescan 2026-04-13

**Probe**: `Country(X).assets()` via API  | **Countries probed**: 14  | **OK**: 13  | **Error**: 1  | **Total rows (ok)**: 8,016,942

> Prior audit: `SkunkWorks/audits/assets.md` (2026-04-12). Changes noted inline.

## 1. Per-Country Status

| Country | Status | Rows | Index Names | Columns |
|---------|--------|-----:|:------------|:--------|
| Benin | ok | 360,540 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Burkina_Faso | ok | 741,440 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| CotedIvoire | ok | 584,640 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Ethiopia | ok | 916,908 | `i, t, j` | Quantity |
| Guinea-Bissau | ok | 240,795 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Malawi | ok | 1,534,498 | `i, t, j` | Quantity, Age, Value, Purchased Recently, Purchase Price |
| Mali | ok | 573,145 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Nepal | **error** `PathMissingError: The path '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Lib` | — | — | — |
| Niger | ok | 824,619 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Nigeria | ok | 1,140,682 | `i, t, j` | Quantity, Age, Value |
| Senegal | ok | 642,420 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Tanzania | ok | 62,752 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Togo | ok | 277,695 | `i, t, j` | Quantity, Age, Value, Purchase Price |
| Uganda | ok | 116,808 | `i, t, j` | Value |

## 2. Index Consistency

All OK countries share a single index structure: `('i', 't', 'j')`

## 3. Canonical Column Checks

No canonical column violations in OK countries.

## 4. Extra (Non-Canonical) Columns

| Column | Countries |
|--------|-----------|
| `Quantity` | Benin, Burkina_Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo |
| `Value` | Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo, Uganda |
| `Age` | Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo |
| `Purchase Price` | Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Malawi, Mali, Niger, Senegal, Tanzania, Togo |
| `Purchased Recently` | Malawi |

## 5. Warnings

- (13×) `ResourceWarning: unclosed file <_io.TextIOWrapper name='/global/scratch/fsa/fc_jevons/ligon/mirrors/`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Benin`
- (1×) `UserWarning: Data scheme does not contain panel_ids for CotedIvoire`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Guinea-Bissau`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Togo`

## 6. Comparison with 2026-04-12 Audit

*Prior audit key excerpt (first 500 chars):*

```
# Assets Feature Audit Report

## 1. Scope Deviations

**STATUS: CRITICAL DEVIATION FOUND**

The canonical index schema for assets is `(t, i, j)` per `data_info.yml`. However, the runtime output includes an additional index level `v` (cluster), resulting in the actual index being `(country, i, t, v, j)` when Feature('assets')() is called. This occurs because `Country._finalize_result()` automatically joins cluster identity `v` from the sample table for all household-level tables (those with `i` 
```

*Rescan status*: See sections above. Where prior audit noted errors/violations,
check whether those countries still appear in the error or violation lists above.
