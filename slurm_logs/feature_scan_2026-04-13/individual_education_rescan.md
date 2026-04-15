# individual_education — Rescan 2026-04-13

**Probe**: `Country(X).individual_education()` via API  | **Countries probed**: 11  | **OK**: 9  | **Error**: 2  | **Total rows (ok)**: 841,044

> Prior audit: `SkunkWorks/audits/individual_education.md` (2026-04-12). Changes noted inline.

## 1. Per-Country Status

| Country | Status | Rows | Index Names | Columns |
|---------|--------|-----:|:------------|:--------|
| Benin | ok | 42,343 | `i, t, v, pid` | Educational Attainment |
| Burkina_Faso | **error** `RuntimeError: Could not materialize Burkina_Faso/individual_education: no wave-l` | — | — | — |
| CotedIvoire | ok | 61,116 | `i, t, v, pid` | Educational Attainment |
| Guinea-Bissau | ok | 42,839 | `i, t, v, pid` | Educational Attainment |
| Malawi | ok | 260,255 | `i, t, v, pid` | Educational Attainment |
| Mali | ok | 188,706 | `i, t, v, pid` | Educational Attainment |
| Nepal | **error** `RuntimeError: Could not materialize Nepal/individual_education: no wave-level bu` | — | — | — |
| Niger | ok | 74,159 | `i, t, v, pid` | Educational Attainment |
| Senegal | ok | 129,650 | `i, t, v, pid` | Educational Attainment |
| Togo | ok | 27,482 | `i, t, v, pid` | Educational Attainment |
| Uganda | ok | 14,494 | `i, t, v, pid` | Educational Attainment |

## 2. Index Consistency

All OK countries share a single index structure: `('i', 't', 'v', 'pid')`

## 3. Canonical Column Checks

### High Null Rate (>50%) on Canonical Columns

- Uganda / `Educational Attainment`: 44.0% non-null
- Malawi / `Educational Attainment`: 49.5% non-null

## 4. Extra (Non-Canonical) Columns

No extra columns found.

## 5. Warnings

- (9×) `ResourceWarning: unclosed file <_io.TextIOWrapper name='/global/scratch/fsa/fc_jevons/ligon/mirrors/`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Benin`
- (2×) `UserWarning: Data scheme does not contain panel_ids for CotedIvoire`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Guinea-Bissau`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Togo`

## 6. Comparison with 2026-04-12 Audit

*Prior audit key excerpt (first 500 chars):*

```
# Audit Report: individual_education Feature

**Generated**: 2026-04-12  
**Probe**: `ll.Feature('individual_education')()`  
**Total Rows**: 841,044  
**Rows per Index**: 1 (unique MultiIndex)

---

## 1. Scope Deviations

**Status: NONE**

The feature assembled cleanly across all registered countries declaring `individual_education`. Three countries failed to materialize data due to missing DVC-tracked build artifacts (Burkina_Faso, Nepal) and two warned about missing panel_ids (Benin, CotedIv
```

*Rescan status*: See sections above. Where prior audit noted errors/violations,
check whether those countries still appear in the error or violation lists above.
