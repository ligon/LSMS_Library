# shocks — Rescan 2026-04-13

**Probe**: `Country(X).shocks()` via API  | **Countries probed**: 13  | **OK**: 12  | **Error**: 1  | **Total rows (ok)**: 4,639,538

> Prior audit: `SkunkWorks/audits/shocks.md` (2026-04-12). Changes noted inline.

## 1. Per-Country Status

| Country | Status | Rows | Index Names | Columns |
|---------|--------|-----:|:------------|:--------|
| Benin | ok | 176,264 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Burkina_Faso | ok | 419,426 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| CotedIvoire | ok | 285,824 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Ethiopia | ok | 500,121 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Guinea-Bissau | ok | 117,722 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Malawi | ok | 1,090,918 | `i, t, Shock` | AffectedIncome, AffectedAssets, HowCoped0, HowCoped1, HowCop |
| Mali | ok | 375,481 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Niger | ok | 424,764 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Nigeria | ok | 636,256 | `i, t, Shock` | HowCoped0, HowCoped1, HowCoped2 |
| Senegal | ok | 314,072 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Tanzania | ok | 162,928 | `i, t, Shock` | AffectedIncome, AffectedAssets, HowCoped0, HowCoped1, HowCop |
| Togo | ok | 135,762 | `i, t, Shock` | AffectedIncome, AffectedAssets, AffectedProduction, Affected |
| Uganda | **error** `TimeoutError: probe exceeded 120s (Makefile !make path hangs without DVC infrast` | — | — | — |

## 2. Index Consistency

All OK countries share a single index structure: `('i', 't', 'Shock')`

## 3. Canonical Column Checks

### Missing Optional Columns (informational)

- `AffectedAssets` absent in: Nigeria
- `AffectedConsumption` absent in: Nigeria, Tanzania
- `AffectedIncome` absent in: Nigeria
- `AffectedProduction` absent in: Nigeria, Tanzania
- `HowCoped2` absent in: Benin, CotedIvoire

### High Null Rate (>50%) on Canonical Columns

- Benin / `AffectedIncome`: 0.0% non-null
- Benin / `AffectedAssets`: 0.0% non-null
- Benin / `AffectedProduction`: 0.0% non-null
- Benin / `AffectedConsumption`: 0.0% non-null
- Burkina_Faso / `AffectedIncome`: 0.0% non-null
- Burkina_Faso / `AffectedAssets`: 0.0% non-null
- Burkina_Faso / `AffectedProduction`: 0.0% non-null
- Burkina_Faso / `AffectedConsumption`: 0.0% non-null
- CotedIvoire / `AffectedIncome`: 0.0% non-null
- CotedIvoire / `AffectedAssets`: 0.0% non-null
- CotedIvoire / `AffectedProduction`: 0.0% non-null
- CotedIvoire / `AffectedConsumption`: 0.0% non-null
- Ethiopia / `AffectedIncome`: 0.0% non-null
- Ethiopia / `AffectedAssets`: 0.0% non-null
- Ethiopia / `AffectedProduction`: 0.0% non-null
- Ethiopia / `AffectedConsumption`: 0.0% non-null
- Guinea-Bissau / `AffectedIncome`: 0.0% non-null
- Guinea-Bissau / `AffectedAssets`: 0.0% non-null
- Guinea-Bissau / `AffectedProduction`: 0.0% non-null
- Guinea-Bissau / `AffectedConsumption`: 0.0% non-null
- Malawi / `AffectedIncome`: 0.0% non-null
- Malawi / `AffectedAssets`: 0.0% non-null
- Malawi / `AffectedProduction`: 0.0% non-null
- Malawi / `AffectedConsumption`: 0.0% non-null
- Mali / `AffectedIncome`: 0.0% non-null
- Mali / `AffectedAssets`: 0.0% non-null
- Mali / `AffectedProduction`: 0.0% non-null
- Mali / `AffectedConsumption`: 0.0% non-null
- Niger / `AffectedIncome`: 0.0% non-null
- Niger / `AffectedAssets`: 0.0% non-null
- Niger / `AffectedProduction`: 0.0% non-null
- Niger / `AffectedConsumption`: 0.0% non-null
- Senegal / `AffectedIncome`: 0.0% non-null
- Senegal / `AffectedAssets`: 0.0% non-null
- Senegal / `AffectedProduction`: 0.0% non-null
- Senegal / `AffectedConsumption`: 0.0% non-null
- Tanzania / `AffectedIncome`: 0.0% non-null
- Tanzania / `AffectedAssets`: 0.0% non-null
- Togo / `AffectedIncome`: 0.0% non-null
- Togo / `AffectedAssets`: 0.0% non-null
- Togo / `AffectedProduction`: 0.0% non-null
- Togo / `AffectedConsumption`: 0.0% non-null
- Togo / `HowCoped2`: 0.0% non-null
- Guinea-Bissau / `HowCoped2`: 0.0% non-null
- Tanzania / `HowCoped2`: 0.0% non-null
- Senegal / `HowCoped2`: 0.1% non-null
- Burkina_Faso / `HowCoped2`: 0.3% non-null
- Togo / `HowCoped1`: 0.4% non-null
- Senegal / `HowCoped1`: 0.5% non-null
- Nigeria / `HowCoped2`: 0.5% non-null
- CotedIvoire / `HowCoped1`: 0.8% non-null
- Ethiopia / `HowCoped2`: 0.8% non-null
- Niger / `HowCoped2`: 0.9% non-null
- Tanzania / `HowCoped1`: 1.0% non-null
- Nigeria / `HowCoped1`: 1.2% non-null
- Niger / `HowCoped1`: 1.4% non-null
- Benin / `HowCoped1`: 1.4% non-null
- Guinea-Bissau / `HowCoped1`: 1.4% non-null
- Burkina_Faso / `HowCoped1`: 1.7% non-null
- Niger / `HowCoped0`: 1.9% non-null
- Ethiopia / `HowCoped1`: 1.9% non-null
- Malawi / `HowCoped2`: 2.9% non-null
- Mali / `HowCoped2`: 3.0% non-null
- Senegal / `HowCoped0`: 3.3% non-null
- Nigeria / `HowCoped0`: 3.5% non-null
- Mali / `HowCoped1`: 3.9% non-null
- Malawi / `HowCoped1`: 4.1% non-null
- Ethiopia / `HowCoped0`: 4.3% non-null
- CotedIvoire / `HowCoped0`: 4.8% non-null
- Togo / `HowCoped0`: 5.3% non-null
- Burkina_Faso / `HowCoped0`: 5.9% non-null
- Benin / `HowCoped0`: 7.0% non-null
- Mali / `HowCoped0`: 7.1% non-null
- Guinea-Bissau / `HowCoped0`: 8.1% non-null
- Malawi / `HowCoped0`: 9.6% non-null
- Tanzania / `HowCoped0`: 18.7% non-null

## 4. Extra (Non-Canonical) Columns

| Column | Countries |
|--------|-----------|
| `Cope1` | Niger |
| `Cope2` | Niger |
| `Cope3` | Niger |
| `Cope4` | Niger |
| `Cope5` | Niger |
| `Cope6` | Niger |
| `Cope7` | Niger |
| `Cope8` | Niger |
| `Cope9` | Niger |
| `Cope10` | Niger |
| `Cope11` | Niger |
| `Cope12` | Niger |
| `Cope13` | Niger |
| `Cope14` | Niger |
| `Cope15` | Niger |
| `Cope16` | Niger |
| `Cope17` | Niger |
| `Cope18` | Niger |
| `Cope19` | Niger |
| `Cope20` | Niger |
| `Cope21` | Niger |
| `Cope22` | Niger |
| `Cope23` | Niger |
| `Cope24` | Niger |
| `Cope25` | Niger |
| `Cope26` | Niger |

## 5. Warnings

- (12×) `ResourceWarning: unclosed file <_io.TextIOWrapper name='/global/scratch/fsa/fc_jevons/ligon/mirrors/`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Benin`
- (1×) `UserWarning: Data scheme does not contain panel_ids for CotedIvoire`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Guinea-Bissau`
- (1×) `UserWarning: Data scheme does not contain panel_ids for Togo`

## 6. Comparison with 2026-04-12 Audit

*Prior audit key excerpt (first 500 chars):*

```
# Shocks Feature Audit

**Date**: 2026-04-13  
**Probe**: `ll.Feature('shocks')()`  
**Total rows**: 4,639,538  
**Countries**: 12  
**Index structure**: (country, i, t, v, Shock)

---

## 1. Scope Deviations

**Assessment**: None detected.

All 12 countries declaring shocks in their data_scheme (Benin, Burkina Faso, Côte d'Ivoire, Ethiopia, Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo) contributed data successfully. No countries silently absent. Index integrity is perfec
```

*Rescan status*: See sections above. Where prior audit noted errors/violations,
check whether those countries still appear in the error or violation lists above.
