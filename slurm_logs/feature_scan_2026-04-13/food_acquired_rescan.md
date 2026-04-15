# food_acquired — Rescan 2026-04-13

**Probe**: `Country(X).food_acquired()` via API  | **Countries probed**: 15  | **OK**: 13  | **Error**: 2  | **Total rows (ok)**: 5,128,139

> Prior audit: `SkunkWorks/audits/food_acquired.md` (2026-04-12). Changes noted inline.

## 1. Per-Country Status

| Country | Status | Rows | Index Names | Columns |
|---------|--------|-----:|:------------|:--------|
| Benin | ok | 187,672 | `i, t, v, visit, j, u` | Expenditure, Quantity, Produced |
| Burkina_Faso | ok | 331,165 | `i, t, v, visit, j, u` | quantity, units, total expenses, quantity obtained, units ob |
| CotedIvoire | ok | 292,741 | `i, t, v, visit, j, u` | Expenditure, Quantity, Produced |
| Ethiopia | ok | 264,418 | `i, t, v, j, units, units_purchased` | quantity, value_purchased, quantity_purchased, unitvalue, Kg |
| GhanaLSS | ok | 1,459,786 | `i, t, v, j` | purchased_value, purchased_value_yearly, produced_value_dail |
| Guinea-Bissau | ok | 125,484 | `i, t, v, visit, j, u` | Expenditure, Quantity, Produced |
| Malawi | **error** `CalledProcessError: Command '['make', '-s', '../2010-11/_/food_acquired.parquet'` | — | — | — |
| Mali | ok | 433,779 | `i, t, v, visit, j, u` | Expenditure, Quantity, Produced |
| Nepal | **error** `RuntimeError: Could not materialize Nepal/food_acquired: no wave-level build suc` | — | — | — |
| Niger | ok | 237,733 | `i, t, v, visit, j, u` | Expenditure, Quantity, Produced |
| Nigeria | ok | 646,185 | `i, t, v, j, u` | m, Quantity, Expenditure, Produced |
| Senegal | ok | 433,871 | `i, t, v, visit, j, u` | Expenditure, Quantity, Produced |
| Tanzania | ok | 250,302 | `i, t, v, j` | quant_ttl_consume, unit_ttl_consume, quant_purchase, unit_pu |
| Togo | ok | 112,087 | `i, t, v, visit, j, u` | Expenditure, Quantity, Produced |
| Uganda | ok | 352,916 | `i, t, v, j, u` | market, farmgate, value_home, value_away, value_own, value_i |

## 2. Index Consistency

**Warning**: 4 distinct index structures across countries.

- `('i', 't', 'v', 'visit', 'j', 'u')` — Benin, Burkina_Faso, CotedIvoire, Guinea-Bissau, Mali, Niger, Senegal, Togo
- `('i', 't', 'v', 'j')` — GhanaLSS, Tanzania
- `('i', 't', 'v', 'j', 'u')` — Nigeria, Uganda
- `('i', 't', 'v', 'j', 'units', 'units_purchased')` — Ethiopia

## 3. Canonical Column Checks

No canonical column violations in OK countries.

## 4. Extra (Non-Canonical) Columns

| Column | Countries |
|--------|-----------|
| `Expenditure` | Benin, Burkina_Faso, CotedIvoire, GhanaLSS, Guinea-Bissau, Mali, Niger, Nigeria, Senegal, Togo |
| `Quantity` | Benin, Burkina_Faso, CotedIvoire, GhanaLSS, Guinea-Bissau, Mali, Niger, Nigeria, Senegal, Togo |
| `Produced` | Benin, Burkina_Faso, CotedIvoire, GhanaLSS, Guinea-Bissau, Mali, Niger, Nigeria, Senegal, Togo |
| `quantity` | Burkina_Faso, Ethiopia |
| `Kgs` | Ethiopia, Uganda |
| `units` | Burkina_Faso |
| `total expenses` | Burkina_Faso |
| `quantity obtained` | Burkina_Faso |
| `units obtained` | Burkina_Faso |
| `price per unit` | Burkina_Faso |
| `value_purchased` | Ethiopia |
| `quantity_purchased` | Ethiopia |
| `unitvalue` | Ethiopia |
| `Kgs Purchased` | Ethiopia |
| `purchased_value` | GhanaLSS |
| `purchased_value_yearly` | GhanaLSS |
| `produced_value_daily` | GhanaLSS |
| `produced_value_yearly` | GhanaLSS |
| `h` | GhanaLSS |
| `u` | GhanaLSS |
| `visit` | GhanaLSS |
| `Purchased` | GhanaLSS |
| `Price` | GhanaLSS |
| `produced_price` | GhanaLSS |
| `produced_quantity` | GhanaLSS |
| `m` | Nigeria |
| `quant_ttl_consume` | Tanzania |
| `unit_ttl_consume` | Tanzania |
| `quant_purchase` | Tanzania |
| `unit_purchase` | Tanzania |
| `value_purchase` | Tanzania |
| `quant_own` | Tanzania |
| `unit_own` | Tanzania |
| `quant_inkind` | Tanzania |
| `unit_inkind` | Tanzania |
| `unitvalue_purchase` | Tanzania |
| `agg_u` | Tanzania |
| `market` | Uganda |
| `farmgate` | Uganda |
| `value_home` | Uganda |
| `value_away` | Uganda |
| `value_own` | Uganda |
| `value_inkind` | Uganda |
| `quantity_home` | Uganda |
| `quantity_away` | Uganda |
| `quantity_own` | Uganda |
| `quantity_inkind` | Uganda |
| `unitvalue_home` | Uganda |
| `unitvalue_away` | Uganda |
| `unitvalue_own` | Uganda |
| `unitvalue_inkind` | Uganda |
| `market_home` | Uganda |
| `market_away` | Uganda |
| `market_own` | Uganda |

## 5. Warnings

- (13×) `ResourceWarning: unclosed file <_io.TextIOWrapper name='/global/scratch/fsa/fc_jevons/ligon/mirrors/`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Benin`
- (2×) `UserWarning: Data scheme does not contain panel_ids for CotedIvoire`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Guinea-Bissau`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Togo`

## 6. Comparison with 2026-04-12 Audit

*Prior audit key excerpt (first 500 chars):*

```
# Food Acquired Feature Audit

**Execution**: 2026-04-12  
**Probe**: `ll.Feature('food_acquired')()`  
**Total rows**: 5,128,136 | 54 columns  
**Canonical index spec**: `(t, m, v, i, j, u)` | **Observed**: `(country, hh_id, t, v, i, j, u)` — **7 levels**

---

## 1. Scope Deviations

**Status**: Index structure mismatch detected.

The canonical schema specifies index `(t, m, v, i, j, u)` (6 levels) but the Feature output presents `(country, hh_id, t, v, i, j, u)` (7 levels). The `country` and 
```

*Rescan status*: See sections above. Where prior audit noted errors/violations,
check whether those countries still appear in the error or violation lists above.
