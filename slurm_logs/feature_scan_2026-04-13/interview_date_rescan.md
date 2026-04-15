# interview_date — Rescan 2026-04-13

**Probe**: `Country(X).interview_date()` via API  | **Countries probed**: 15  | **OK**: 11  | **Error**: 4  | **Total rows (ok)**: 176,822

> Prior audit: `SkunkWorks/audits/interview_date.md` (2026-04-12). Changes noted inline.

## 1. Per-Country Status

| Country | Status | Rows | Index Names | Columns |
|---------|--------|-----:|:------------|:--------|
| Benin | ok | 8,012 | `i, t, v` | int_t |
| Burkina_Faso | ok | 10,237 | `i, t, v` | int_t |
| CotedIvoire | ok | 12,992 | `i, t, v` | int_t |
| Ethiopia | ok | 8,351 | `i, t, v` | int_t |
| GhanaLSS | **error** `RuntimeError: Could not materialize GhanaLSS/interview_date: no wave-level build` | — | — | — |
| Guinea-Bissau | ok | 5,351 | `i, t, v` | int_t |
| Malawi | **error** `RuntimeError: Could not materialize Malawi/interview_date: no wave-level build s` | — | — | — |
| Mali | ok | 37,125 | `i, t, v, visit` | Int_t |
| Nepal | **error** `RuntimeError: Could not materialize Nepal/interview_date: no wave-level build su` | — | — | — |
| Niger | ok | 12,646 | `i, t, v` | int_t |
| Nigeria | ok | 39,228 | `i, t, v` | int_t |
| Senegal | ok | 14,276 | `i, t, v` | int_t |
| Tanzania | ok | 22,433 | `i, t, v` | date, int_t |
| Togo | ok | 6,171 | `i, t, v` | int_t |
| Uganda | **error** `TimeoutError: probe exceeded 120s (Makefile !make path hangs without DVC infrast` | — | — | — |

## 2. Index Consistency

**Warning**: 2 distinct index structures across countries.

- `('i', 't', 'v')` — Benin, Burkina_Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Niger, Nigeria, Senegal, Tanzania, Togo
- `('i', 't', 'v', 'visit')` — Mali

## 3. Canonical Column Checks

No canonical column violations in OK countries.

## 4. Extra (Non-Canonical) Columns

| Column | Countries |
|--------|-----------|
| `int_t` | Benin, Burkina_Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Niger, Nigeria, Senegal, Tanzania, Togo |
| `date` | Tanzania |

## 5. Warnings

- (11×) `ResourceWarning: unclosed file <_io.TextIOWrapper name='/global/scratch/fsa/fc_jevons/ligon/mirrors/`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Benin`
- (2×) `UserWarning: Data scheme does not contain panel_ids for CotedIvoire`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Guinea-Bissau`
- (2×) `UserWarning: Data scheme does not contain panel_ids for Togo`

## 6. Comparison with 2026-04-12 Audit

*Prior audit key excerpt (first 500 chars):*

```
# Audit: `interview_date` Feature

## 1. SCOPE DEVIATIONS

**Status**: MAJOR DEVIATIONS DETECTED.

The canonical schema (`data_info.yml`) specifies:
- **Index**: `(t, v, i)`
- **Column**: `Int_t` (title-case, type: `datetime`)

**Findings**:
- 10 of 11 countries have index `(t, i)` — **missing `v` (cluster) entirely**.
- Mali has index `(t, visit, i)` — non-standard `visit` level instead of `v`.
- **10 of 11 countries use lowercase `int_t`; only Mali uses canonical `Int_t`**.

---

## 2. SHAPE &
```

*Rescan status*: See sections above. Where prior audit noted errors/violations,
check whether those countries still appear in the error or violation lists above.
