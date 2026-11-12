---
name: ghanasps
description: "Skill for the Ghanasps area of LSMS_Library. 15 symbols across 6 files."
---

# Ghanasps

15 symbols | 6 files | Cohesion: 57%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how null_read_reports, main, main work
- Modifying ghanasps-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/ghanasps/plot_identity_permutation.py` | _load, prep, agree, null_draws, main |
| `slurm_logs/ghanasps/livestock_verify.py` | main, raw_w1, raw_w2, raw_w3 |
| `lsms_library/null_read_audit.py` | null_read_reports, _clear_null_read_reports |
| `slurm_logs/ghanasps/plot_labor_verify.py` | main, indirect |
| `slurm_logs/ghanasps/plot_features_verify.py` | main |
| `tests/test_null_read_guard.py` | test_niger_2014_15_latitude_is_reported |

## Entry Points

Start here when exploring this area:

- **`null_read_reports`** (Function) — `lsms_library/null_read_audit.py:390`
- **`main`** (Function) — `slurm_logs/ghanasps/plot_features_verify.py:20`
- **`main`** (Function) — `slurm_logs/ghanasps/plot_labor_verify.py:34`
- **`indirect`** (Function) — `slurm_logs/ghanasps/plot_labor_verify.py:167`
- **`test_niger_2014_15_latitude_is_reported`** (Function) — `tests/test_null_read_guard.py:387`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `null_read_reports` | Function | `lsms_library/null_read_audit.py` | 390 |
| `main` | Function | `slurm_logs/ghanasps/plot_features_verify.py` | 20 |
| `main` | Function | `slurm_logs/ghanasps/plot_labor_verify.py` | 34 |
| `indirect` | Function | `slurm_logs/ghanasps/plot_labor_verify.py` | 167 |
| `test_niger_2014_15_latitude_is_reported` | Function | `tests/test_null_read_guard.py` | 387 |
| `prep` | Function | `slurm_logs/ghanasps/plot_identity_permutation.py` | 40 |
| `agree` | Function | `slurm_logs/ghanasps/plot_identity_permutation.py` | 58 |
| `null_draws` | Function | `slurm_logs/ghanasps/plot_identity_permutation.py` | 76 |
| `main` | Function | `slurm_logs/ghanasps/plot_identity_permutation.py` | 89 |
| `main` | Function | `slurm_logs/ghanasps/livestock_verify.py` | 33 |
| `raw_w1` | Function | `slurm_logs/ghanasps/livestock_verify.py` | 91 |
| `raw_w2` | Function | `slurm_logs/ghanasps/livestock_verify.py` | 104 |
| `raw_w3` | Function | `slurm_logs/ghanasps/livestock_verify.py` | 111 |
| `_clear_null_read_reports` | Function | `lsms_library/null_read_audit.py` | 478 |
| `_load` | Function | `slurm_logs/ghanasps/plot_identity_permutation.py` | 35 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 7 calls |
| _ | 6 calls |
| Lsms_library | 4 calls |
| Integration_2026-05-07 | 2 calls |

## How to Explore

1. `context({name: "null_read_reports"})` — see callers and callees
2. `query({search_query: "ghanasps"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
