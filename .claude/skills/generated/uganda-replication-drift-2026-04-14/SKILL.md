---
name: uganda-replication-drift-2026-04-14
description: "Skill for the Uganda_replication_drift_2026-04-14 area of LSMS_Library. 26 symbols across 4 files."
---

# Uganda_replication_drift_2026-04-14

26 symbols | 4 files | Cohesion: 81%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how content_hash, fingerprint, compare_dfs work
- Modifying uganda_replication_drift_2026-04-14-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | _call_api, _merged, diagnose_household_roster, diagnose_food_prices, diagnose_nutrition (+5) |
| `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | content_hash, fingerprint, compare_dfs, functional_equivalence_check, attempt_compare (+1) |
| `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | content_hash, fingerprint, row_compare, functional_equivalence, compare_dfs (+1) |
| `slurm_logs/uganda_replication_drift_2026-04-14/compare.py` | content_hash, fingerprint, compare_dfs, main |

## Entry Points

Start here when exploring this area:

- **`content_hash`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py:62`
- **`fingerprint`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py:70`
- **`compare_dfs`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py:95`
- **`functional_equivalence_check`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py:176`
- **`attempt_compare`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py:195`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `content_hash` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 62 |
| `fingerprint` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 70 |
| `compare_dfs` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 95 |
| `functional_equivalence_check` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 176 |
| `attempt_compare` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 195 |
| `main` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 454 |
| `content_hash` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 67 |
| `fingerprint` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 75 |
| `row_compare` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 100 |
| `functional_equivalence` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 157 |
| `compare_dfs` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 424 |
| `main` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 494 |
| `diagnose_household_roster` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 73 |
| `diagnose_food_prices` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 158 |
| `diagnose_nutrition` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 197 |
| `banner` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 65 |
| `diagnose_household_characteristics` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 107 |
| `diagnose_shocks` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 126 |
| `diagnose_food_quantities` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 178 |
| `content_hash` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare.py` | 60 |

## How to Explore

1. `context({name: "content_hash"})` — see callers and callees
2. `query({search_query: "uganda_replication_drift_2026-04-14"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
