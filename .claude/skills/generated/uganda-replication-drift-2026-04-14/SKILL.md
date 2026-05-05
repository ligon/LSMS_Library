---
name: uganda-replication-drift-2026-04-14
description: "Skill for the Uganda_replication_drift_2026-04-14 area of LSMS_Library. 47 symbols across 8 files."
---

# Uganda_replication_drift_2026-04-14

47 symbols | 8 files | Cohesion: 89%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how content_hash, fingerprint, call_with_timeout work
- Modifying uganda_replication_drift_2026-04-14-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | _call_api, _merged, diagnose_household_roster, diagnose_food_prices, diagnose_nutrition (+5) |
| `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | content_hash, fingerprint, call_with_timeout, row_compare, fe_verdict (+3) |
| `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | content_hash, fingerprint, call_with_timeout, compare_dfs, functional_equivalence_check (+3) |
| `slurm_logs/uganda_replication_drift_2026-04-14/compare.py` | content_hash, fingerprint, call_with_timeout, compare_dfs, main |
| `slurm_logs/uganda_replication_drift_2026-04-14/fe_checks.py` | content_hash, row_compare, verdict_from, main |
| `slurm_logs/uganda_replication_drift_2026-04-14/eyeball_hsize.py` | _call_api, _load_repl, hr, main |
| `slurm_logs/uganda_replication_drift_2026-04-14/diagnose_hsize2.py` | _call_api, _load_repl, banner, main |
| `slurm_logs/uganda_replication_drift_2026-04-14/diagnose_hsize.py` | _call_api, _load_repl, banner, main |

## Entry Points

Start here when exploring this area:

- **`content_hash`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py:67`
- **`fingerprint`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py:75`
- **`call_with_timeout`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py:85`
- **`row_compare`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py:100`
- **`fe_verdict`** (Function) — `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py:148`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `content_hash` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 67 |
| `fingerprint` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 75 |
| `call_with_timeout` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 85 |
| `row_compare` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 100 |
| `fe_verdict` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 148 |
| `functional_equivalence` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 157 |
| `compare_dfs` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 424 |
| `main` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py` | 494 |
| `content_hash` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 62 |
| `fingerprint` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 70 |
| `call_with_timeout` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 80 |
| `compare_dfs` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 95 |
| `functional_equivalence_check` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 176 |
| `attempt_compare` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 195 |
| `agg_to_it` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 383 |
| `main` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/compare_enhanced.py` | 454 |
| `diagnose_household_roster` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 73 |
| `diagnose_food_prices` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 158 |
| `diagnose_nutrition` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 197 |
| `banner` | Function | `slurm_logs/uganda_replication_drift_2026-04-14/diagnose.py` | 65 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → Content_hash` | intra_community | 5 |
| `Main → Content_hash` | intra_community | 4 |
| `Main → Content_hash` | intra_community | 4 |
| `Main → Row_compare` | intra_community | 4 |
| `Main → Fe_verdict` | intra_community | 4 |
| `Main → Agg_to_it` | intra_community | 3 |

## How to Explore

1. `gitnexus_context({name: "content_hash"})` — see callers and callees
2. `gitnexus_query({query: "uganda_replication_drift_2026-04-14"})` — find related execution flows
3. Read key files listed above for implementation details
