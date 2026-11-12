---
name: final-cache-validation
description: "Skill for the Final-cache-validation area of LSMS_Library. 41 symbols across 10 files."
---

# Final-cache-validation

41 symbols | 10 files | Cohesion: 83%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how fixture, main, sha work
- Modifying final-cache-validation-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | sha, fsync_dir, mkdir_durable, save, journal (+15) |
| `slurm_logs/release_landing_2026-10-14/final-cache-validation/run_final.py` | dump, frame_summary, parquet_info, snapshot_country, worker (+4) |
| `slurm_logs/release_landing_2026-10-14/final-cache-validation/prepare_copy.py` | sha, verify_original, main |
| `slurm_logs/release_landing_2026-10-14/final-cache-validation/check_promotion_transaction.py` | fixture, main |
| `slurm_logs/release_landing_2026-10-14/final-cache-validation/check_final_hashes.py` | verify_copy, main |
| `lsms_library/diagnostics.py` | load_feature |
| `slurm_logs/2026-06-07_foodsec_nonfies/verify_nonfies.py` | verify |
| `slurm_logs/2026-06-13_foodsec_verify/verify_foodsec.py` | verify |
| `slurm_logs/release_landing_2026-10-14/final-cache-validation/trailing_uganda.py` | main |
| `slurm_logs/release_landing_2026-10-14/final-cache-validation/build_final_manifest.py` | main |

## Entry Points

Start here when exploring this area:

- **`fixture`** (Function) — `slurm_logs/release_landing_2026-10-14/final-cache-validation/check_promotion_transaction.py:10`
- **`main`** (Function) — `slurm_logs/release_landing_2026-10-14/final-cache-validation/check_promotion_transaction.py:55`
- **`sha`** (Function) — `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py:21`
- **`fsync_dir`** (Function) — `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py:29`
- **`mkdir_durable`** (Function) — `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py:37`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `fixture` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/check_promotion_transaction.py` | 10 |
| `main` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/check_promotion_transaction.py` | 55 |
| `sha` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 21 |
| `fsync_dir` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 29 |
| `mkdir_durable` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 37 |
| `save` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 49 |
| `journal` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 61 |
| `relative` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 68 |
| `safe_path` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 79 |
| `info` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 90 |
| `same_bytes` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 103 |
| `inventory` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 109 |
| `load_manifest` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 127 |
| `snapshot` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 155 |
| `copy_verified` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 163 |
| `stage` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 174 |
| `transaction_lock` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 200 |
| `check_transaction` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 206 |
| `verify_final` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 216 |
| `apply` | Function | `slurm_logs/release_landing_2026-10-14/final-cache-validation/promote_cache.py` | 230 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → Sha` | intra_community | 5 |
| `Main → Sha` | intra_community | 5 |
| `Main → Relative` | intra_community | 4 |
| `Main → Countries_root` | cross_community | 4 |
| `Main → Read_parquet_cache_hash` | cross_community | 4 |
| `Main → Relative` | intra_community | 4 |
| `Main → Read_parquet_cache_hash` | cross_community | 4 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 4 calls |
| Lsms_library | 2 calls |

## How to Explore

1. `context({name: "fixture"})` — see callers and callees
2. `query({search_query: "final-cache-validation"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
