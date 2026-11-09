---
name: dvc-lock-repro
description: "Skill for the Dvc_lock_repro area of LSMS_Library. 10 symbols across 2 files."
---

# Dvc_lock_repro

10 symbols | 2 files | Cohesion: 100%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how run_cold_cold, run_cold_warm_process, run_lock_only work
- Modifying dvc_lock_repro-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/dvc_lock_repro/bench_fetch.py` | _md5_for_target, _file_size_bytes, _clear_blob, _emit, run_cold_cold (+3) |
| `slurm_logs/dvc_lock_repro/repro_micro.py` | clear_cache_for_targets, main |

## Entry Points

Start here when exploring this area:

- **`run_cold_cold`** (Function) — `slurm_logs/dvc_lock_repro/bench_fetch.py:93`
- **`run_cold_warm_process`** (Function) — `slurm_logs/dvc_lock_repro/bench_fetch.py:120`
- **`run_lock_only`** (Function) — `slurm_logs/dvc_lock_repro/bench_fetch.py:164`
- **`main`** (Function) — `slurm_logs/dvc_lock_repro/bench_fetch.py:198`
- **`clear_cache_for_targets`** (Function) — `slurm_logs/dvc_lock_repro/repro_micro.py:71`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `run_cold_cold` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 93 |
| `run_cold_warm_process` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 120 |
| `run_lock_only` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 164 |
| `main` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 198 |
| `clear_cache_for_targets` | Function | `slurm_logs/dvc_lock_repro/repro_micro.py` | 71 |
| `main` | Function | `slurm_logs/dvc_lock_repro/repro_micro.py` | 113 |
| `_md5_for_target` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 47 |
| `_file_size_bytes` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 56 |
| `_clear_blob` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 78 |
| `_emit` | Function | `slurm_logs/dvc_lock_repro/bench_fetch.py` | 87 |

## How to Explore

1. `context({name: "run_cold_cold"})` — see callers and callees
2. `query({search_query: "dvc_lock_repro"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
