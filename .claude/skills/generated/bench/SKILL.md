---
name: bench
description: "Skill for the Bench area of LSMS_Library. 5 symbols across 1 files."
---

# Bench

5 symbols | 1 files | Cohesion: 100%

## When to Use

- Working with code in `bench/`
- Understanding how time_step, df_summary, main work
- Modifying bench-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `bench/build_feature.py` | _profile_context, _emit, time_step, df_summary, main |

## Entry Points

Start here when exploring this area:

- **`time_step`** (Function) — `bench/build_feature.py:98`
- **`df_summary`** (Function) — `bench/build_feature.py:110`
- **`main`** (Function) — `bench/build_feature.py:133`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `time_step` | Function | `bench/build_feature.py` | 98 |
| `df_summary` | Function | `bench/build_feature.py` | 110 |
| `main` | Function | `bench/build_feature.py` | 133 |
| `_profile_context` | Function | `bench/build_feature.py` | 49 |
| `_emit` | Function | `bench/build_feature.py` | 93 |

## How to Explore

1. `gitnexus_context({name: "time_step"})` — see callers and callees
2. `gitnexus_query({query: "bench"})` — find related execution flows
3. Read key files listed above for implementation details
