---
name: gh850-design
description: "Skill for the Gh850_design area of LSMS_Library. 20 symbols across 6 files."
---

# Gh850_design

20 symbols | 6 files | Cohesion: 76%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how main, load, prep work
- Modifying gh850_design-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/gh850_design/measure.py` | load, prep, infer_old, infer_b, infer_c (+6) |
| `slurm_logs/gh850_design/measure_impl.py` | load, measure, measure_crop, main |
| `slurm_logs/gh850_design/measure_d5.py` | measure, main |
| `slurm_logs/gh850_design/mali_groundtruth.py` | main |
| `slurm_logs/gh850_design/metric_scoreboard.py` | main |
| `slurm_logs/gh850_design/two_more.py` | t_spread |

## Entry Points

Start here when exploring this area:

- **`main`** (Function) — `slurm_logs/gh850_design/mali_groundtruth.py:28`
- **`load`** (Function) — `slurm_logs/gh850_design/measure.py:70`
- **`prep`** (Function) — `slurm_logs/gh850_design/measure.py:84`
- **`infer_old`** (Function) — `slurm_logs/gh850_design/measure.py:129`
- **`infer_b`** (Function) — `slurm_logs/gh850_design/measure.py:140`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `main` | Function | `slurm_logs/gh850_design/mali_groundtruth.py` | 28 |
| `load` | Function | `slurm_logs/gh850_design/measure.py` | 70 |
| `prep` | Function | `slurm_logs/gh850_design/measure.py` | 84 |
| `infer_old` | Function | `slurm_logs/gh850_design/measure.py` | 129 |
| `infer_b` | Function | `slurm_logs/gh850_design/measure.py` | 140 |
| `infer_c` | Function | `slurm_logs/gh850_design/measure.py` | 151 |
| `measure` | Function | `slurm_logs/gh850_design/measure.py` | 231 |
| `main` | Function | `slurm_logs/gh850_design/measure.py` | 381 |
| `main` | Function | `slurm_logs/gh850_design/metric_scoreboard.py` | 61 |
| `t_spread` | Function | `slurm_logs/gh850_design/two_more.py` | 28 |
| `load` | Function | `slurm_logs/gh850_design/measure_impl.py` | 46 |
| `measure` | Function | `slurm_logs/gh850_design/measure_impl.py` | 61 |
| `measure_crop` | Function | `slurm_logs/gh850_design/measure_impl.py` | 113 |
| `main` | Function | `slurm_logs/gh850_design/measure_impl.py` | 134 |
| `seeded_factors` | Function | `slurm_logs/gh850_design/measure.py` | 99 |
| `measure` | Function | `slurm_logs/gh850_design/measure_d5.py` | 78 |
| `main` | Function | `slurm_logs/gh850_design/measure_d5.py` | 159 |
| `quantities_kgs` | Function | `slurm_logs/gh850_design/measure.py` | 204 |
| `prices_kgvalue` | Function | `slurm_logs/gh850_design/measure.py` | 215 |
| `_kg` | Function | `slurm_logs/gh850_design/measure.py` | 199 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Measure → _metric_scalar` | cross_community | 6 |
| `Measure → _is_currency_denominated` | cross_community | 5 |
| `Measure → _as_float` | cross_community | 5 |
| `Measure → _level_series` | cross_community | 4 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 7 calls |

## How to Explore

1. `context({name: "main"})` — see callers and callees
2. `query({search_query: "gh850_design"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
