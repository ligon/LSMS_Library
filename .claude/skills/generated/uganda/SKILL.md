---
name: uganda
description: "Skill for the Uganda area of LSMS_Library. 11 symbols across 1 files."
---

# Uganda

11 symbols | 1 files | Cohesion: 71%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how org_table, aggregate_label_map, build_sources work
- Modifying uganda-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/price_sources/Uganda/analysis.py` | _clean_positive, org_table, _pairs, aggregate_label_map, _code_map (+6) |

## Entry Points

Start here when exploring this area:

- **`org_table`** (Function) — `slurm_logs/price_sources/Uganda/analysis.py:129`
- **`aggregate_label_map`** (Function) — `slurm_logs/price_sources/Uganda/analysis.py:155`
- **`build_sources`** (Function) — `slurm_logs/price_sources/Uganda/analysis.py:176`
- **`source4_sold_unit`** (Function) — `slurm_logs/price_sources/Uganda/analysis.py:288`
- **`within_household_margin`** (Function) — `slurm_logs/price_sources/Uganda/analysis.py:415`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `org_table` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 129 |
| `aggregate_label_map` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 155 |
| `build_sources` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 176 |
| `source4_sold_unit` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 288 |
| `within_household_margin` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 415 |
| `main` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 512 |
| `unknown_unit_cost` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 673 |
| `_clean_positive` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 104 |
| `_pairs` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 143 |
| `_code_map` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 160 |
| `_prep` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 194 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → _fwl_slope` | cross_community | 5 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Price_sources | 7 calls |
| Tests | 4 calls |
| _ | 3 calls |
| Tanzania | 1 calls |

## How to Explore

1. `context({name: "org_table"})` — see callers and callees
2. `query({search_query: "uganda"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
