---
name: price-sources
description: "Skill for the Price_sources area of LSMS_Library. 26 symbols across 8 files."
---

# Price_sources

26 symbols | 8 files | Cohesion: 73%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how build_sources, main, to_kg work
- Modifying price_sources-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/price_sources/common.py` | geo_ladder, cell_medians, match_gaps, model_row, model_rows (+2) |
| `slurm_logs/price_sources/synthesis.py` | describe, build_models, add, market_identity_table, pooled_fit (+1) |
| `slurm_logs/price_sources/Tanzania/analysis.py` | _import_tanzania, _drop_null_geo_cells, cells_for, run |
| `slurm_logs/price_sources/Ethiopia/analysis.py` | _kg_map, build_sources, main |
| `slurm_logs/price_sources/Malawi/analysis.py` | to_kg, run |
| `slurm_logs/price_sources/Nigeria/analysis.py` | safe_model_rows, main |
| `slurm_logs/price_sources/Mali/analysis.py` | main |
| `slurm_logs/price_sources/Niger/analysis.py` | main |

## Entry Points

Start here when exploring this area:

- **`build_sources`** (Function) — `slurm_logs/price_sources/Ethiopia/analysis.py:203`
- **`main`** (Function) — `slurm_logs/price_sources/Ethiopia/analysis.py:276`
- **`to_kg`** (Function) — `slurm_logs/price_sources/Malawi/analysis.py:132`
- **`run`** (Function) — `slurm_logs/price_sources/Malawi/analysis.py:224`
- **`main`** (Function) — `slurm_logs/price_sources/Mali/analysis.py:260`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `build_sources` | Function | `slurm_logs/price_sources/Ethiopia/analysis.py` | 203 |
| `main` | Function | `slurm_logs/price_sources/Ethiopia/analysis.py` | 276 |
| `to_kg` | Function | `slurm_logs/price_sources/Malawi/analysis.py` | 132 |
| `run` | Function | `slurm_logs/price_sources/Malawi/analysis.py` | 224 |
| `main` | Function | `slurm_logs/price_sources/Mali/analysis.py` | 260 |
| `main` | Function | `slurm_logs/price_sources/Niger/analysis.py` | 244 |
| `safe_model_rows` | Function | `slurm_logs/price_sources/Nigeria/analysis.py` | 138 |
| `main` | Function | `slurm_logs/price_sources/Nigeria/analysis.py` | 185 |
| `cells_for` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 240 |
| `run` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 249 |
| `geo_ladder` | Function | `slurm_logs/price_sources/common.py` | 74 |
| `cell_medians` | Function | `slurm_logs/price_sources/common.py` | 83 |
| `match_gaps` | Function | `slurm_logs/price_sources/common.py` | 113 |
| `model_row` | Function | `slurm_logs/price_sources/common.py` | 281 |
| `model_rows` | Function | `slurm_logs/price_sources/common.py` | 295 |
| `write_outputs` | Function | `slurm_logs/price_sources/common.py` | 314 |
| `describe` | Function | `slurm_logs/price_sources/synthesis.py` | 406 |
| `build_models` | Function | `slurm_logs/price_sources/synthesis.py` | 527 |
| `add` | Function | `slurm_logs/price_sources/synthesis.py` | 532 |
| `market_identity_table` | Function | `slurm_logs/price_sources/synthesis.py` | 963 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → _fwl_slope` | cross_community | 5 |
| `Run → _fwl_slope` | cross_community | 5 |
| `Main → _fwl_slope` | cross_community | 5 |
| `Main → _fwl_slope` | cross_community | 5 |
| `Run → _fwl_slope` | cross_community | 5 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tanzania | 9 calls |
| Niger | 4 calls |
| Tests | 2 calls |

## How to Explore

1. `context({name: "build_sources"})` — see callers and callees
2. `query({search_query: "price_sources"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
