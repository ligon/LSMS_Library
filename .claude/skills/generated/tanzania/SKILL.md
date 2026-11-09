---
name: tanzania
description: "Skill for the Tanzania area of LSMS_Library. 14 symbols across 6 files."
---

# Tanzania

14 symbols | 6 files | Cohesion: 77%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how build_sources, build_sources, source1_unit_value work
- Modifying tanzania-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/price_sources/Tanzania/analysis.py` | _tidy_geo, _to_kg_basis, _fold_u, source1_unit_value, source4_crop_sale (+4) |
| `slurm_logs/price_sources/Ethiopia/analysis.py` | _prep |
| `slurm_logs/price_sources/Malawi/analysis.py` | build_sources |
| `slurm_logs/price_sources/Nigeria/analysis.py` | build_sources |
| `slurm_logs/price_sources/Uganda/analysis.py` | attach_v_and_geo |
| `slurm_logs/price_sources/common.py` | attach_geo |

## Entry Points

Start here when exploring this area:

- **`build_sources`** (Function) — `slurm_logs/price_sources/Malawi/analysis.py:146`
- **`build_sources`** (Function) — `slurm_logs/price_sources/Nigeria/analysis.py:174`
- **`source1_unit_value`** (Function) — `slurm_logs/price_sources/Tanzania/analysis.py:146`
- **`source4_crop_sale`** (Function) — `slurm_logs/price_sources/Tanzania/analysis.py:165`
- **`source5_community`** (Function) — `slurm_logs/price_sources/Tanzania/analysis.py:188`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `build_sources` | Function | `slurm_logs/price_sources/Malawi/analysis.py` | 146 |
| `build_sources` | Function | `slurm_logs/price_sources/Nigeria/analysis.py` | 174 |
| `source1_unit_value` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 146 |
| `source4_crop_sale` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 165 |
| `source5_community` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 188 |
| `implausible_community_prices` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 353 |
| `attach_v_and_geo` | Function | `slurm_logs/price_sources/Uganda/analysis.py` | 389 |
| `attach_geo` | Function | `slurm_logs/price_sources/common.py` | 52 |
| `split_basis` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 374 |
| `per_item_table` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 389 |
| `_prep` | Function | `slurm_logs/price_sources/Ethiopia/analysis.py` | 163 |
| `_tidy_geo` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 107 |
| `_to_kg_basis` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 123 |
| `_fold_u` | Function | `slurm_logs/price_sources/Tanzania/analysis.py` | 136 |

## How to Explore

1. `context({name: "build_sources"})` — see callers and callees
2. `query({search_query: "tanzania"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
