---
name: bench
description: "Skill for the Bench area of LSMS_Library. 12 symbols across 5 files."
---

# Bench

12 symbols | 5 files | Cohesion: 74%

## When to Use

- Working with code in `bench/`
- Understanding how render_html, main, main work
- Modifying bench-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `bench/grain_census_871.py` | _diff_at, census_cell, main, build_tasks, add |
| `lsms_library/coverage_matrix.py` | default_snapshot_path, save_snapshot, coverage |
| `bench/matrix.py` | render_html, main |
| `bench/matrix_aggregate.py` | main |
| `lsms_library/local_tools.py` | read_parquet_grain_audit |

## Entry Points

Start here when exploring this area:

- **`render_html`** (Function) — `bench/matrix.py:81`
- **`main`** (Function) — `bench/matrix.py:129`
- **`main`** (Function) — `bench/matrix_aggregate.py:43`
- **`default_snapshot_path`** (Function) — `lsms_library/coverage_matrix.py:191`
- **`save_snapshot`** (Function) — `lsms_library/coverage_matrix.py:1246`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `render_html` | Function | `bench/matrix.py` | 81 |
| `main` | Function | `bench/matrix.py` | 129 |
| `main` | Function | `bench/matrix_aggregate.py` | 43 |
| `default_snapshot_path` | Function | `lsms_library/coverage_matrix.py` | 191 |
| `save_snapshot` | Function | `lsms_library/coverage_matrix.py` | 1246 |
| `coverage` | Function | `lsms_library/coverage_matrix.py` | 1314 |
| `census_cell` | Function | `bench/grain_census_871.py` | 257 |
| `main` | Function | `bench/grain_census_871.py` | 445 |
| `build_tasks` | Function | `bench/grain_census_871.py` | 311 |
| `add` | Function | `bench/grain_census_871.py` | 340 |
| `read_parquet_grain_audit` | Function | `lsms_library/local_tools.py` | 3153 |
| `_diff_at` | Function | `bench/grain_census_871.py` | 142 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → _repo_root` | cross_community | 5 |
| `Main → _token_css` | cross_community | 5 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Lsms_library | 3 calls |
| Docs_hooks | 2 calls |
| Tests | 1 calls |

## How to Explore

1. `context({name: "render_html"})` — see callers and callees
2. `query({search_query: "bench"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
