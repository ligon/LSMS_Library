---
name: docs-hooks
description: "Skill for the Docs_hooks area of LSMS_Library. 15 symbols across 1 files."
---

# Docs_hooks

15 symbols | 1 files | Cohesion: 65%

## When to Use

- Working with code in `docs_hooks/`
- Understanding how wave_key, ordered_features, render_fragment work
- Modifying docs_hooks-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `docs_hooks/coverage_matrix.py` | wave_key, ordered_features, _grid, _table_view, render_fragment (+10) |

## Entry Points

Start here when exploring this area:

- **`wave_key`** (Function) — `docs_hooks/coverage_matrix.py:160`
- **`ordered_features`** (Function) — `docs_hooks/coverage_matrix.py:174`
- **`render_fragment`** (Function) — `docs_hooks/coverage_matrix.py:308`
- **`timeline_rows`** (Function) — `docs_hooks/coverage_matrix.py:440`
- **`render_timeline`** (Function) — `docs_hooks/coverage_matrix.py:477`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `wave_key` | Function | `docs_hooks/coverage_matrix.py` | 160 |
| `ordered_features` | Function | `docs_hooks/coverage_matrix.py` | 174 |
| `render_fragment` | Function | `docs_hooks/coverage_matrix.py` | 308 |
| `timeline_rows` | Function | `docs_hooks/coverage_matrix.py` | 440 |
| `render_timeline` | Function | `docs_hooks/coverage_matrix.py` | 477 |
| `on_page_markdown` | Function | `docs_hooks/coverage_matrix.py` | 562 |
| `_grid` | Function | `docs_hooks/coverage_matrix.py` | 253 |
| `_table_view` | Function | `docs_hooks/coverage_matrix.py` | 293 |
| `_token_css` | Function | `docs_hooks/coverage_matrix.py` | 186 |
| `_css` | Function | `docs_hooks/coverage_matrix.py` | 201 |
| `_snapshot_path` | Function | `docs_hooks/coverage_matrix.py` | 141 |
| `_status_banner` | Function | `docs_hooks/coverage_matrix.py` | 343 |
| `_fragment` | Function | `docs_hooks/coverage_matrix.py` | 378 |
| `_read_rows` | Function | `docs_hooks/coverage_matrix.py` | 146 |
| `_fragment_timeline` | Function | `docs_hooks/coverage_matrix.py` | 551 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → _token_css` | cross_community | 5 |

## How to Explore

1. `context({name: "wave_key"})` — see callers and callees
2. `query({search_query: "docs_hooks"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
