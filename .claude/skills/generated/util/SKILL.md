---
name: util
description: "Skill for the Util area of LSMS_Library. 30 symbols across 6 files."
---

# Util

30 symbols | 6 files | Cohesion: 76%

## When to Use

- Working with code in `lsms_library/`
- Understanding how find_geo_files, read_source_url, cmd_audit work
- Modifying util-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `lsms_library/util/geo_audit.py` | find_geo_files, read_source_url, cmd_audit, _find_missing_geo_waves, cmd_download (+5) |
| `lsms_library/util/generate_dvc_stages.py` | build_materialize_block, generate_country, discover_countries, main, load_yaml_file (+2) |
| `lsms_library/util/run_stage.py` | _compute_make_jobs, _default_target, _run_make, main, _python_bin (+2) |
| `lsms_library/util/orgtbl.py` | _split_row, add_or_update_column, main |
| `SkunkWorks/aggregation/publish_ghana.py` | publish, main |
| `lsms_library/cli.py` | generate_dvc |

## Entry Points

Start here when exploring this area:

- **`find_geo_files`** (Function) — `lsms_library/util/geo_audit.py:62`
- **`read_source_url`** (Function) — `lsms_library/util/geo_audit.py:98`
- **`cmd_audit`** (Function) — `lsms_library/util/geo_audit.py:195`
- **`cmd_download`** (Function) — `lsms_library/util/geo_audit.py:428`
- **`main`** (Function) — `lsms_library/util/geo_audit.py:583`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `find_geo_files` | Function | `lsms_library/util/geo_audit.py` | 62 |
| `read_source_url` | Function | `lsms_library/util/geo_audit.py` | 98 |
| `cmd_audit` | Function | `lsms_library/util/geo_audit.py` | 195 |
| `cmd_download` | Function | `lsms_library/util/geo_audit.py` | 428 |
| `main` | Function | `lsms_library/util/geo_audit.py` | 583 |
| `publish` | Function | `SkunkWorks/aggregation/publish_ghana.py` | 20 |
| `main` | Function | `SkunkWorks/aggregation/publish_ghana.py` | 64 |
| `add_or_update_column` | Function | `lsms_library/util/orgtbl.py` | 64 |
| `main` | Function | `lsms_library/util/orgtbl.py` | 219 |
| `generate_dvc` | Function | `lsms_library/cli.py` | 634 |
| `build_materialize_block` | Function | `lsms_library/util/generate_dvc_stages.py` | 146 |
| `generate_country` | Function | `lsms_library/util/generate_dvc_stages.py` | 229 |
| `discover_countries` | Function | `lsms_library/util/generate_dvc_stages.py` | 256 |
| `main` | Function | `lsms_library/util/generate_dvc_stages.py` | 276 |
| `guess_lat_lon_vars` | Function | `lsms_library/util/geo_audit.py` | 113 |
| `guess_idxvar` | Function | `lsms_library/util/geo_audit.py` | 124 |
| `generate_snippet` | Function | `lsms_library/util/geo_audit.py` | 162 |
| `cmd_ingest` | Function | `lsms_library/util/geo_audit.py` | 506 |
| `main` | Function | `lsms_library/util/run_stage.py` | 115 |
| `load_yaml_file` | Function | `lsms_library/util/generate_dvc_stages.py` | 80 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 4 calls |
| Lsms_library | 1 calls |
| Aggregation | 1 calls |

## How to Explore

1. `context({name: "find_geo_files"})` — see callers and callees
2. `query({search_query: "util"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
