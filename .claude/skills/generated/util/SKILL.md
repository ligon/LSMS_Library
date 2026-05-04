---
name: util
description: "Skill for the Util area of LSMS_Library. 43 symbols across 5 files."
---

# Util

43 symbols | 5 files | Cohesion: 85%

## When to Use

- Working with code in `lsms_library/`
- Understanding how check_configured, read_source_url, guess_lat_lon_vars work
- Modifying util-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `lsms_library/util/geo_audit.py` | check_configured, read_source_url, guess_lat_lon_vars, guess_idxvar, data_path_for_yaml (+12) |
| `lsms_library/util/generate_dvc_stages.py` | load_yaml_file, country_stage_entries, wave_tables, ensure_gitkeep, dump_yaml (+11) |
| `lsms_library/util/run_stage.py` | _python_bin, _runtime_env, _compute_make_jobs, _default_target, _run_make (+3) |
| `lsms_library/cli.py` | generate_dvc |
| `lsms_library/country.py` | _slugify |

## Entry Points

Start here when exploring this area:

- **`check_configured`** (Function) — `lsms_library/util/geo_audit.py:76`
- **`read_source_url`** (Function) — `lsms_library/util/geo_audit.py:92`
- **`guess_lat_lon_vars`** (Function) — `lsms_library/util/geo_audit.py:106`
- **`guess_idxvar`** (Function) — `lsms_library/util/geo_audit.py:117`
- **`data_path_for_yaml`** (Function) — `lsms_library/util/geo_audit.py:134`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `check_configured` | Function | `lsms_library/util/geo_audit.py` | 76 |
| `read_source_url` | Function | `lsms_library/util/geo_audit.py` | 92 |
| `guess_lat_lon_vars` | Function | `lsms_library/util/geo_audit.py` | 106 |
| `guess_idxvar` | Function | `lsms_library/util/geo_audit.py` | 117 |
| `data_path_for_yaml` | Function | `lsms_library/util/geo_audit.py` | 134 |
| `generate_snippet` | Function | `lsms_library/util/geo_audit.py` | 152 |
| `cmd_audit` | Function | `lsms_library/util/geo_audit.py` | 185 |
| `cmd_ingest` | Function | `lsms_library/util/geo_audit.py` | 494 |
| `main` | Function | `lsms_library/util/geo_audit.py` | 571 |
| `parse_args` | Function | `lsms_library/util/run_stage.py` | 102 |
| `main` | Function | `lsms_library/util/run_stage.py` | 114 |
| `find_geo_files` | Function | `lsms_library/util/geo_audit.py` | 57 |
| `cmd_download` | Function | `lsms_library/util/geo_audit.py` | 416 |
| `load_yaml_file` | Function | `lsms_library/util/generate_dvc_stages.py` | 79 |
| `country_stage_entries` | Function | `lsms_library/util/generate_dvc_stages.py` | 86 |
| `wave_tables` | Function | `lsms_library/util/generate_dvc_stages.py` | 119 |
| `ensure_gitkeep` | Function | `lsms_library/util/generate_dvc_stages.py` | 132 |
| `dump_yaml` | Function | `lsms_library/util/generate_dvc_stages.py` | 202 |
| `wave_dirs` | Function | `lsms_library/util/generate_dvc_stages.py` | 217 |
| `generate_country` | Function | `lsms_library/util/generate_dvc_stages.py` | 228 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Generate_dvc → Load_yaml` | cross_community | 5 |
| `Main → Load_yaml` | cross_community | 5 |
| `Main → _warn_on_whitespace` | cross_community | 4 |
| `Main → _python_bin` | intra_community | 4 |
| `Generate_dvc → _default_target` | cross_community | 4 |
| `Generate_dvc → _output_from_target` | cross_community | 4 |
| `Generate_dvc → Relative_path` | cross_community | 4 |
| `Main → Find_geo_files` | cross_community | 4 |
| `Main → Data_path_for_yaml` | intra_community | 4 |
| `Main → Guess_lat_lon_vars` | intra_community | 4 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 2 calls |
| Lsms_library | 1 calls |

## How to Explore

1. `gitnexus_context({name: "check_configured"})` — see callers and callees
2. `gitnexus_query({query: "util"})` — find related execution flows
3. Read key files listed above for implementation details
