---
name: lsms-library
description: "Skill for the Lsms_library area of LSMS_Library. 140 symbols across 17 files."
---

# Lsms_library

140 symbols | 17 files | Cohesion: 84%

## When to Use

- Working with code in `lsms_library/`
- Understanding how safe_concat_dataframe_dict, load_from_waves, load_json_cache work
- Modifying lsms_library-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `lsms_library/country.py` | _working_directory, _redirect_stdout_to_stderr, _load_materialize_stage_map, _rebuild_failure_error, _resolve_materialize_stages (+36) |
| `lsms_library/diagnostics.py` | summarize, _check_not_empty, _check_has_index, _check_index_levels, _check_no_null_index (+21) |
| `lsms_library/cli.py` | _available_country_dirs, _print_list, cache_list, cache_clear, countries (+14) |
| `lsms_library/data_access.py` | _parse_dvc_remotes, permissions, can_read, can_write, _dvc_cmd (+8) |
| `lsms_library/transformations.py` | apply_derived, conversion_to_kgs, _normalize_columns, _get_kg_factors, _apply_kg_conversion (+4) |
| `lsms_library/config.py` | _config_dir, _config_file, _load_config, get, microdata_api_key (+3) |
| `tests/test_table_structure.py` | test_feature_is_sane, _parse_index_tuple, _load_all_schemes, _countries_with_housing |
| `lsms_library/feature.py` | _discover_countries_for_table, countries, _load_global_columns, columns |
| `tests/test_data_access.py` | test_parses_both_remotes, test_returns_empty_when_no_config, test_returns_credentialpath, test_returns_gdrive_cred_key |
| `lsms_library/paths.py` | _caller_country_and_wave, var_path, wave_data_path |

## Entry Points

Start here when exploring this area:

- **`safe_concat_dataframe_dict`** (Function) — `lsms_library/country.py:1633`
- **`load_from_waves`** (Function) — `lsms_library/country.py:1806`
- **`load_json_cache`** (Function) — `lsms_library/country.py:1879`
- **`load_dataframe_with_dvc`** (Function) — `lsms_library/country.py:1934`
- **`collect_stage_outputs`** (Function) — `lsms_library/country.py:2003`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `safe_concat_dataframe_dict` | Function | `lsms_library/country.py` | 1633 |
| `load_from_waves` | Function | `lsms_library/country.py` | 1806 |
| `load_json_cache` | Function | `lsms_library/country.py` | 1879 |
| `load_dataframe_with_dvc` | Function | `lsms_library/country.py` | 1934 |
| `collect_stage_outputs` | Function | `lsms_library/country.py` | 2003 |
| `consolidate_stage_outputs` | Function | `lsms_library/country.py` | 2021 |
| `load_with_dvc_cache` | Function | `lsms_library/country.py` | 2137 |
| `summarize` | Function | `lsms_library/diagnostics.py` | 111 |
| `is_this_feature_sane` | Function | `lsms_library/diagnostics.py` | 481 |
| `test_feature_is_sane` | Function | `tests/test_table_structure.py` | 253 |
| `validate_feature` | Function | `lsms_library/diagnostics.py` | 678 |
| `load_yaml` | Function | `lsms_library/yaml_utils.py` | 42 |
| `countries` | Function | `lsms_library/feature.py` | 89 |
| `resources` | Function | `lsms_library/country.py` | 360 |
| `resources` | Function | `lsms_library/country.py` | 880 |
| `test_column_table_has_countries` | Function | `tests/test_feature.py` | 70 |
| `apply_derived` | Function | `lsms_library/transformations.py` | 316 |
| `method` | Function | `lsms_library/country.py` | 346 |
| `column_mapping` | Function | `lsms_library/country.py` | 396 |
| `map_formatting_function` | Function | `lsms_library/country.py` | 421 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Load_json_cache → _warn_on_whitespace` | cross_community | 6 |
| `Load_with_dvc_cache → _warn_on_whitespace` | cross_community | 6 |
| `Load_with_dvc_cache → _make_jobs_flag` | cross_community | 6 |
| `Load_from_waves → _warn_on_whitespace` | cross_community | 6 |
| `Populate_and_push → _gpg_decrypt` | cross_community | 6 |
| `_aggregate_wave_data → _warn_on_whitespace` | cross_community | 6 |
| `Load_dataframe_with_dvc → _warn_on_whitespace` | cross_community | 5 |
| `Generate_dvc → Load_yaml` | cross_community | 5 |
| `Main → Load_yaml` | cross_community | 5 |
| `Push_to_cache → _gpg_decrypt` | cross_community | 5 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 15 calls |

## How to Explore

1. `gitnexus_context({name: "safe_concat_dataframe_dict"})` — see callers and callees
2. `gitnexus_query({query: "lsms_library"})` — find related execution flows
3. Read key files listed above for implementation details
