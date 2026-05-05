---
name: tests
description: "Skill for the Tests area of LSMS_Library. 141 symbols across 23 files."
---

# Tests

141 symbols | 23 files | Cohesion: 93%

## When to Use

- Working with code in `tests/`
- Understanding how pytest_configure, data_root, test_uganda_makefile_backfill work
- Modifying tests-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `tests/test_age_intervals.py` | test_default_first_bucket_contains_0_through_3, test_default_bucket_boundaries_are_left_closed, test_default_top_bucket_is_unbounded, test_ages_below_zero_become_nan, test_sub_year_infant_split (+19) |
| `tests/test_data_access.py` | test_s3_read_when_creds_exist, test_s3_write_when_env_set, test_s3_none_when_no_creds, test_s3_none_when_creds_empty, test_gdrive_read_when_creds_exist (+13) |
| `tests/test_food_labels.py` | _uganda_food_cache_exists, _fake_country, _sample_expenditure_df, _food_items_table, test_relabel_j_preferred_is_noop (+6) |
| `tests/test_dvc_caching.py` | test_stale_cache_triggers_rebuild, test_valid_cache_uses_cached_file, test_dvc_cache_applies_location_index, test_panel_ids_dict_cache_written_as_json, test_panel_ids_dataframe_cache_written_as_parquet (+4) |
| `tests/test_data_separation.py` | test_env_override, test_default_without_env, test_default_is_space_free, test_whitespace_override_warns, _country_makefiles (+2) |
| `tests/test_canonical_spellings.py` | _make_roster_df, test_categorical_variants_replaced, test_categorical_lowercase_variants, test_categorical_already_canonical_is_noop, test_string_variants_replaced (+2) |
| `tests/test_schema_consistency.py` | _load_yaml, _all_data_scheme_paths, _schemes_with_table, test_no_rejected_column_spellings, test_all_data_scheme_files_parse (+2) |
| `tests/test_panel_attrition.py` | _has_cached_table, _attrition_matrix, _is_upper_triangular, _nonzero_adjacent, test_panel_attrition_household_characteristics_is_upper_triangular (+1) |
| `lsms_library/transformations.py` | age_intervals, _is_int_bound, _fmt_bound, format_interval, dummies (+1) |
| `tests/test_uganda_api_vs_replication.py` | _load_replication, _call_api, _merge_on_common_index, _coerce_string_na, _compare_column (+1) |

## Entry Points

Start here when exploring this area:

- **`pytest_configure`** (Function) — `conftest.py:53`
- **`data_root`** (Function) — `lsms_library/paths.py:68`
- **`test_uganda_makefile_backfill`** (Function) — `tests/test_uganda_tables.py:43`
- **`test_uganda_household_characteristics_has_m_index`** (Function) — `tests/test_uganda_tables.py:88`
- **`test_fallback_path_uses_wave_data_scheme`** (Function) — `tests/test_uganda_tables.py:110`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `pytest_configure` | Function | `conftest.py` | 53 |
| `data_root` | Function | `lsms_library/paths.py` | 68 |
| `test_uganda_makefile_backfill` | Function | `tests/test_uganda_tables.py` | 43 |
| `test_uganda_household_characteristics_has_m_index` | Function | `tests/test_uganda_tables.py` | 88 |
| `test_fallback_path_uses_wave_data_scheme` | Function | `tests/test_uganda_tables.py` | 110 |
| `uganda_root` | Function | `tests/test_uganda_invariance.py` | 112 |
| `test_parquet_matches_baseline` | Function | `tests/test_uganda_invariance.py` | 133 |
| `test_food_prices_cache_dtype_float64` | Function | `tests/test_food_prices_dtype.py` | 64 |
| `test_food_quantities_cache_dtype_float64` | Function | `tests/test_food_prices_dtype.py` | 80 |
| `test_env_override` | Function | `tests/test_data_separation.py` | 26 |
| `test_default_without_env` | Function | `tests/test_data_separation.py` | 34 |
| `test_default_is_space_free` | Function | `tests/test_data_separation.py` | 47 |
| `test_whitespace_override_warns` | Function | `tests/test_data_separation.py` | 70 |
| `fingerprint` | Function | `tests/generate_baseline.py` | 21 |
| `build_manifest` | Function | `tests/generate_baseline.py` | 44 |
| `main` | Function | `tests/generate_baseline.py` | 77 |
| `age_intervals` | Function | `lsms_library/transformations.py` | 11 |
| `test_default_first_bucket_contains_0_through_3` | Function | `tests/test_age_intervals.py` | 31 |
| `test_default_bucket_boundaries_are_left_closed` | Function | `tests/test_age_intervals.py` | 35 |
| `test_default_top_bucket_is_unbounded` | Function | `tests/test_age_intervals.py` | 42 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Load_json_cache → _warn_on_whitespace` | cross_community | 6 |
| `Load_with_dvc_cache → _warn_on_whitespace` | cross_community | 6 |
| `Load_from_waves → _warn_on_whitespace` | cross_community | 6 |
| `Populate_and_push → _gpg_decrypt` | cross_community | 6 |
| `_aggregate_wave_data → _warn_on_whitespace` | cross_community | 6 |
| `Load_dataframe_with_dvc → _warn_on_whitespace` | cross_community | 5 |
| `Push_to_cache → _gpg_decrypt` | cross_community | 5 |
| `Populate_and_push → _validate_wb_api_key` | cross_community | 5 |
| `Populate_and_push → _check_remote_access` | cross_community | 5 |
| `Collect_stage_outputs → _warn_on_whitespace` | cross_community | 5 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Lsms_library | 1 calls |

## How to Explore

1. `gitnexus_context({name: "pytest_configure"})` — see callers and callees
2. `gitnexus_query({query: "tests"})` — find related execution flows
3. Read key files listed above for implementation details
