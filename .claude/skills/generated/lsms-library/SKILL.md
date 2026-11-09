---
name: lsms-library
description: "Skill for the Lsms_library area of LSMS_Library. 426 symbols across 68 files."
---

# Lsms_library

426 symbols | 68 files | Cohesion: 67%

## When to Use

- Working with code in `lsms_library/`
- Understanding how is_this_feature_sane, validate_feature, populate_and_push work
- Modifying lsms_library-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `lsms_library/transformations.py` | _level_or_column, _own_price_rung, _median_price_rung, _missing_looking_unit_mask, _survey_median_factors (+39) |
| `lsms_library/data_access.py` | _normalise_subdir, _acquisition_data_dir, _receipt_key, _subdir_mapping, _check_subdir_acquisition (+33) |
| `lsms_library/diagnostics.py` | _load_scheme, _check_not_empty, _check_has_index, _check_index_levels, _check_no_null_index (+25) |
| `lsms_library/local_tools.py` | _is_sas_xport, _stata_release, _is_spss, _read_parquet_with_index_policy, read_file (+25) |
| `lsms_library/ehcvm.py` | inputs_last_purchase, inputs_region_milieu_stratum, _normalise_index_as_served, _extracted_units, derive_nsu_kg_factor (+23) |
| `lsms_library/coverage_matrix.py` | _repo_root, default_blessed_path, default_verdicts_path, load_blessed, default_blocked_path (+17) |
| `lsms_library/provenance.py` | acquired_release, read_provenance, _regions, _primary_url, preserved_prose (+13) |
| `lsms_library/_parallel_waves.py` | _ledgers, _datetime_ledger, _drop_ledger, _build_captured, _run_group (+13) |
| `lsms_library/cli.py` | _registered_country_dirs, _country_or_exit, _print_list, cache_list, countries (+13) |
| `lsms_library/datetime_parse.py` | one, _file_datetime_report, _emit_datetime_report, _record_datetime_report, _blank (+7) |

## Entry Points

Start here when exploring this area:

- **`is_this_feature_sane`** (Function) — `lsms_library/diagnostics.py:739`
- **`validate_feature`** (Function) — `lsms_library/diagnostics.py:969`
- **`populate_and_push`** (Function) — `lsms_library/data_access.py:2092`
- **`add_wave`** (Function) — `lsms_library/data_access.py:2665`
- **`acquired_release`** (Function) — `lsms_library/provenance.py:855`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `DatetimeWarning` | Class | `lsms_library/datetime_parse.py` | 137 |
| `DatetimeCoercionWarning` | Class | `lsms_library/datetime_parse.py` | 159 |
| `is_this_feature_sane` | Function | `lsms_library/diagnostics.py` | 739 |
| `validate_feature` | Function | `lsms_library/diagnostics.py` | 969 |
| `populate_and_push` | Function | `lsms_library/data_access.py` | 2092 |
| `add_wave` | Function | `lsms_library/data_access.py` | 2665 |
| `acquired_release` | Function | `lsms_library/provenance.py` | 855 |
| `read_provenance` | Function | `lsms_library/provenance.py` | 992 |
| `audit_quantities` | Function | `lsms_library/quantity_audit.py` | 851 |
| `offenders` | Function | `slurm_logs/isa_backlog_2026-10-21/637-tanzania-unit-grain/round2_measure.py` | 105 |
| `test_tanzania_2020_21_coconut_and_timber_rows_fire` | Function | `tests/test_quantity_screen.py` | 648 |
| `build_transforms_fingerprint` | Function | `lsms_library/_build_registry.py` | 586 |
| `framework_imports_fingerprint` | Function | `lsms_library/_build_registry.py` | 606 |
| `test_the_context_does_not_move_any_build_fingerprint` | Function | `tests/test_gh868_reduce_to_agreed_ledger.py` | 292 |
| `test_label_selection_is_outside_the_build_fingerprint` | Function | `tests/test_label_selection.py` | 450 |
| `check_panel_consistency` | Function | `lsms_library/diagnostics.py` | 1780 |
| `verdict` | Function | `slurm_logs/weight_normalisation/xpass_attrib.py` | 12 |
| `read_file` | Function | `lsms_library/local_tools.py` | 1861 |
| `retry_with_path` | Function | `lsms_library/local_tools.py` | 2019 |
| `scan` | Function | `slurm_logs/isa_backlog_2026-10-21/1234-map-index-unit/unit_census.py` | 64 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Seed_kg → _metric_scalar` | cross_community | 8 |
| `Prebuild → _env_int` | cross_community | 7 |
| `Seed_kg → _is_currency_denominated` | cross_community | 7 |
| `Seed_kg → _as_float` | cross_community | 7 |
| `Prebuild → _cgroup_limit_paths` | cross_community | 6 |
| `Cluster_features → Countries_root` | cross_community | 6 |
| `Cluster_features → _intree_sources_allowed` | cross_community | 6 |
| `Cluster_features → _dct_path_for` | cross_community | 6 |
| `Cluster_features → _parse_dct` | cross_community | 6 |
| `Cluster_features → _detect_dat_layout` | cross_community | 6 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 99 calls |
| _ | 11 calls |
| Scripts | 5 calls |

## How to Explore

1. `context({name: "is_this_feature_sane"})` — see callers and callees
2. `query({search_query: "lsms_library"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
