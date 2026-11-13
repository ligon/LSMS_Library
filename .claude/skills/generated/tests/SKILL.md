---
name: tests
description: "Skill for the Tests area of LSMS_Library. 3799 symbols across 437 files."
---

# Tests

3799 symbols | 437 files | Cohesion: 75%

## When to Use

- Working with code in `tests/`
- Understanding how inputs_cluster_features, inputs_cluster_features, ehcvm_is_sale work
- Modifying tests-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `tests/test_wave_provenance.py` | _shipped_source_orgs, _write_source, _catalog, _unresolved, _snapshot (+86) |
| `tests/test_visualizations.py` | test_nigeria_default_wave_is_the_most_recent_the_measure_holds, _all_text, _households, _long, _fills (+57) |
| `tests/test_863_transforms.py` | _plot_inputs, _plot_features, test_fertilizer_rate_nitrogen_per_hectare, test_fertilizer_rate_product_basis_is_the_raw_kilograms, test_fertilizer_rate_sums_inputs_and_areas_to_the_same_land_grain (+53) |
| `tests/test_feature_canonical_index.py` | test_sentinel_levels_are_canonical_levels, test_alias_targets_are_canonical_levels, test_every_real_corpus_shape_reaches_the_canonical_index, test_alias_runs_before_alignment, test_promotion_of_DISTINCT_values_refines_and_loses_nothing (+47) |
| `lsms_library/transformations.py` | _seeded_kg_factors, _shipped_factor_lookup, harvest_kg_factors, food_acquired_valued, median_price_valuation (+42) |
| `tests/test_coverage_matrix.py` | _env, _tiers, test_coverage_only_declared_vs_absent, test_readiness_builds_when_sanity_fails, test_readiness_broken_when_build_raises (+42) |
| `tests/test_crop_kg_factor.py` | _frame, _legacy_harvest_kg, test_frame_without_kgfactor_reproduces_the_old_algorithm, test_only_the_bottom_layers_act_without_reports, test_reported_factor_wins_and_converts_an_unknown_unit (+38) |
| `tests/test_ehcvm_unit_weights.py` | test_the_derivation_is_registered_with_the_shared_function, built, test_no_exact_metric_row_has_one_and_every_one_carries_the_key, test_the_ladder_counts_these_rows_as_the_surveys, test_the_inputs_callable_joins_and_counts_the_branches (+38) |
| `tests/test_gh1276_acquisition_subdir.py` | _record, _zip, _hold, _snapshot, _data (+37) |
| `tests/test_median_price_valuation.py` | _sold_frame, _sold_price, test_sold_unit_literal_price_oracle, test_sold_different_unit_one_row_negative_control, test_sold_reported_factor_precedes_label_and_missing_label (+36) |

## Entry Points

Start here when exploring this area:

- **`inputs_cluster_features`** (Function) — `lsms_library/countries/Burkina_Faso/2018-19/_/mapping.py:205`
- **`inputs_cluster_features`** (Function) — `lsms_library/countries/Burkina_Faso/2021-22/_/mapping.py:136`
- **`ehcvm_is_sale`** (Function) — `lsms_library/countries/Mali/_/mali.py:1021`
- **`inputs_hh_crop_sale_single_row`** (Function) — `lsms_library/countries/Mali/_/mali.py:1293`
- **`cached_file_hash`** (Function) — `lsms_library/local_tools.py:3034`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `TestRules` | Class | `tests/test_gh1060_1057_burkina_cluster_features.py` | 142 |
| `inputs_cluster_features` | Function | `lsms_library/countries/Burkina_Faso/2018-19/_/mapping.py` | 205 |
| `inputs_cluster_features` | Function | `lsms_library/countries/Burkina_Faso/2021-22/_/mapping.py` | 136 |
| `ehcvm_is_sale` | Function | `lsms_library/countries/Mali/_/mali.py` | 1021 |
| `inputs_hh_crop_sale_single_row` | Function | `lsms_library/countries/Mali/_/mali.py` | 1293 |
| `cached_file_hash` | Function | `lsms_library/local_tools.py` | 3034 |
| `countries_root` | Function | `lsms_library/paths.py` | 37 |
| `roster_has_interview_date` | Function | `slurm_logs/2026-06-16_share_female/evidence_table.py` | 20 |
| `main` | Function | `slurm_logs/ghana_audit/api_before_after_731.py` | 9 |
| `main` | Function | `slurm_logs/ghana_audit/community_prices/probe_price_files.py` | 25 |
| `run_script` | Function | `slurm_logs/isa_backlog_2026-10-21/1123-ehcvm-identity/diagnose_source.py` | 57 |
| `main` | Function | `slurm_logs/null_read_guard/sweep_build.py` | 35 |
| `drive` | Function | `tests/test_1111_malawi_applied_kg.py` | 21 |
| `test_all_six_driver_refs_equal_their_loader_literals_and_capture_before_fields` | Function | `tests/test_1120_malawi_labor_observations.py` | 154 |
| `test_every_real_native_line_and_payload_is_served` | Function | `tests/test_ehcvm_plot_inputs_identity.py` | 208 |
| `test_niger_make_recipe_stops_when_any_wave_script_fails` | Function | `tests/test_ehcvm_plot_inputs_identity.py` | 266 |
| `w1ph` | Function | `tests/test_gh1035_nigeria_w1_stated_weights.py` | 73 |
| `upd_raw` | Function | `tests/test_gh1441_tanzania_district_code.py` | 235 |
| `test_y5_column_is_the_stata_value_label` | Function | `tests/test_gh1441_tanzania_district_code.py` | 332 |
| `test_farm_level_rows_are_the_producing_roster` | Function | `tests/test_gh1476_mali_tree_farm_level.py` | 52 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Seed_kg → _metric_scalar` | cross_community | 8 |
| `Seed_kg → _is_currency_denominated` | cross_community | 7 |
| `Seed_kg → _as_float` | cross_community | 7 |
| `Measure → _metric_scalar` | cross_community | 6 |
| `Cluster_features → _warn_on_whitespace` | cross_community | 6 |
| `Cluster_features → Countries_root` | cross_community | 6 |
| `Cluster_features → _parse_dct` | cross_community | 6 |
| `Seed_kg → _level_series` | cross_community | 6 |
| `Main → _derive_fernet_key` | cross_community | 6 |
| `Inputs_survey_kg_estimate → _warn_on_whitespace` | cross_community | 6 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Lsms_library | 130 calls |
| _ | 113 calls |
| Scripts | 6 calls |
| Ghanasps | 2 calls |
| Feature_audit | 2 calls |
| Gh850_design | 1 calls |

## How to Explore

1. `context({name: "inputs_cluster_features"})` — see callers and callees
2. `query({search_query: "tests"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
