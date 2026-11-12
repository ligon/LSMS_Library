---
name: skill
description: "Skill for the _ area of LSMS_Library. 808 symbols across 219 files."
---

# _

808 symbols | 219 files | Cohesion: 75%

## When to Use

- Working with code in `lsms_library/`
- Understanding how food_expenditures, nonfood_expenditures, change_id work
- Modifying _-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `lsms_library/countries/Ethiopia/_/ethiopia.py` | _household_identification_from_file, _household_roster_from_file, _clean_unit_label, derive_sect12_sale, _no_sale_reading (+58) |
| `lsms_library/countries/Malawi/_/malawi.py` | food_acquired_to_canonical, apply_harmonize_food, _malawi_code_map, _map_codes, plot_features_for_wave (+58) |
| `lsms_library/countries/Nigeria/_/nigeria.py` | _species_labels, harmonized_food_labels, _food_item_labels, code_key, food_unit_labels (+53) |
| `lsms_library/countries/Uganda/_/uganda.py` | locating_cover, inputs_unlocated_identity, inputs_agriculture_cover, _comm_frame_ea_map, comm_frame_ea (+43) |
| `lsms_library/countries/Mali/_/mali.py` | pid, producing_trees, _tree_roster_inputs, _species_labels, livestock_finalize (+41) |
| `lsms_library/countries/Niger/_/niger.py` | _crop_reported_observations, _dry_season_inputs, concat_wave_parquets, _crop_condition_labels, i (+34) |
| `lsms_library/countries/Tanzania/_/tanzania.py` | _household_identification_from_file, _household_roster_from_file, food_acquired, _geocode_fields, cluster_region_code (+23) |
| `lsms_library/countries/GhanaLSS/_/ghanalss.py` | _sum_expenditures_from_file, _household_roster_from_file, change_id, i, _as_float (+15) |
| `lsms_library/countries/GhanaSPS/_/ghanasps.py` | _sum_expenditures_from_file, _household_roster_from_file, product_label, month_string, sum_duplicate_harvest_records (+11) |
| `lsms_library/local_tools.py` | allow_intree_sources, get_dataframe, format_id, get_categorical_mapping, _dvc_working_directory (+8) |

## Entry Points

Start here when exploring this area:

- **`food_expenditures`** (Function) — `lsms_library/countries/CotedIvoire/2018-19/_/food_expenditures.py:41`
- **`nonfood_expenditures`** (Function) — `lsms_library/countries/CotedIvoire/2018-19/_/nonfood_expenditures.py:41`
- **`change_id`** (Function) — `lsms_library/countries/GhanaLSS/_/ghanalss.py:302`
- **`id_walk`** (Function) — `lsms_library/countries/GhanaLSS/_/household_roster.py:13`
- **`concat_wave_parquets`** (Function) — `lsms_library/countries/Niger/_/niger.py:2354`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `food_expenditures` | Function | `lsms_library/countries/CotedIvoire/2018-19/_/food_expenditures.py` | 41 |
| `nonfood_expenditures` | Function | `lsms_library/countries/CotedIvoire/2018-19/_/nonfood_expenditures.py` | 41 |
| `change_id` | Function | `lsms_library/countries/GhanaLSS/_/ghanalss.py` | 302 |
| `id_walk` | Function | `lsms_library/countries/GhanaLSS/_/household_roster.py` | 13 |
| `concat_wave_parquets` | Function | `lsms_library/countries/Niger/_/niger.py` | 2354 |
| `inputs_split_off_parent_cluster` | Function | `lsms_library/countries/Tanzania/2019-20/_/mapping.py` | 79 |
| `inputs_upd_district_name` | Function | `lsms_library/countries/Tanzania/_/_upd_district.py` | 151 |
| `food_acquired` | Function | `lsms_library/countries/Tanzania/_/tanzania.py` | 540 |
| `locating_cover` | Function | `lsms_library/countries/Uganda/_/uganda.py` | 355 |
| `inputs_unlocated_identity` | Function | `lsms_library/countries/Uganda/_/uganda.py` | 395 |
| `inputs_agriculture_cover` | Function | `lsms_library/countries/Uganda/_/uganda.py` | 470 |
| `allow_intree_sources` | Function | `lsms_library/local_tools.py` | 905 |
| `get_dataframe` | Function | `lsms_library/local_tools.py` | 1780 |
| `report` | Function | `slurm_logs/2026-05-09_session/probe_serbia_cardinality.py` | 37 |
| `glss7_names` | Function | `slurm_logs/2026-09-12_ghanalss_nonfood/nf_labels.py` | 16 |
| `labels` | Function | `slurm_logs/ghana_audit/community_prices/probe_labels.py` | 10 |
| `main` | Function | `slurm_logs/ghana_audit/community_prices/probe_labels.py` | 23 |
| `main` | Function | `slurm_logs/ghana_audit/u6_measure_731.py` | 10 |
| `load` | Function | `slurm_logs/isa_backlog_2026-10-21/637-tanzania-unit-grain/round3/oracle.py` | 55 |
| `rd` | Function | `slurm_logs/isa_backlog_2026-10-21/820-ethiopia-sect12/scripts/s11drop.py` | 26 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Cluster_features → _warn_on_whitespace` | cross_community | 6 |
| `Cluster_features → Countries_root` | cross_community | 6 |
| `Cluster_features → _intree_sources_allowed` | cross_community | 6 |
| `Cluster_features → _dct_path_for` | cross_community | 6 |
| `Cluster_features → _parse_dct` | cross_community | 6 |
| `Cluster_features → _detect_dat_layout` | cross_community | 6 |
| `Read_file → _is_transient_fetch_error` | cross_community | 6 |
| `Inputs_survey_kg_estimate → _warn_on_whitespace` | cross_community | 6 |
| `Inputs_survey_kg_estimate → Countries_root` | cross_community | 6 |
| `Food_acquired → Countries_root` | cross_community | 6 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 118 calls |
| Lsms_library | 20 calls |
| Integration_2026-05-07 | 1 calls |
| Ghanasps | 1 calls |

## How to Explore

1. `context({name: "food_expenditures"})` — see callers and callees
2. `query({search_query: "_"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
