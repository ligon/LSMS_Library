---
name: aggregation
description: "Skill for the Aggregation area of LSMS_Library. 75 symbols across 10 files."
---

# Aggregation

75 symbols | 10 files | Cohesion: 71%

## When to Use

- Working with code in `SkunkWorks/`
- Understanding how evaluate_partitions, test_partition_must_be_exhaustive_and_keep_anchor, test_no_household_can_cross_training_evaluation_split work
- Modifying aggregation-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `SkunkWorks/aggregation/test_evaluation.py` | _fine_only, test_partition_must_be_exhaustive_and_keep_anchor, test_no_household_can_cross_training_evaluation_split, test_comparison_and_reference_populations_have_correct_roles, test_common_error_and_weighted_coverage_are_literal_population_means (+12) |
| `SkunkWorks/aggregation/ghana_validation.py` | household_split, evaluation_arguments, prediction_diagnostics, paired_prediction_difference, run_comparison (+9) |
| `SkunkWorks/aggregation/ghana_inventory.py` | draft_partition, _grouped, _delivered_goods, _amounts, wave_inventory (+4) |
| `SkunkWorks/aggregation/test_ghana_inventory.py` | proposal, frames, test_draft_defaults_to_singletons_and_records_absent_members, test_singleton_cautions_are_carried_into_review_rows, test_draft_rejects_duplicate_members_name_collisions_and_nonfood_merges (+3) |
| `SkunkWorks/aggregation/evaluation.py` | evaluate_partitions, _weighted_mean, _cells, _expenditures, _membership (+2) |
| `SkunkWorks/aggregation/survey_simulation.py` | curated_membership, template_from_frames, load_uganda, run_template, main (+1) |
| `SkunkWorks/aggregation/test_survey_simulation.py` | template, test_each_regime_preserves_joint_mask_weights_markets_and_zero_rows, test_substitution_holds_equal_beta_no_error_group_amount_fixed, test_empirical_template_fits_through_existing_evaluator, test_empirical_dgp_is_reproducible_without_global_rng_side_effects |
| `SkunkWorks/aggregation/test_ghana_validation.py` | test_reserved_targets_do_not_enter_fit_score_or_reference_design, test_missing_controls_remain_in_denominator_but_not_reference, test_prediction_calibration_uses_only_training_targets_and_fixed_population, test_psu_resampling_keeps_blocks_intact_and_does_not_use_heldout_rows |
| `SkunkWorks/aggregation/simulation.py` | simulation_inputs, run_experiment, main |
| `SkunkWorks/aggregation/ghana_gate.py` | gate_groups, main |

## Entry Points

Start here when exploring this area:

- **`evaluate_partitions`** (Function) — `SkunkWorks/aggregation/evaluation.py:100`
- **`test_partition_must_be_exhaustive_and_keep_anchor`** (Function) — `SkunkWorks/aggregation/test_evaluation.py:55`
- **`test_no_household_can_cross_training_evaluation_split`** (Function) — `SkunkWorks/aggregation/test_evaluation.py:66`
- **`test_comparison_and_reference_populations_have_correct_roles`** (Function) — `SkunkWorks/aggregation/test_evaluation.py:73`
- **`test_common_error_and_weighted_coverage_are_literal_population_means`** (Function) — `SkunkWorks/aggregation/test_evaluation.py:95`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `evaluate_partitions` | Function | `SkunkWorks/aggregation/evaluation.py` | 100 |
| `test_partition_must_be_exhaustive_and_keep_anchor` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 55 |
| `test_no_household_can_cross_training_evaluation_split` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 66 |
| `test_comparison_and_reference_populations_have_correct_roles` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 73 |
| `test_common_error_and_weighted_coverage_are_literal_population_means` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 95 |
| `test_truth_never_changes_fitting_scores_or_normalization` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 148 |
| `test_held_out_expenditures_cannot_change_fitted_parameters` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 163 |
| `test_missing_core_score_suppresses_mse_instead_of_shrinking_population` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 176 |
| `test_zero_weight_core_cell_does_not_change_error_population` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 188 |
| `test_missing_reference_score_is_explicit_failure` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 199 |
| `test_missing_anchor_is_explicit_failure` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 207 |
| `test_single_retained_good_is_reported_before_estimation` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 214 |
| `test_anchor_unit_change_rescales_both_scores_and_error` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 265 |
| `test_input_order_and_caller_objects_do_not_change_results` | Function | `SkunkWorks/aggregation/test_evaluation.py` | 274 |
| `gate_groups` | Function | `SkunkWorks/aggregation/ghana_gate.py` | 84 |
| `household_split` | Function | `SkunkWorks/aggregation/ghana_validation.py` | 90 |
| `evaluation_arguments` | Function | `SkunkWorks/aggregation/ghana_validation.py` | 134 |
| `prediction_diagnostics` | Function | `SkunkWorks/aggregation/ghana_validation.py` | 163 |
| `paired_prediction_difference` | Function | `SkunkWorks/aggregation/ghana_validation.py` | 224 |
| `run_comparison` | Function | `SkunkWorks/aggregation/ghana_validation.py` | 276 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → Text_evidence` | cross_community | 5 |
| `Main → Draft_partition` | cross_community | 5 |
| `Main → _cells` | cross_community | 5 |
| `Main → _membership` | cross_community | 5 |
| `Run_comparison → _cells` | cross_community | 4 |
| `Main → _delivered_goods` | cross_community | 4 |
| `Main → Survey_inputs` | cross_community | 4 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 3 calls |

## How to Explore

1. `context({name: "evaluate_partitions"})` — see callers and callees
2. `query({search_query: "aggregation"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`
