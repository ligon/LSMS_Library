# #1452: recover EHCVM survey unit-weight wiring

Search tier: git + ripgrep. Current base 8558fefe0; preserved candidate a0e31481a, based on 2a888bcea, plus saved option-B repair.

## §1 Task
Recover the approved survey NSU table wiring for Burkina Faso, Mali, Niger and Senegal 2018-19/2021-22. Keep earlier waves on the estimate without this table's Quantity_kg (approved option B), preserving #1450's unsized pooling and #1487's subset behavior.

## §2 Existing machinery
Reuse ehcvm.food_acquired_ehcvm, served_unit_labels, size_label and the country derivation registry/stamping path. The held candidate extends this module with one NSU hook and per-country registry keys; tests/test_ehcvm_unit_weights.py covers its source selection and joins. Reuse transformations.food_kg_factors, _inferred_kg_rungs, _size_pooled_frame, _KG_RUNG_ARRAYS and the existing survey_kg precedence. Existing tests/test_food_kg_size_pool.py and subset/request-dependence tests own those invariants. Preserve ehcvm.community_price_slots and the purchase-value construction. Inherit STANDING.md §2 for IO/cache machinery.

## §3 Definitions and approved choices
Quantity and u remain reported native values. Quantity_kg is a summable construction from source weights, registered with Derivation plus nsu_year/nsu_join inputs; survey_kg reads it before inferred factors. National medians across matched table codes, 2018 use of 2021 files, and the disclosed dependency are approved in the root brief. Older waves use a tableless estimate scoped to the NSU derivation only; unrelated survey kilograms are not removed. See CLAUDE.md, canonical data_info.yml and STANDING.md §3.

## §4 Invariants
Preserve native rows/indexes, Quantity/Expenditure and unrelated features. Do not reuse 2021 product codes for 2018; use the reviewed label join. Preserve #1450's strict older-cell assertions and #1487's subset slice invariants. No new xfails, fixtures/baselines/dependencies/lockfiles, shared L2 writes or LSMS_NO_CACHE. Sanctioned get_dataframe/get_data_file IO only, exact checkout import/config identity, private caches with shared L1. Old worktrees and saved artifacts remain untouched. Inherit STANDING.md §4.

## §5 Reuse decision
Extend the held NSU hook and derivation declarations, not a new loader. Recover only the task's net 28-file payload and option-B repair onto current development; do not import unrelated old tree differences. Factor the existing inference/size-pool calls for the scoped second estimate; retain its existing mathematics. Keep source defects and evidence limits disclosed.

## §6 Open questions
No new policy ruling requested. Verify the saved scope detection, test adequacy, source/metadata selection, and current-development replay before treating the old green reports as current evidence. Ambiguous new source findings must be reported, not guessed.

## Verification
Pending recovery and exact-current source, control, cache-hash, Feature and offline-CI checks. The bounded design is recorded in the durable recovery report before implementation.
