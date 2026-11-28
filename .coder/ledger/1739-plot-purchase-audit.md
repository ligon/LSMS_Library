# Prior-art ledger — #1739 purchase quantities in Site Q

Inherits [STANDING.md](STANDING.md) and
[1482-1485](1482-1485-quantity-diagnostics.md). Search tier: git + ripgrep.
Base: 47df62d49e614c9a56e319def7324145d138c3d9.

## §1 Task

Screen unusually high recorded plot_inputs.Quantity_purchased on its own
unit and reference population. This is a report, not a correction or price
test. Existing Quantity (applied), gift and other diagnostics must not move.

## §2 Existing machinery

- quantity_audit._axis and _usable_units handle named axes and absent,
  sentinel or currency labels. Reuse rather than infer a unit.
- quantity_audit._cell_references, _TAILS and _reports_for implement the
  existing positive-only p90, minimum 30 rows, floor and multiplier.
  Existing controls: test_quantity_screen.py,
  test_gh1482_1485_quantity_diagnostics.py, test_gh1493_near_zero_screen.py.
- _audit_crop_sales is precedent for keeping the reported transaction unit
  separate from harvest u and carrying an explicit unit_column in reports.
  It is not the purchase screen: crop state and sale prices are out of scope.
- Country._finalize_result and check_quantities route reports to the existing
  ledger, warning and strict control. They are on the read path.

## §3 Semantics

See data_info.yml plot_inputs and country scheme notes. Quantity is applied,
not necessarily bought; Unit_purchased names the purchase denominator. A
country without that field or a row without a usable value is unassessed.
Purchases are not gift quantities or money. Do not use applied u as fallback.

## §4 Invariants

Preserve exact frame/dtypes/index/attrs/order and all existing reports.
Purchases with usable explicit units share references only within country,
wave and any supplied period/season/visit and input kind. Within those bounds
try the crop-specific cell first, then pool crops but not input kinds.
No new threshold, low-tail rule, conversion, flag-based recode or row omission.
No cache version change. Measure at least three ISA country APIs and audit at
least one non-ISA comparison if its bounded held sources are complete.

## §5 Reuse decisions

Extend column dispatch and emit the same built-table-quantity report with
unit_column=Unit_purchased, not a separate audit site. Keep the normal
plot_inputs ladder as-is. Use the existing vectorized reference kernel with
explicit purchase rungs. Add a source-free independent hand-calculated test
and verify no fingerprint movement.

## §6 Limits

The incident reports do not prove currency was entered into a quantity field.
Thin cells are unassessed, not clean. All verified changes are reviewable and
report-only; the generic warning's existing strict option remains opt-in.
