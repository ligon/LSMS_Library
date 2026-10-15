# Prior-art ledger: Peru #700 and Panama #777 landing checks

## §1 Task
Pin the existing branch's Peru cluster joins and Panama money-unit correction,
and make sample expectations reflect documented acquired but unwired Peru waves.

## §2 Existing machinery
Search: rg + git. Reuse Country.sample, _join_v_from_sample,
Country.food_acquired, and get_dataframe (STANDING.md §2).
tests/test_sample.py owns country coverage and missing-weight exceptions.
Peru 1994/_/mapping.py pads both sample and cluster keys; Panama's 1997
script masks quantity by raw unit code 77 before constructing source rows.
Existing test_food_prices_units_kwarg.py pins derived price counts.

## §3 Definitions
Inherit STANDING.md §3 and lsms_library/data_info.yml.
Peru CONTENTS "sample carries NO weight": REG01 _weight_ is the constant
SPSS export variable, not the seven documented domain expansion factors.
Peru CONTENTS "Wiring status": only 1994 is configured; 1985/1990/1991 are
acquired but unwired. Panama CONTENTS distinguishes library Value (Q=E)
from raw code 77 value (no interpretable quantity).

## §4 Invariants
Preserve every other country's wave coverage expectation; name only Peru's
three missing waves. Assert Peru 1994 sample/cluster/household prefix equality,
all roster clusters joined, and original missing weights.
Panama expenditure survives while code-77 Quantity is absent; retain capital
Value semantics. No fixtures, derived baselines, inference policy or sources change.

## §5 Reuse decision
Extend existing sample exceptions and add data-gated assertions against
independent raw quantities/keys. Reuse all runtime transforms unchanged.

## §6 Questions
None blocking: the approved landing workplan permits these narrow expectations.
