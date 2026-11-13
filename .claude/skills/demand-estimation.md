# Demand Estimation via the Country API

Estimate a CFE demand system after explicitly adapting the Country tables:

```python
import lsms_library as ll
from cfe.regression import Regression
from lsms_library.demands import prepare_demand_inputs

c = ll.Country('Tanzania')

x = c.food_expenditures(market='Region', basis='purchased')
d = c.household_characteristics(market='Region')
y, d = prepare_demand_inputs(x, d)

r = Regression(y=y, d=d, alltm=False)
r.get_beta()
```

Or via CLI / Makefile:

    make -C lsms_library demands country=Tanzania market=Region

Or run `python -m lsms_library.demands Tanzania --market Region`.
The helper and CLI retain the existing cash-outlay default. To include all
recorded acquisition values, request `basis='total'` in the food call or
`--basis total` on the CLI; this does not impute missing own-production or
in-kind values. A fit still requires enough observed goods and households;
successful input preparation is not an identified demand system.

## Acquisition source and the CFE index (GH #1439 / #1526)

`food_expenditures` intentionally retains acquisition source `s`. CFE accepts
log expenditures on exactly `(i, t, m, j)`, so passing the unadapted Series
raises `y must have index (i,t,m,j)`. The in-repository
`lsms_library.demands.prepare_demand_inputs` sums every **supplied** source
at `(i,t,m,j)` in monetary levels, **before** taking logs. This implements the
consumer aggregation documented by `food_expenditures_from_acquired`; it
does not change the Country food API or the installed CFE package.

The reduction is `sum(min_count=1, observed=True, dropna=False)`: all-missing
groups remain missing, partially missing groups yield the observed subtotal,
and reported zeros and negative adjustments enter the sum. No unreported
source is treated as a reported zero. Zero totals and missing totals have no
log and are omitted. Negative or infinite totals raise. Keep `x` for the
original source rows and their missingness; `y` is the model input.

The adapter also orders the characteristic index as `(i,t,m)`. It refuses
missing identity/market keys, repeated native keys, multiple markets for one
household-period, disagreements between the inputs' markets, and extra axes.
Resolve these upstream instead of dropping levels. CFE's own fitting selection
still governs missing characteristics, unmatched rows and support thresholds.

This is a **single-country** adapter. A `country` index or column on either
Feature operand is refused; explicitly select the same country from both
inputs first. Country attrs, when supplied, must agree on both operands.
It never pools currencies or aliases household IDs across countries.

`labels='Aggregate'` changes item `j`, not source `s`, so it still needs this
adapter. Before adding a `labels=` call, read
[`aggregate-labels/SKILL.md`](add-feature/food-acquired/aggregate-labels/SKILL.md):
Uganda curates an Aggregate column; Tanzania does not. Country raises
`LabelUnavailableError` for absent curation; a Feature call instead drops
unsupported countries with a warning and `attrs['labels_unavailable']`.

## The `market=` argument

`cfe.Regression` requires data indexed by `(i, t, m, j)` for
expenditures and `(i, t, m)` for household characteristics, where `m`
identifies a market (price regime).  The `market=` parameter names a
column from `cluster_features` (e.g. `'Region'`, `'District'`) to use
as `m`.

The analyst must choose: `Region` gives coarser markets with more
households per cell; `District` is finer but may have thin cells.

## Derived tables

Two derivation fallbacks avoid the need for wave-level scripts:

- `food_expenditures`, `food_prices`, `food_quantities` are derived
  from `food_acquired` via `transformations.py` (`_FOOD_DERIVED`).
- `household_characteristics` is derived from `household_roster` via
  `roster_to_characteristics()` (`_ROSTER_DERIVED`).

For these to work, `food_acquired` and `household_roster` must be in
the country's `data_scheme.yml`.

### `units=` kwarg on prices and quantities (Phase 4)

`food_prices()` and `food_quantities()` accept a `units=` kwarg that
controls the price / quantity basis.  Defaults preserve the
pre-Phase-4 behavior (`'kgvalue'` / `'kgs'`).

For demand estimation, `food_prices(units='kgvalue')` (the default —
`Expenditure / Quantity_kg`) is what the literature calls "unit
value" (Deaton 1988, 1997).  `food_prices(units='unitvalue')` gives
per-native-`u` unit values, useful when working in survey units.
`food_prices(units='unitprice')` returns survey-reported prices
directly (NaN where not recorded) — surfaces farmgate prices for
`s='produced'` rows on countries whose wave scripts populate `Price`
(Uganda Phase 3 onwards).  See
`slurm_logs/DESIGN_food_prices_units_kwarg_2026-05-06.org` and the
`add-feature/food-acquired` skill for the full menu.

## Column aliases

Tanzania's wave scripts produce `value_purchase` / `quant_ttl_consume`
instead of the canonical `Expenditure` / `Quantity`.  The
`_normalize_columns()` function in `transformations.py` handles this
mapping transparently.  If adding a new country with non-standard
column names, add entries to the `_COLUMN_ALIASES` dict.

## Common issues

- **Empty food_expenditures**: Check that `food_acquired` is listed in
  `data_scheme.yml`.  Without it, the `_FOOD_DERIVED` fallback is
  bypassed.
- **Missing market index**: A household that `market=` cannot label
  (no `column` in `sample()` and no `cluster_features` row for its
  `(t, v)`) is KEPT with `m` NA and reported by a `NullIndexKeyWarning`
  (`null_index_reports(..., site='market-index')`; fatal under
  `LSMS_READ_STRICT=1`).  Row counts match the call without `market=`.
  Until 2026-11-03 such rows were silently dropped.  `cfe`'s
  `prepare_data` drops NA-`m` rows itself, as does any `groupby('m')`
  with pandas' default `dropna=True`; the fix is wiring the label in
  the country config. The adapter above refuses these keys before fitting.
- **`roster_to_characteristics` index error**: The `final_index`
  defaults to `['t', 'v', 'i']`.  When called via the Country API's
  `_ROSTER_DERIVED` path, the final_index is inferred from the
  roster's actual index levels.
