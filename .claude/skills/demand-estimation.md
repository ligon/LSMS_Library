# Demand Estimation via the Country API

Estimate a CFE demand system for any LSMS country in ~7 lines:

```python
import numpy as np
import lsms_library as ll
from cfe.regression import Regression

c = ll.Country('Tanzania')

x = c.food_expenditures(market='Region')
d = c.household_characteristics(market='Region')
y = np.log(x['Expenditure'].replace(0, np.nan)).dropna()

r = Regression(y=y, d=d, alltm=False)
r.get_beta()
```

Or via CLI / Makefile:

    make -C lsms_library demands country=Tanzania market=Region

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
- **Missing market index**: The `market=` join drops households not
  found in `cluster_features`.  Row counts will be lower than without
  `market=`.
- **`roster_to_characteristics` index error**: The `final_index`
  defaults to `['t', 'v', 'i']`.  When called via the Country API's
  `_ROSTER_DERIVED` path, the final_index is inferred from the
  roster's actual index levels.
