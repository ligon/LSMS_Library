`fertilizer_rate` accepts country-labeled pooled inputs but groups its numerator and area only by `(t, i, land)`. Countries with the same local plot key become one country-less rate.

Independently reproduced with metric inputs at unchanged `852ac2bfcd5f798533f56e7efa1f77db80c0138f` and the country-preserving seed/nitrogen fix at `45f80dcb50da189b7864034b1d92863dca68de09` (runtime `da84c7c79`). The entire downstream result is identical before/after. This is inherited, not a regression from #1159.

```python
import pandas as pd
from lsms_library.transformations import fertilizer_rate

pi = pd.DataFrame([
    ('A', '2000-01', 'h1', 'p1', 'Urea', 'Kg', 1.),
    ('B', '2000-01', 'h1', 'p1', 'Urea', 'Kg', 2.),
], columns=['country', 't', 'i', 'plot_id', 'input', 'u', 'Quantity']).set_index(
    ['country', 't', 'i', 'plot_id', 'input', 'u'])
pf = pd.DataFrame([
    ('A', '2000-01', 'h1', 'p1', 1.),
    ('B', '2000-01', 'h1', 'p1', 4.),
], columns=['country', 't', 'i', 'plot_id', 'Area']).set_index(
    ['country', 't', 'i', 'plot_id'])

print(fertilizer_rate(pi, pf, on='plot'))
# One country-less row: 0.276 kg N/ha.
for c in ['A', 'B']:
    print(c, fertilizer_rate(pi.xs(c, level='country'),
                             pf.xs(c, level='country'), on='plot'))
# A: 0.46; B: 0.23 kg N/ha.
```

The pooled result is `(0.46 + 0.92) / (1 + 4)`, attributed to neither country. The denominator is not either plot's area. Distinct local IDs can hide the collision.

Source: [`fertilizer_rate` at the reviewed baseline](https://github.com/ligon/LSMS_Library/blob/852ac2bfcd5f798533f56e7efa1f77db80c0138f/lsms_library/transformations.py#L6863): `base` selects only `t` and `i`; both reductions use `base + ['_land']`; output retains those same country-less keys. A country-preserving `nitrogen_kg` numerator is reset and grouped again without country.

This is separate from:

- #1159, deliberately scoped to the two mass reducers. Its fix preserves the numerator country axis; this downstream function discards it independently.
- #1110, the country-specific default parcel/plot bridge defect. The minimal reproduction uses explicit `on='plot'` and identical literal keys, so it does not depend on that decision.
- #867, nutrient matching/shares; the example uses existing Urea and metric Kg.
- #637, whose remaining inventory is country first/last aggregation sites.

Acceptance: decide/document the pooled-rate contract, retain country through both numerator and denominator identity or explicitly refuse unsupported pooled inputs, and never silently combine unrelated plots. Pin colliding local IDs, country index/column forms where supported, reversed row order, both nutrient modes and plot/parcel modes; preserve ordinary country-less behavior. Keep any parcel-policy or scientific share changes separate.

Tier A: framework identity reduction. Filing only; no additional rate implementation is included in the currently delegated two-reducer work. No real microdata or new scientific premise was needed. Reported by Sue following independent synthetic execution and all-state deduplication.
