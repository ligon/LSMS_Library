`seed_kg` and `nitrogen_kg` accept a multi-country `plot_inputs` frame but group only by `(t, i, plot_id)`. They discard `country` and sum unrelated plots when two countries reuse the same local identifiers. This occurs with already-metric `Kg` quantities and no declared conversion metadata.

Verified independently on both untouched development `976fc840d12ba40f1602b7dff58b81548632b2a9` and reviewed candidate `852ac2bfcd5f798533f56e7efa1f77db80c0138f`. It is inherited behavior, not a regression introduced by #1093 / PR #1154. No real-country data or new nutrient assumption is needed to reproduce it.

### Minimal reproduction

```python
import pandas as pd
from lsms_library.transformations import seed_kg, nitrogen_kg

rows = [(c, '2000-01', 'h1', 'p1', item, 'Maize', 'Kg', q)
        for c, q in [('A', 1.), ('B', 2.)]
        for item in ['Seed', 'Urea']]
pi = pd.DataFrame(rows, columns=[
    'country', 't', 'i', 'plot_id', 'input', 'j', 'u', 'Quantity'
]).set_index(['country', 't', 'i', 'plot_id', 'input', 'j', 'u'])

print(seed_kg(pi))      # one country-less row: Seed_kg = 3.0
print(nitrogen_kg(pi))  # one country-less row: Nitrogen_kg = 1.38
for c in ['A', 'B']:
    print(c, seed_kg(pi.xs(c, level='country')))
    print(c, nitrogen_kg(pi.xs(c, level='country')))
```

Separate calls give seed quantities **A=1 kg, B=2 kg** and nitrogen quantities **A=0.46 kg, B=0.92 kg**. Pooled calls instead return one `(2000-01, h1, p1)` row with **3 kg seed / 1.38 kg N**, with no country identifier. That is not either country's plot.

Discriminating controls: using distinct household IDs leaves two rows, so a test with different IDs misses the collision; moving `country` from an index level to a column still returns the same single summed row. Those controls also reproduce at the candidate head.

### Source and contract boundary

- [nitrogen reduction keys at the verified head](https://github.com/ligon/LSMS_Library/blob/852ac2bfcd5f798533f56e7efa1f77db80c0138f/lsms_library/transformations.py#L5530) and [seed reduction keys](https://github.com/ligon/LSMS_Library/blob/852ac2bfcd5f798533f56e7efa1f77db80c0138f/lsms_library/transformations.py#L5597) omit `country` before `groupby(...).sum()`.
- The public docstrings currently declare results indexed only by `(t, i, plot_id)` and expose no caller `group_by` argument. They do **not** explicitly define pooled-result behavior or reject multi-country input.
- [Feature's guide](https://github.com/ligon/LSMS_Library/blob/852ac2bfcd5f798533f56e7efa1f77db80c0138f/docs/guide/feature.md#L33) defines the prepended country axis. `_declared_kg_factors` explicitly accepts country levels/columns, and #1093's new tests exercise actual Feature pooling with per-country masses. Those fixtures use distinct household IDs, so they validate factor selection but do not detect this reduction collision.

The confirmed bug is **silently accepting a country-labeled pooled frame and returning a cross-country sum under a country-less plot key**. The precise public remedy needs an explicit API choice:

1. Preserve `country` in per-plot reduction identity when it is present; or
2. Explicitly refuse pooled frames with an actionable error and document that callers must split by country.

No choice about fertilizer shares, missing quantities, parcel meaning or cross-country price pooling is required to recognize the unsafe acceptance. Do not treat loss of country as an intentional scientific pooling default without a documented contract.

### Acceptance

- Pin the identical-local-ID A/B example above for **both** reducers using metric units and no declaration metadata. Neither may silently return the single 3 kg / 1.38 kg N plot.
- Implement/document the chosen contract. If pooled input is accepted, return distinct country-keyed A/B rows with the separate-call values; if unsupported, refuse it clearly before returning numeric results, even when local IDs happen not to collide.
- Cover country as an index level and as a column: retain its identity where supported, otherwise reject the unsupported form explicitly. Swapping input country order must not change the result/decision.
- Preserve ordinary country-less single-country outputs, declared-factor precedence/explicit-empty behavior, unknown-unit refusal, and the existing nutrient matching/refusal diagnostics. Add a shared-local-ID declared-factor control alongside #1093's different-ID tests if pooled conversion remains supported.
- Keep any broader audit of other reducers separate; this issue is only `seed_kg` and `nitrogen_kg`.

### Deduplication and scope

No matching standalone issue found in all-state searches for the reducer names, country-axis/reduction terms, and cross-country seed/nitrogen phrases.

- #1093 / PR #1154 addresses missing declared unit-mass propagation, not these inherited reduction keys; its ledger explicitly excludes this residual.
- #867 is the cited nutrient shares/organic refusal increment; #1107 is Portuguese label harmonization. Neither addresses plot identity across countries.
- Closed #863 is the original derived-transform umbrella, not a pooled-country contract ruling.
- Closed #513 concerns within-country source/id-walk plot collisions and `.first()` loss, not these reducers dropping the country axis.

This is a follow-up filing only, outside the five approved implementation issues for the current release candidate. `median_price_valuation`'s caller-selected price-pooling keys are a separate policy surface and are not part of this issue.
