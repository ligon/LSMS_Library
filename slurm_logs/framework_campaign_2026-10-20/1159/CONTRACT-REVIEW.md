# GH #1159 contract and acceptance review

The maintainer chose retained country identity. Both reducers therefore use
`(country, t, i, plot_id)` whenever the input carries a country index level or
column, even for one named country or a shaped empty result. Input row order
does not select a surviving country. Ordinary country-less frames keep their
existing index, and `attrs['country']` alone creates no new axis.

The declaration resolver already selects each row's country-specific mass;
the bug was the later groupby. The fix retains its factor precedence,
explicit-empty suppression, unit refusal, nutrient shares and diagnostics.
A shared private normalization helper promotes a country column before any
filtering; positional identity survives colliding local indexes. Present null
country labels raise an actionable error before filtering. Country alone does
not turn an otherwise missing local reduction index into a supported plot key.

Acceptance covers metric A/B collisions and distinct IDs, both country forms,
reversed order, one country, empty/filtered/unknown-unit/missing-quantity paths,
explicit/attrs/flat declarations, explicit-empty overrides, unit columns,
plot aliases, null-country refusal, immutable inputs and real Feature assembly
using fake country loaders with colliding IDs. Existing country-less nutrient,
factor and fertilizer-rate tests remain unchanged.

Compatibility risk is limited but intentional: a caller that passed a labeled
Feature frame and assumed a three-level reducer result now receives country
identity. Callers that deliberately want a country-less frame must select one
country first; silently dropping a pooled country axis recreates the bug.
Unknown-unit and all-missing-quantity results remain empty, with the retained
axis shape. Nitrogen's old docstring incorrectly claimed an all-unconvertible
plot summed to zero; the corrected prose describes its existing dropna path,
which the empty-result controls pin without changing the runtime policy.

## Separate downstream residual draft

`fertilizer_rate` calls `nitrogen_kg`, then independently reduces its numerator
and area by `(t, i, land)` at transformations.py around line 6900. Thus it still
omits country even after receiving a correctly country-keyed numerator.
Synthetic colliding A/B plots give the same pooled country-less rate, 0.276,
before and after this patch; separate calls give A=0.46 and B=0.23. This is an
inherited downstream identity bug, not a regression caused by the new index.
The before/after aggregate probe records the equality. Fixing its area join,
the plot/parcel bridge, or any other reducer requires separate scope; this
increment changes only `seed_kg` and `nitrogen_kg`.

No real-country microdata or new scientific premise was needed for this
contract reproduction. No country configuration or CONTENTS document changed.
The implementation is anchored on `.coder/ledger/1159-pooled-reducers.md`.

Sue
