# Migrating off `Country.locality()`

## What changed

`Country('Uganda').locality()` is deprecated as of the 2026-04-11 release.
The method still works and emits a `DeprecationWarning`; it will be removed
in a future release.

The legacy `locality` table returned a DataFrame indexed by `(i, t, m)` with
a single column `v`, where `m` was a harmonised region label
(Central / Eastern / Northern / Western / Kampala) and `v` was the
parish/cluster identifier.

As of the 2026-04-10 architecture migration, all of that information is
already present in two first-class tables:

| Table | Index | Columns relevant to locality |
|---|---|---|
| `sample()` | `(i, t)` | `v` (cluster / parish) |
| `cluster_features()` | `(t, v)` | `Region`, `Rural`, `District`, `Latitude`, `Longitude` |

## Migration

### Recommended: use `sample()` and `cluster_features()` directly

```python
import lsms_library as ll
c = ll.Country('Uganda')

# Household -> cluster mapping
s = c.sample()           # index (i, t), column v

# Cluster -> region mapping
cf = c.cluster_features()  # index (t, v), columns Region, Rural, District, ...
```

To reproduce the original `locality` join (household -> region) for downstream
analysis:

```python
s = c.sample().reset_index()
cf = c.cluster_features().reset_index()[['t', 'v', 'Region']]
loc = s.merge(cf, on=['t', 'v'], how='left')
loc = loc.rename(columns={'Region': 'm'})
loc = loc.set_index(['i', 't', 'm'])[['v']].sort_index()
```

### Compatibility shim (temporary)

If you cannot update your code immediately, import the shim:

```python
from lsms_library.transformations import legacy_locality
import lsms_library as ll

c = ll.Country('Uganda')
loc = legacy_locality(c)   # same shape as the old locality() output
```

The shim performs the join described above and returns `(i, t, m) -> v`.
It is intended only as a bridge during migration; it will be removed when
`locality()` itself is removed.

## Rationale

The 2026-04-10 architecture migration introduced `sample()` as the single
source of truth for the household-to-cluster mapping. Before that migration,
every feature parquet baked in `v` (the cluster identifier), creating
redundancy and drift risk. After the migration:

- `sample()` owns `(i, t) -> v`.
- `cluster_features()` owns `(t, v) -> Region, Rural, District, ...`.
- All other feature tables receive `v` at API read time via
  `_join_v_from_sample()` in `_finalize_result()`.

`locality` was a thin wrapper over this join with no independent source data.
Keeping it as a first-class feature would mean maintaining a script that
merely duplicates what the framework already provides. Deprecating it removes
that redundancy and points callers to the more capable and better-maintained
replacement tables.
