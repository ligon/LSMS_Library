# Migrating from 0.15.0 to 0.16.0

The index and error changes below can affect callers even when their
country-native data is unchanged. This note accompanies the
[v0.16.0 release notes](../releases/v0.16.0.md).

## Use index names and choose aggregation deliberately

Feature assembly now uses these canonical indexes:

| Feature | Index, including country |
| --- | --- |
| `plot_labor` | `(country, t, v, i, plot_id, stage, source, worker, season)` |
| `plot_inputs` | `(country, t, v, i, plot_id, input, j, u, season)` |
| `community_prices` | `(country, t, v, j, u, variety, visit, obs)` |

Code unpacking positions, selecting exactly the old levels or assuming a
unique `(t, v, j, u)` community-price key must be updated. Use named levels,
and choose any aggregation after retaining the distinctions the source
actually recorded. `crop` aligns to `j` for plot inputs.

`Farm-level` is a structural sentinel for a missing plot, stage or worker
dimension. It does not turn a household-level input into an observed plot,
or mean that a labor row totals every stage/worker type. Seasons may carry
`unknown_season`; Feature's missing-unit-column promotion uses `Unknown`.
These do not cure every native null-key or source-unit problem. Country-native
indexes and details remain survey-specific.
[PR #1126](https://github.com/ligon/LSMS_Library/pull/1126),
[PR #1086](https://github.com/ligon/LSMS_Library/pull/1086).

## Keep country on both sides of pooled calculations

`seed_kg` and `nitrogen_kg` now emit `(country, t, i, plot_id)` if country is
present as an index level or column. `fertilizer_rate` emits the same keys
at `on='plot'`, or `(country, t, i, parcel)` at its existing default
`on='parcel'`. This includes one labeled country and shaped empty results.

Retain country when joining mass and area frames. Labels on just one input
to `fertilizer_rate` raise `ValueError`; labels are not inferred or broadcast.
Present country labels must be non-null before filtering. An index level
takes precedence over a redundant country column. Plot-area local keys can
still be supplied as columns.

Country-less inputs retain the old shape; `attrs['country']` alone does not
create an index level. Other transforms and caller-selected price pools are
not automatically country-safe. The existing parcel/plot policy, nutrient
shares and positive-area sums are unchanged by this identity repair.
[PR #1164](https://github.com/ligon/LSMS_Library/pull/1164),
[PR #1165](https://github.com/ligon/LSMS_Library/pull/1165).

Agriculture consumers now use declared unit masses consistently. Explicit
empty overrides still disable declarations, and existing conversion/refusal
precedence remains in force. Do not remove `unit_kg` metadata before a pooled
conversion if you need country-specific declarations.
[PR #1092](https://github.com/ligon/LSMS_Library/pull/1092),
[PR #1154](https://github.com/ligon/LSMS_Library/pull/1154).

## Handle unavailable modes and invalid mappings explicitly

| Trigger | 0.16.0 contract |
| --- | --- |
| Explicit `units=` mode lacks required input columns | `UnitsUnavailableError`, a `ValueError` subclass; Country names the country |
| Feature cannot supply the requested unit mode for a country | Structured degradation: warning and `attrs['units_unavailable']` |
| Bare categorical mapping request without a value column | `TypeError` |
| Local org table is declared but empty | `EmptyCategoricalTableError`; no silent lower-level inheritance |
| Same table is declared twice within one mapping level | `CategoricalMappingConflictError` |
| Existing country directory lacks `_/data_scheme.yml` | Actionable refusal; explicit `allow_unregistered=True` is for acquisition/scaffolding |
| A `.parquet` file is not parquet | `ParquetFormatError`; no fallback to empty CSV |
| Sidecar-less raw source is read inside the config tree, outside `_/` | `InTreeSourceError`, unless explicitly opted in |

Mapping directories are named `categorical_mapping/`, with tables found by
`#+name:`. File and directory spellings at the same level are combined; the
first wave/country/global level declaring a table overrides it whole-table.
Use `idxvars=` or `#+key:` when naming its key. Deleting a declaration means
inheritance; emptying it does not.

The in-tree source opt-in is `LSMS_ALLOW_INTREE_SOURCES=1` or the sanctioned
acquisition context. A tracked source with a sidecar is resolved through DVC;
ordinary config lookup tables under `_/` remain outside the raw-source guard.

`Feature(...).attrs['labels_unmatched']` is now keyed by country even if only
one country remains. Update readers that treated that one-country report as
flat. Country-native report shape is unchanged.
[PR #1029](https://github.com/ligon/LSMS_Library/pull/1029),
[PR #1018](https://github.com/ligon/LSMS_Library/pull/1018),
[PR #1024](https://github.com/ligon/LSMS_Library/pull/1024),
[PR #1098](https://github.com/ligon/LSMS_Library/pull/1098),
[PR #1105](https://github.com/ligon/LSMS_Library/pull/1105).

## Preserve uncertainty in kinship, dates and cluster identity

Kinship's Generation, Distance and Affinity are separate claims. Some labels
now retain the first two while leaving Affinity missing. Revisit filters
requiring `Affinity == 'consanguineal'`; missing Affinity is not a new positive
classification or a missing person.
[PR #1104](https://github.com/ligon/LSMS_Library/pull/1104).

Datetime fallback parsing is per value, with numeric day/month order settled
per wave only when the values establish it. Undecidable orders become null
with an explicit warning; mixed-timezone parse failures can retain the original strings, now with an
explicit warning. Build-time parsing is preferred where the survey
format is known. Recorded audit reports replay on warm reads.
[PR #1025](https://github.com/ligon/LSMS_Library/pull/1025),
[PR #1105](https://github.com/ligon/LSMS_Library/pull/1105).

Multi-wave Country calls can emit `ClusterIdentityWarning` for recorded
incompatible `v` regimes. `LSMS_CLUSTER_STRICT=1` converts that to
`ClusterIdentityError`; through Feature this can exclude the country with a
warning. A warning is a declaration about comparability, not a generated
cluster crosswalk or a survey-weight correction. Inspect `Country.sampling`
and the returned metadata before choosing cross-wave joins/clustering.
[PR #1137](https://github.com/ligon/LSMS_Library/pull/1137).

## Allow caches to rebuild and preserve acquisition evidence

Reader, mapping and build changes invalidate harmonized parquets across both
L2 tiers. Automatic checks rebuild on access; raw L1 blobs remain reusable.
A global physical cache clear is unnecessary. A successful old warm read is
not evidence that the changed build has run.

Known strict wave subsets that lack the requested table can now produce a
typed empty result without discarding cache coverage for other waves. This
bounded rule does not turn unknown requests or failed source builds into
success. Assumed-fresh missing-file recovery does not suppress corruption or
change the trust contract for unstamped script output.
[PR #1155](https://github.com/ligon/LSMS_Library/pull/1155),
[PR #1162](https://github.com/ligon/LSMS_Library/pull/1162).

Provenance tools distinguish the held release from latest catalog observations
and may refuse unsafe scalar rewrites before changing destination bytes.
Only successful acquisition can certify a different held release; a failed
requested push keeps the existing record. An explicit complete local
`push=False` download remains supported. The guards do not implement raw-file
staging, rollback or configuration-vintage replacement (#1149).
[PR #1117](https://github.com/ligon/LSMS_Library/pull/1117),
[PR #1163](https://github.com/ligon/LSMS_Library/pull/1163).

Reader-credential automation should use `config.s3_creds_path()` rather than
writing into the installed countries tree. Reader and writer credentials
remain distinct; DVC temporary writes and legacy permission paths are separate
limitations. The dependency floor is now `fooddatacentral >=1.0.13`; Python
>=3.11 and pandas >=3.0 remain unchanged.
[PR #1022](https://github.com/ligon/LSMS_Library/pull/1022),
[PR #1065](https://github.com/ligon/LSMS_Library/pull/1065),
[PR #1097](https://github.com/ligon/LSMS_Library/pull/1097).
