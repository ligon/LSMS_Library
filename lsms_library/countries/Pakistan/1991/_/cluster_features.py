#!/usr/bin/env python
"""Pakistan 1991 cluster_features -- exactly one row per sampling cluster (t, v).

GH #323.
=========
The previous YAML extraction built this table from F00A.DTA -- the household
COVER SHEET -- with ``idxvars: {v: clust, i: hid}``.  That emits one row per
HOUSEHOLD (4,957) into a table whose declared index is (t, v) (301 clusters).
``_normalize_dataframe_index`` then dropped the surplus ``i`` level and collapsed
4,957 -> 301 with a silent ``groupby().first()``, discarding 4,656 rows.

Worse, the columns it carried were not cluster attributes at all:

    Region   <- `religion`  (the household's RELIGION code -- not geography)
    Language <- `langint`   (the language OF THE INTERVIEW)

Both vary WITHIN cluster (religion in 75/301 clusters, langint in 141/301), so
``first()`` was not deduplicating identical rows -- it elected one arbitrary
household as spokesperson for its entire cluster.  Measured: 252 households'
religion and 849 households' langint disagreed with the value ``first()``
assigned to their cluster: 1,101 silently-wrong attributions.

This script instead sources genuine CLUSTER-level geography from REGIONS.DTA
(already used by ``sample``).  ``province`` and ``urbrural`` are provably
constant within cluster -- 0 of 300 clusters vary -- so reducing them to one row
per cluster is EXACT and lossless.  The result is unique by construction, so no
reducer runs in the framework at all.

Value labels are taken from the codebook shipped with the data, Data/REGIONS.TXT
(they are NOT inferred from the distributions)::

    code     province     newprov           urbrural
    1        Punjab       North Punjab       urban
    2        Sind         South Punjab       rural
    3        NWFP         Sind
    4        Balochistan  NWFP
    5                     Balochistan

Cluster 2202029 (a single-household cluster) is entirely absent from REGIONS.DTA
and therefore has NO region data.  Its Region/Rural are left <NA> -- honestly
missing (class-2) rather than silently wrong (class-1).  The leading digit of
`clust` does encode province, but that rule can only be validated on the 300
clusters where province is ALREADY known and is untestable on the one cluster
where it would actually be needed, so it is used here as a build-time GUARD
(see INVARIANT 2) and never as a data source.
"""
import warnings

import pandas as pd

from lsms_library.local_tools import get_dataframe, format_id, to_parquet

# --- Value labels: Data/REGIONS.TXT (the codebook shipped with the data) ------
PROVINCE = {1: 'Punjab', 2: 'Sind', 3: 'NWFP', 4: 'Balochistan'}
URBRURAL = {1: 'Urban', 2: 'Rural'}   # canonical capitalised spellings

t = '1991'

# --- Cluster universe: the cover sheet is authoritative for WHICH clusters ----
# exist and which households belong to them (REGIONS.DTA is missing 204 of them,
# and carries 10 households that are not on the cover sheet at all).
cover = get_dataframe('../Data/F00A.DTA')[['hid', 'clust']].copy()
cover['hid'] = cover['hid'].astype('int64')
cover['clust'] = cover['clust'].astype('int64')

regions = get_dataframe('../Data/REGIONS.DTA')[['hhcode', 'province', 'urbrural']].copy()
regions = regions.dropna(subset=['hhcode'])
regions['hid'] = regions['hhcode'].astype('int64')

# The 9-digit household id is prefixed by the 7-digit cluster id.  Verified
# exact on all 4,957 cover households; assert rather than assume.
_prefix = cover['hid'].astype(str).str[:7]
if not (_prefix == cover['clust'].astype(str)).all():
    raise ValueError(
        'Pakistan 1991: hid is not prefixed by clust; the cluster id can no '
        'longer be recovered from hhcode.  Refusing to guess (GH #323).')
regions['clust'] = regions['hid'].astype(str).str[:7].astype('int64')

# --- INVARIANT 1: province/urbrural really ARE cluster-level ------------------
# This is what makes the reduction below lossless.  If it ever breaks, the
# reduction stops being exact and we must NOT silently elect a winner -- fail.
for col in ('province', 'urbrural'):
    varying = regions.groupby('clust')[col].nunique(dropna=True)
    offenders = varying[varying > 1]
    if len(offenders):
        raise ValueError(
            f'Pakistan 1991 cluster_features: {col!r} varies WITHIN '
            f'{len(offenders)} cluster(s) {sorted(offenders.index)[:5]}...; it is '
            f'therefore not a cluster-level attribute and reducing it to one row '
            f'per cluster would silently discard data (GH #323).')

# Exact + lossless: one row per cluster, verified constant above.
geo = regions.drop_duplicates(subset='clust').set_index('clust')[['province', 'urbrural']]

# --- INVARIANT 2: clust's leading digit encodes province ----------------------
# Used as a GUARD (catches a mis-keyed join / shifted hhcode), never as a data
# source -- see module docstring.
_lead = geo.index.astype(str).str[0].astype(int)
_mismatch = geo[_lead.values != geo['province'].astype(int).values]
if len(_mismatch):
    raise ValueError(
        f'Pakistan 1991 cluster_features: {len(_mismatch)} cluster(s) whose '
        f'clust prefix disagrees with REGIONS.province -- the hhcode<->clust join '
        f'is suspect.  Refusing to emit possibly-wrong geography (GH #323).')

# --- Assemble: 301 clusters from the cover sheet, geography left-joined -------
df = (pd.DataFrame({'v': sorted(cover['clust'].unique())})
        .join(geo, on='v'))

df['Region'] = df['province'].map(PROVINCE).astype('string')
df['Rural'] = df['urbrural'].map(URBRURAL).astype('string')

# Any code outside the codebook would be silently dropped to <NA> by .map --
# surface it instead.
for src, dst, table in (('province', 'Region', PROVINCE), ('urbrural', 'Rural', URBRURAL)):
    unknown = set(df.loc[df[src].notna(), src].astype(int)) - set(table)
    if unknown:
        raise ValueError(
            f'Pakistan 1991 cluster_features: {src!r} code(s) {sorted(unknown)} are '
            f'not in the Data/REGIONS.TXT codebook; refusing to emit an unlabelled '
            f'value (GH #323).')

# --- Clusters with no region data: honestly missing, and say so out loud ------
orphans = df.loc[df['Region'].isna(), 'v'].tolist()
if orphans:
    warnings.warn(
        f'Pakistan 1991 cluster_features: {len(orphans)} cluster(s) {orphans} are '
        f'absent from REGIONS.DTA and have NO region data; Region/Rural left <NA> '
        f'rather than imputed (GH #323 -- honestly missing beats silently wrong).',
        RuntimeWarning)

df['v'] = df['v'].apply(format_id)
df['t'] = t
df = df.set_index(['t', 'v'])[['Region', 'Rural']]

# Unique by construction -- assert it, so the framework's groupby().first()
# fallback can never fire on this table again.
if not df.index.is_unique:
    raise ValueError('Pakistan 1991 cluster_features: (t, v) is not unique (GH #323).')

to_parquet(df, 'cluster_features.parquet')
