#!/usr/bin/env python
"""Sample (sampling design) for Tanzania 2008-15 (multi-round file covering rounds 1-4).

Extracts cluster assignment, sampling weight, strata, and urban/rural classification
from the cover page file (upd4_hh_a.dta).
"""
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd
import warnings

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

df = get_dataframe('../Data/upd4_hh_a.dta')

sample = pd.DataFrame({
    'i': df['r_hhid'].values.tolist(),
    'round': df['round'].values.tolist(),
    'v': df['clusterid'].values.tolist(),
    'weight': df['weight'].values.tolist(),
    'strata': df['strataid'].values.tolist(),
    'Rural': df['urb_rur'].values.tolist(),
})

# Panel weight: same as weight for rounds 1-3 (all panel).
# Round 4 introduced a refresh sample; refresh HHs (ha_07_1 == 'NO')
# should not get a panel weight.
sample['panel_weight'] = sample['weight']
if 'ha_07_1' in df.columns:
    is_refresh = (df['round'] == 4) & (df['ha_07_1'].astype(str).str.upper() == 'NO')
    sample.loc[is_refresh.values, 'panel_weight'] = pd.NA

# Map round numbers to wave labels
sample['t'] = sample['round'].map(round_match)
sample = sample.drop(columns=['round'])

# Convert IDs to clean strings
sample['i'] = sample['i'].astype(str)
sample['v'] = sample['v'].apply(format_id)
sample['strata'] = sample['strata'].apply(format_id)

# Harmonize Rural labels
rural_map = {
    'RURAL': 'Rural',
    'Rural': 'Rural',
    'rural': 'Rural',
    'URBAN': 'Urban',
    'Urban': 'Urban',
    'urban': 'Urban',
}
sample['Rural'] = sample['Rural'].map(rural_map)

sample = sample.set_index(['i', 't'])

# GH #323 -- the SAME (UPHI, round) replication that hit interview_date and
# cluster_features.  upd4_hh_a.dta is keyed on the panel-tracking LINE (UPHI),
# not the household, so each household-round arrives once per descendant line
# (29,250 source rows -> 16,540 distinct (i, t)).  The old
# `groupby(level=...).first()` hid that collapse BEFORE the parquet was written,
# so it was invisible even to a wave-parquet audit.
#
# weight / strata / Rural / panel_weight are identical across the replicates
# (verified: 0 (r_hhid, round) pairs with >1 distinct value), so deduping them
# is value-preserving -- ASSERT that rather than trust it.
_cols = ['weight', 'panel_weight', 'strata', 'Rural']
_nun = sample.groupby(level=['i', 't'], observed=True)[_cols].nunique(dropna=False)
assert (_nun.max() <= 1).all(), \
    f'sample: {_cols} vary within (i, t); dedup would pick arbitrarily (GH #323)'

# `v` is the EXCEPTION.  59 of the 16,540 (i, t) pairs carry more than one
# clusterid: two panel lines with different ORIGIN EAs ended up in one physical
# household, so the household's sampling cluster is genuinely ambiguous.  The old
# .first() picked one at random -- and because _join_v_from_sample() joins this v
# onto EVERY Tanzania household table, that arbitrary pick propagated library-
# wide.  Emit <NA> instead: a loudly-missing cluster (class-2) beats a silently-
# wrong one (class-1).  The left-join keeps the rows; only v goes null.
_vn = sample.groupby(level=['i', 't'], observed=True)['v'].transform('nunique')
_ambiguous = _vn > 1
if _ambiguous.any():
    n = int(sample.loc[_ambiguous].index.nunique())
    warnings.warn(
        f'sample: {n} (i, t) pair(s) map to >1 clusterid (a household holding '
        f'two panel lines with different origin EAs); v set to <NA> rather than '
        f'picking one arbitrarily (GH #323).',
        RuntimeWarning,
    )
    sample.loc[_ambiguous, 'v'] = pd.NA

sample = sample[~sample.index.duplicated(keep='first')]

to_parquet(sample, 'sample.parquet')
