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

# GH #323 -- the SAME (UPHI, round) replication that hits interview_date and
# cluster_features.  upd4_hh_a.dta is keyed on the panel-tracking LINE (UPHI),
# not the household, so each household-round arrives once per descendant line
# (29,250 source rows -> 16,540 distinct (i, t); group sizes run 1..11 lines,
# not merely 2).  The old `groupby(level=...).first()` hid that collapse BEFORE
# the parquet was written, so it was invisible even to a wave-parquet audit.
#
# Everything below groups on (i, t) -- the household-ROUND, never the household
# alone.  Both halves of that key are load-bearing across this four-round folder:
#   * `t`, because r_hhid is NOT a stable panel id.  Its FORMAT changes by round
#     (14-digit R1, 16-digit R2, NNNN-NNN R3/R4), and rounds 3 and 4 reuse 1,857
#     of the same r_hhid strings.  Grouping on `i` alone would silently fuse a
#     2012-13 household with an unrelated 2014-15 one.
#   * `i`, because a household legitimately appears in up to four rounds and `v`
#     is NOT round-invariant: the NPS TRACKS movers, and 1,607 of the 8,359 panel
#     lines seen in more than one round change clusterid over their life.  A
#     household's cluster is a per-ROUND fact, so the invariance we may demand is
#     within (i, t) -- never across t.
#
# weight / panel_weight / strata / Rural are identical across the replicate lines
# of a household-round in all four rounds -- so deduping them is value-preserving.
# ASSERT that rather than trust it.
_cols = ['weight', 'panel_weight', 'strata', 'Rural']
_nun = sample.groupby(level=['i', 't'], observed=True)[_cols].nunique(dropna=False)
assert (_nun.max() <= 1).all(), \
    f'sample: {_cols} vary within (i, t); dedup would pick arbitrarily (GH #323)'

# `v` is the EXCEPTION.  59 of the 16,540 (i, t) pairs carry more than one
# clusterid: two panel lines with different ORIGIN EAs ended up in one physical
# household, so that household's sampling cluster is genuinely ambiguous.  All 59
# are in ROUND 4 (2014-15) and all 59 are EXTENDED-panel households -- rounds 1-3
# have zero, and the refresh panel is drawn fresh in round 4 with one line per
# household, so it cannot produce this.  The test is `nunique > 1`, not "exactly
# two lines": the ambiguous groups carry 2, 3 or 5 lines apiece (they happen to
# resolve to exactly 2 distinct clusterids each, but nothing here depends on that).
#
# The old .first() picked one at random -- and because _join_v_from_sample() joins
# this v onto EVERY Tanzania household table, that arbitrary pick propagated
# library-wide.  Emit <NA> instead: a loudly-missing cluster beats a silently-wrong
# one.  The left-join keeps the rows; only v goes null.
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
