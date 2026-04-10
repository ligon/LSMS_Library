#!/usr/bin/env python
"""Sample (sampling design) for Tanzania 2008-15 (multi-round file covering rounds 1-4).

Extracts cluster assignment, sampling weight, strata, and urban/rural classification
from the cover page file (upd4_hh_a.dta).
"""
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd

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

# Handle duplicates by keeping first occurrence
if not sample.index.is_unique:
    sample = sample.groupby(level=sample.index.names).first()

to_parquet(sample, 'sample.parquet')
