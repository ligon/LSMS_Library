#!/usr/bin/env python
"""Extract dwelling materials (Roof, Floor) for Tanzania 2008-15 (rounds 1-4).

Source file: upd4_hh_i1.dta (multi-round housing module).
Variables:
    hi_09  Roof material
    hi_10  Floor material
"""
from lsms_library.local_tools import get_dataframe, to_parquet
import pandas as pd

round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

roof_map = {
    'GRASS, LEAVES, BAMBOO': 'Grass/Leaves/Bamboo',
    'MUD AND GRASS': 'Mud And Grass',
    'CONCRETE, CEMENT': 'Concrete/Cement',
    'METAL SHEETS (GCI)': 'Metal Sheets',
    'ASBESTOS SHEETS': 'Asbestos Sheets',
    'TILES': 'Tiles',
    'OTHER (SPECIFY)': 'Other',
}

floor_map = {
    'EARTH': 'Earth',
    'CONCRETE,CEMENT,TILES,TIMBER': 'Concrete/Cement/Tiles/Timber',
    'OTHER (SPECIFY)': 'Other',
}

df = get_dataframe('../Data/upd4_hh_i1.dta')

housing = pd.DataFrame({
    'i': df['r_hhid'].astype(str),
    't': df['round'].map(round_match),
    'Roof': df['hi_09'].map(roof_map),
    'Floor': df['hi_10'].map(floor_map),
})

housing = housing.set_index(['t', 'i'])

# GH #637 key-soundness review -- the key is SOUND and the collapse is a
# de-replication, not a choice.
#
# upd4_hh_i1.dta is keyed on the panel-tracking LINE (UPHI), not the household
# -- the same replication sample.py documents for the cover page.  29,250
# source rows carry 16,540 household-rounds; 8,488 of those arrive more than
# once (group sizes 2:5477, 3:2247, 4:494, 5:164, 6:61, 7:31, 8:7, 9:4, 10:1,
# 11:2), one row per DESCENDANT line.  (round, r_hhid, UPHI) is unique; no
# (round, UPHI) maps to two r_hhid.
#
# These are the SAME dwelling recorded once per line, not different dwellings
# sharing an id: across all 8,488 duplicate groups, ZERO differ on ANY of the
# 74 hi_* columns -- including the continuous ones (hi_04 rent) and including
# the 59 round-4 groups sample.py flags as cluster-ambiguous.  Roof (hi_09) /
# Floor (hi_10) differ in 0 groups.  So .first() cannot fabricate here; it
# discards exact copies.  ("exact" is not by itself reassurance -- see GH #637
# -- which is why the lineage was checked too: a round-1 household with k lines
# maps to k DISTINCT round-4 households in 174 of the 211 cases where more than
# one of its lines is still observed in round 4.)
if not housing.index.is_unique:
    housing = housing.groupby(level=housing.index.names).first()

to_parquet(housing, 'housing.parquet')
