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

# Handle duplicates (split-off households may share r_hhid in some rounds)
if not housing.index.is_unique:
    housing = housing.groupby(level=housing.index.names).first()

to_parquet(housing, 'housing.parquet')
